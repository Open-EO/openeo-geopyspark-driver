import json
import logging
import os
import pathlib
import re
import sys
import tempfile
import types
import zipfile
from datetime import datetime
from typing import Dict, Tuple, Union

import epsel
import geopyspark
import numpy
import pyproj
import pyspark
import shapely.geometry
import shapely.ops
from py4j.java_gateway import JVMView, JavaObject

from openeo.util import TimingLogger
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, FeatureUnsupportedException
from openeo_driver.utils import smart_bool
from openeogeotrellis._utm import utm_zone_from_epsg
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.utils import lonlat_to_mercator_tile_indices, nullcontext, get_jvm

logger = logging.getLogger(__name__)


def _import_orfeo_toolbox(otb_home_env_var="OTB_HOME") -> types.ModuleType:
    """
    Helper to import Orfeo Toolbox module (`otbApplication`), taking care of incomplete environment setup.
    """
    try:
        import otbApplication as otb
    except ImportError as e:
        logger.info(f"Failed to load 'otbApplication' module: {e!r}. Will retry with additional env settings.")

        otb_home = os.environ.get(otb_home_env_var, "").rstrip("/")
        if not otb_home:
            raise OpenEOApiException(f"Env var {otb_home_env_var} is not set.")

        if "OTB_APPLICATION_PATH" not in os.environ:
            otb_application_path = f"{otb_home}/lib/otb/applications"
            logger.info(f"Setting env var 'OTB_APPLICATION_PATH' to {otb_application_path}")
            os.environ["OTB_APPLICATION_PATH"] = otb_application_path

        otb_python_wrapper = f"{otb_home}/lib/otb/python"
        if otb_python_wrapper not in sys.path:
            # TODO: It would be cleaner to append to sys.path instead of prepending,
            #   but unfortunately on Jenkins test environment there is currently
            #   a (broken) otbApplication.py in global `/usr/lib64/python3.8/site-packages`,
            #   which ruins this fallback mechanism.
            logger.info(f"Prepending to Python path: {otb_python_wrapper}")
            sys.path.insert(0, otb_python_wrapper)

        # Note: fixing the dynamic linking search paths for orfeo shared libs (in $OTB_HOME/lib)
        # can not be done at this point because that should happen before Python process starts
        # (e.g. with `LD_LIBRARY_PATH` env var or `ldconfig`)

        # Retry importing it
        import otbApplication as otb
    return otb


def _instant_ms_to_day(instant: int) -> datetime:
    """
    Convert Geotrellis SpaceTimeKey instant (Scala Long, millisecond resolution) to Python datetime object,
    rounded down to day resolution (UTC time 00:00:00), a convention used in other places
    of our openEO backend implementation and necessary to follow, for example
    to ensure that timeseries related data joins work properly.
    """
    return datetime(*(datetime.utcfromtimestamp(instant // 1000).timetuple()[:3]))


class S1BackscatterOrfeo:
    """
    Collection loader that uses Orfeo pipeline to calculate Sentinel-1 Backscatter on the fly.
    """

    def __init__(self, jvm: JVMView = None):
        self.jvm = jvm or get_jvm()

    def _load_feature_rdd(
            self, file_factory: JavaObject, projected_polygons, from_date: str, to_date: str, zoom: int, tile_size: int
    ) -> Tuple[pyspark.RDD, JavaObject]:
        logger.info("Loading feature JSON RDD from {f}".format(f=file_factory))
        json_rdd = file_factory.loadSpatialFeatureJsonRDD(projected_polygons, from_date, to_date, zoom, tile_size)
        jrdd = json_rdd._1()
        layer_metadata_sc = json_rdd._2()

        # Decode/unwrap the JavaRDD of JSON blobs we built in Scala,
        # additionally pickle-serialized by the PySpark adaption layer.
        j2p_rdd = self.jvm.SerDe.javaToPython(jrdd)
        serializer = pyspark.serializers.PickleSerializer()
        pyrdd = geopyspark.create_python_rdd(j2p_rdd, serializer=serializer)
        pyrdd = pyrdd.map(json.loads)
        return pyrdd, layer_metadata_sc

    def _convert_scala_metadata(self, metadata_sc: JavaObject) -> geopyspark.Metadata:
        """
        Convert geotrellis TileLayerMetadata (Java) object to geopyspark Metadata object
        """
        logger.info("Convert {m!r} to geopyspark.Metadata".format(m=metadata_sc))
        crs_py = str(metadata_sc.crs())
        cell_type_py = str(metadata_sc.cellType())

        def convert_key(key_sc: JavaObject) -> geopyspark.SpaceTimeKey:
            return geopyspark.SpaceTimeKey(
                col=key_sc.col(), row=key_sc.row(),
                instant=_instant_ms_to_day(key_sc.instant())
            )

        bounds_sc = metadata_sc.bounds()
        bounds_py = geopyspark.Bounds(minKey=convert_key(bounds_sc.minKey()), maxKey=convert_key(bounds_sc.maxKey()))

        def convert_extent(extent_sc: JavaObject) -> geopyspark.Extent:
            return geopyspark.Extent(extent_sc.xmin(), extent_sc.ymin(), extent_sc.xmax(), extent_sc.ymax())

        extent_py = convert_extent(metadata_sc.extent())

        layout_definition_sc = metadata_sc.layout()
        tile_layout_sc = layout_definition_sc.tileLayout()
        tile_layout_py = geopyspark.TileLayout(
            layoutCols=tile_layout_sc.layoutCols(), layoutRows=tile_layout_sc.layoutRows(),
            tileCols=tile_layout_sc.tileCols(), tileRows=tile_layout_sc.tileRows()
        )
        layout_definition_py = geopyspark.LayoutDefinition(
            extent=convert_extent(layout_definition_sc.extent()),
            tileLayout=tile_layout_py
        )

        return geopyspark.Metadata(
            bounds=bounds_py, crs=crs_py, cell_type=cell_type_py,
            extent=extent_py, layout_definition=layout_definition_py
        )

    def creodias(
            self,
            projected_polygons,
            from_date: str, to_date: str,
            collection_id: str = "Sentinel1",
            correlation_id: str = "NA",
            sar_backscatter_arguments: SarBackscatterArgs = SarBackscatterArgs(),
            bands=None,
            zoom=0,  # TODO: what to do with zoom? It is not used at the moment.
            result_dtype="float32"
    ) -> Dict[int, geopyspark.TiledRasterLayer]:
        """
        Implementation of S1 backscatter calculation with Orfeo in Creodias environment
        """
        # Initial argument checking
        bands = bands or ["VH", "VV"]

        # Mapping of `sar_backscatter` coefficient value to `SARCalibration` Lookup table value
        coefficient_mapping = {
            "beta0": "beta",
            "sigma0-ellipsoid": "sigma",
            "gamma0-ellipsoid": "gamma",
        }

        if sar_backscatter_arguments.coefficient in coefficient_mapping:
            sar_calibration_lut = coefficient_mapping[sar_backscatter_arguments.coefficient]
        else:
            raise OpenEOApiException(
                f"Backscatter coefficient {sar_backscatter_arguments.coefficient!r} is not supported. "
                f"Use one of {list(coefficient_mapping.keys())}.")

        if sar_backscatter_arguments.mask:
            raise FeatureUnsupportedException("sar_backscatter: mask band is not supported")
        if sar_backscatter_arguments.contributing_area:
            raise FeatureUnsupportedException("sar_backscatter: contributing_area band is not supported")
        if sar_backscatter_arguments.local_incidence_angle:
            raise FeatureUnsupportedException("sar_backscatter: local_incidence_angle band is not supported")
        if sar_backscatter_arguments.ellipsoid_incidence_angle:
            raise FeatureUnsupportedException("sar_backscatter: ellipsoid_incidence_angle band is not supported")

        noise_removal = bool(sar_backscatter_arguments.noise_removal)

        # Tile size to use in the TiledRasterLayer.
        tile_size = sar_backscatter_arguments.options.get("tile_size", 512)

        # Geoid for orthorectification: get from options, fallback on config.
        elev_geoid = sar_backscatter_arguments.options.get("elev_geoid") or ConfigParams().s1backscatter_elev_geoid
        elev_default = sar_backscatter_arguments.options.get("elev_default")
        logger.info(f"elev_geoid: {elev_geoid!r}")

        # Build RDD of file metadata from Creodias catalog query.
        # TODO openSearchLinkTitles?
        attributeValues = {
            "productType": "GRD",
            "sensorMode": "IW",
            "processingLevel": "LEVEL1",
        }
        file_factory = self.jvm.org.openeo.geotrellis.file.FileRDDFactory.creo(
            collection_id, [], attributeValues, correlation_id
        )
        feature_pyrdd, layer_metadata_sc = self._load_feature_rdd(
            file_factory, projected_polygons=projected_polygons, from_date=from_date, to_date=to_date,
            zoom=zoom, tile_size=tile_size
        )
        layer_metadata_py = self._convert_scala_metadata(layer_metadata_sc)

        if smart_bool(sar_backscatter_arguments.options.get("debug")):
            with TimingLogger(title="Collect RDD info", logger=logger):
                record_count = feature_pyrdd.count()
                key_ranges = {
                    k: feature_pyrdd.map(lambda f: f["key"][k]).distinct().collect()
                    for k in ["col", "row", "instant"]
                }
                paths = feature_pyrdd.map(lambda f: f["feature"]["id"]).distinct().count()
                logger.info(f"RDD info: {record_count} records, {paths} creo paths, key_ranges: {key_ranges}")

        @epsel.ensure_info_logging
        @TimingLogger(title="process_feature", logger=logger)
        def process_feature(feature):

            col, row, instant = (feature["key"][k] for k in ["col", "row", "instant"])
            log_prefix = "p{p}-key({c},{r},{i}): ".format(p=os.getpid(), c=col, r=row, i=instant)

            key_ext = feature["key_extent"]
            key_epsg = feature["metadata"]["crs_epsg"]
            creo_path = pathlib.Path(feature["feature"]["id"])
            logger.info(log_prefix + f"Feature creo path: {creo_path}, key {key_ext} (EPSG {key_epsg})")
            if not creo_path.exists():
                raise OpenEOApiException("Creo path does not exist")

            with TimingLogger(title=f"{log_prefix} scan {creo_path}", logger=logger):
                # We expect the desired geotiff files under `creo_path` at location like
                #       measurements/s1a-iw-grd-vh-20200606t063717-20200606t063746-032893-03cf5f-002.tiff
                # TODO Get tiff path from manifest instead of assuming this `measurement` file structure?
                band_regex = re.compile(r"^s1[ab]-iw-grd-([hv]{2})-", flags=re.IGNORECASE)
                band_tiffs = {}
                for tiff in creo_path.glob("measurement/*.tiff"):
                    match = band_regex.match(tiff.name)
                    if match:
                        band_tiffs[match.group(1).lower()] = tiff
                if not band_tiffs:
                    raise OpenEOApiException("No tiffs found")
                logger.info(log_prefix + f"Detected band tiffs: {band_tiffs}")

            logger.info(log_prefix + f"sar_backscatter_arguments: {sar_backscatter_arguments!r}")

            elevation_model = sar_backscatter_arguments.elevation_model
            if elevation_model:
                elevation_model = elevation_model.lower()
            if elevation_model in [None, "srtmgl1"]:
                dem_dir_context = S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
                    bbox=(key_ext["xmin"], key_ext["ymin"], key_ext["xmax"], key_ext["ymax"]), bbox_epsg=key_epsg,
                    srtm_root="/eodata/auxdata/SRTMGL1/dem",
                )
            elif elevation_model in ["geotiff", "mapzen"]:
                dem_dir_context = S1BackscatterOrfeo._creodias_dem_subset_geotiff(
                    bbox=(key_ext["xmin"], key_ext["ymin"], key_ext["xmax"], key_ext["ymax"]), bbox_epsg=key_epsg,
                    zoom=sar_backscatter_arguments.options.get("dem_zoom_level", 10),
                    dem_tile_size=512,
                    dem_path_tpl="/eodata/auxdata/Elevation-Tiles/geotiff/{z}/{x}/{y}.tif"
                )
            elif elevation_model in ["off"]:
                # Context that returns None when entering
                dem_dir_context = nullcontext()
            else:
                raise FeatureUnsupportedException(
                    f"Unsupported elevation model {sar_backscatter_arguments.elevation_model!r}"
                )

            msg = f"{log_prefix}Process {creo_path} and load into geopyspark Tile"
            with TimingLogger(title=msg, logger=logger), dem_dir_context as dem_dir:
                # Allocate numpy array tile
                tile_data = numpy.zeros((len(bands), tile_size, tile_size), dtype=result_dtype)

                for b, band in enumerate(bands):
                    if band.lower() not in band_tiffs:
                        raise OpenEOApiException(f"No tiff for band {band}")
                    data, nodata = orfeo_pipeline(
                        input_tiff=band_tiffs[band.lower()], key_extent=key_ext, key_epsg=key_epsg, dem_dir=dem_dir,
                        tile_size=tile_size, log_prefix=log_prefix.replace(": ", f"-{band}: ")
                    )
                    if data.shape != (tile_size, tile_size):
                        if sar_backscatter_arguments.options.get("orfeo_output_mismatch_handling") == "warn":
                            logger.warning(log_prefix + f"Crop/pad shape {data.shape} to ({tile_size},{tile_size})")
                            pad_width = [(0, max(0, tile_size - data.shape[0])), (0, max(0, tile_size - data.shape[1]))]
                            data = numpy.pad(data, pad_width)[:tile_size, :tile_size]
                        else:
                            # Fail with exception by default
                            raise OpenEOApiException(f"Orfeo output mismatch {data.shape} != ({tile_size},{tile_size})")

                    tile_data[b] = data

                if sar_backscatter_arguments.options.get("to_db", False):
                    # TODO: keep this "to_db" shortcut feature or drop it
                    #       and require user to use standard openEO functionality (`apply` based conversion)?
                    logger.info(log_prefix + "Converting backscatter intensity to decibel")
                    tile_data = 10 * numpy.log10(tile_data)

                key = geopyspark.SpaceTimeKey(row=row, col=col, instant=_instant_ms_to_day(instant))
                cell_type = geopyspark.CellType(tile_data.dtype.name)
                logger.info(log_prefix + f"Create Tile for key {key} from {tile_data.shape}")
                tile = geopyspark.Tile(tile_data, cell_type, no_data_value=nodata)
                return key, tile

        @TimingLogger(title="orfeo_pipeline", logger=logger)
        def orfeo_pipeline(
                input_tiff: pathlib.Path, key_extent, key_epsg, dem_dir: Union[str, None], tile_size: int = 512,
                log_prefix: str = ""
        ):
            logger.info(log_prefix + f"Input tiff {input_tiff}")

            key_utm_zone, key_utm_northhem = utm_zone_from_epsg(key_epsg)
            logger.info(
                log_prefix + ("extent {e} (UTM {u}, EPSG {c})").format(e=key_extent, u=key_utm_zone, c=key_epsg))

            otb = _import_orfeo_toolbox()

            def otb_param_dump(app):
                return {
                    p: str(v) if app.GetParameterType(p) == otb.ParameterType_Choice else v
                    for (p, v) in app.GetParameters().items()
                }

            msg = f"{log_prefix}Orfeo processing pipeline on {input_tiff}"
            with tempfile.TemporaryDirectory() as temp_dir, TimingLogger(title=msg, logger=logger):

                # SARCalibration
                sar_calibration = otb.Registry.CreateApplication('SARCalibration')
                sar_calibration.SetParameterString("in", str(input_tiff))
                sar_calibration.SetParameterString("lut", sar_calibration_lut)
                sar_calibration.SetParameterValue('noise', noise_removal)
                sar_calibration.SetParameterInt('ram', 512)
                logger.info(log_prefix + f"SARCalibration params: {otb_param_dump(sar_calibration)}")
                sar_calibration.Execute()

                # OrthoRectification
                ortho_rect = otb.Registry.CreateApplication('OrthoRectification')
                ortho_rect.SetParameterInputImage("io.in", sar_calibration.GetParameterOutputImage("out"))
                if dem_dir:
                    ortho_rect.SetParameterString("elev.dem", dem_dir)
                if elev_geoid:
                    ortho_rect.SetParameterString("elev.geoid", elev_geoid)
                if elev_default is not None:
                    ortho_rect.SetParameterFloat("elev.default", float(elev_default))
                ortho_rect.SetParameterString("map", "utm")
                ortho_rect.SetParameterInt("map.utm.zone", key_utm_zone)
                ortho_rect.SetParameterValue("map.utm.northhem", key_utm_northhem)
                ortho_rect.SetParameterFloat("outputs.spacingx", 10.0)
                ortho_rect.SetParameterFloat("outputs.spacingy", -10.0)
                ortho_rect.SetParameterInt("outputs.sizex", tile_size)
                ortho_rect.SetParameterInt("outputs.sizey", tile_size)
                ortho_rect.SetParameterInt("outputs.ulx", int(key_extent["xmin"]))
                ortho_rect.SetParameterInt("outputs.uly", int(key_extent["ymax"]))
                ortho_rect.SetParameterString("interpolator", "nn")
                ortho_rect.SetParameterFloat("opt.gridspacing", 40.0)
                ortho_rect.SetParameterInt("opt.ram", 512)
                logger.info(log_prefix + f"OrthoRectification params: {otb_param_dump(ortho_rect)}")
                ortho_rect.Execute()

                # TODO: extract numpy array directly (instead of through on disk files)
                #       with GetImageAsNumpyArray (https://www.orfeo-toolbox.org/CookBook/PythonAPI.html#numpy-array-processing)
                #       but requires orfeo toolbox to be compiled with numpy support
                #       (numpy header files must be available at compile time I guess)

                out_path = os.path.join(temp_dir, "out.tiff")
                ortho_rect.SetParameterString("io.out", out_path)
                ortho_rect.ExecuteAndWriteOutput()

                import rasterio
                msg = f"{log_prefix}Reading orfeo output tiff: {out_path}"
                with TimingLogger(title=msg, logger=logger), rasterio.open(out_path) as ds:
                    logger.info(log_prefix + "Output tiff metadata: {m}, bounds {b}".format(m=ds.meta, b=ds.bounds))
                    assert (ds.count, ds.width, ds.height) == (1, tile_size, tile_size)
                    data = ds.read(1)
                    nodata = ds.nodata

            logger.info(log_prefix + f"Data: shape {data.shape}, min {numpy.nanmin(data)}, max {numpy.nanmax(data)}")
            return data, nodata

        tile_rdd = feature_pyrdd.map(process_feature)
        if result_dtype:
            layer_metadata_py.cell_type = result_dtype
        logger.info("Constructing TiledRasterLayer from numpy rdd, with metadata {m!r}".format(m=layer_metadata_py))
        tile_layer = geopyspark.TiledRasterLayer.from_numpy_rdd(
            layer_type=geopyspark.LayerType.SPACETIME,
            numpy_rdd=tile_rdd,
            metadata=layer_metadata_py
        )
        return {zoom: tile_layer}

    @staticmethod
    def _creodias_dem_subset_geotiff(
            bbox: Tuple, bbox_epsg: int, zoom: int = 5,
            dem_tile_size: int = 512, dem_path_tpl: str = "/eodata/auxdata/Elevation-Tiles/geotiff/{z}/{x}/{y}.tif"
    ) -> tempfile.TemporaryDirectory:
        """
        Create subset of Creodias DEM symlinks covering the given lon-lat bbox to pass to Orfeo
        based on the geotiff DEM tiles at /eodata/auxdata/Elevation-Tiles/geotiff/Z/X/Y.tiff

        :return: tempfile.TemporaryDirectory to be used as context manager (for automatic cleanup)
        """
        # Get "bounding box" of DEM tiles
        bbox_lonlat = shapely.ops.transform(
            pyproj.Transformer.from_crs(crs_from=bbox_epsg, crs_to=4326, always_xy=True).transform,
            shapely.geometry.box(*bbox)
        )
        bbox_indices = shapely.ops.transform(
            lambda x, y: lonlat_to_mercator_tile_indices(x, y, zoom=zoom, tile_size=dem_tile_size, flip_y=True),
            bbox_lonlat
        )
        xmin, ymin, xmax, ymax = [int(b) for b in bbox_indices.bounds]

        # Set up temp symlink tree
        temp_dir = tempfile.TemporaryDirectory(suffix="-openeo-dem-geotiff")
        root = pathlib.Path(temp_dir.name)
        logger.info(
            "Creating temporary DEM tile subset tree for {b} (epsg {e}): {r!s}/{z}/[{xi}:{xa}]/[{yi}:{ya}] ({c} tiles) symlinking to {t}".format(
                b=bbox, e=bbox_epsg, r=root, z=zoom, xi=xmin, xa=xmax, yi=ymin, ya=ymax,
                c=(xmax - xmin + 1) * (ymax - ymin + 1), t=dem_path_tpl
            ))
        for x in range(xmin, xmax + 1):
            x_dir = (root / str(zoom) / str(x))
            x_dir.mkdir(parents=True, exist_ok=True)
            for y in range(ymin, ymax + 1):
                (x_dir / ("%d.tif" % y)).symlink_to(dem_path_tpl.format(z=zoom, x=x, y=y))

        return temp_dir

    @staticmethod
    def _creodias_dem_subset_srtm_hgt_unzip(
            bbox: Tuple, bbox_epsg: int, srtm_root="/eodata/auxdata/SRTMGL1/dem"
    ) -> tempfile.TemporaryDirectory:
        """
        Create subset of Creodias SRTM hgt files covering the given lon-lat bbox to pass to Orfeo
        obtained from unzipping the necessary .SRTMGL1.hgt.zip files at /eodata/auxdata/SRTMGL1/dem/
        (e.g. N50E003.SRTMGL1.hgt.zip)

        :return: tempfile.TemporaryDirectory to be used as context manager (for automatic cleanup)
        """
        # Get range of lon-lat tiles to cover
        to_lonlat = pyproj.Transformer.from_crs(crs_from=bbox_epsg, crs_to=4326, always_xy=True)
        bbox_lonlat = shapely.ops.transform(to_lonlat.transform, shapely.geometry.box(*bbox)).bounds
        lon_min, lat_min, lon_max, lat_max = [int(b) for b in bbox_lonlat]

        # Unzip to temp dir
        temp_dir = tempfile.TemporaryDirectory(suffix="-openeo-dem-srtm")
        msg = f"Unzip SRTM tiles from {srtm_root} in range lon [{lon_min}:{lon_max}] x lat [{lat_min}:{lat_max}] to {temp_dir}"
        with TimingLogger(title=msg, logger=logger):
            for lon in range(lon_min, lon_max + 1):
                for lat in range(lat_min, lat_max + 1):
                    # Something like: N50E003.SRTMGL1.hgt.zip"
                    basename = "{ns}{lat:02d}{ew}{lon:03d}.SRTMGL1.hgt".format(
                        ew="E" if lon >= 0 else "W", lon=abs(lon),
                        ns="N" if lat >= 0 else "S", lat=abs(lat)
                    )
                    zip_filename = pathlib.Path(srtm_root) / (basename + '.zip')
                    with zipfile.ZipFile(zip_filename, 'r') as z:
                        logger.info(f"{zip_filename}: {z.infolist()}")
                        z.extractall(temp_dir.name)

        return temp_dir
