import ctypes
import functools
import json
import logging
import multiprocessing
import os
import pathlib
import re
import shutil
import signal
import sys
import tempfile
import types
import zipfile
from datetime import datetime
from multiprocessing import Process
from typing import Dict, Tuple, Union, List, Optional

import geopyspark
import numpy
import numpy as np
import pyproj
import pyspark
import shapely.geometry
import shapely.geometry.polygon
import shapely.ops
from py4j.java_gateway import JVMView, JavaObject
from pyspark import TaskContext

from openeo.util import TimingLogger
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, FeatureUnsupportedException
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import smart_bool
from openeogeotrellis.collections import convert_scala_metadata
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.load_stac import PropertyFilterPGMap
from openeogeotrellis.util.runtime import in_batch_job_context
from openeogeotrellis.utils import lonlat_to_mercator_tile_indices, nullcontext, get_jvm, \
    ensure_executor_logging, download_s3_directory

logger = logging.getLogger(__name__)
_SOFT_ERROR_TRACKER_ID = "orfeo_backscatter_soft_errors"
_EXECUTION_TRACKER_ID = "orfeo_backscatter_execution_counter"
_INPUTPIXELS_TRACKER_ID = "orfeo_backscatter_input_pixels"


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


def get_total_extent(features):
    xmin_min = min(f["key_extent"]["xmin"] for f in features)
    xmax_max = max(f["key_extent"]["xmax"] for f in features)
    ymin_min = min(f["key_extent"]["ymin"] for f in features)
    ymax_max = max(f["key_extent"]["ymax"] for f in features)
    layout_extent = {"xmin": xmin_min, "xmax": xmax_max, "ymin": ymin_min, "ymax": ymax_max}
    return layout_extent


def get_area_in_square_kilometers(projected_polygons: any) -> str:
    try:
        area_to_display = projected_polygons.areaInSquareMeters() / (1000.0 * 1000.0)
    except Exception as e:
        logger.error("sar_backscatter: Error while calculating areaInSquareMeters: " + str(e))
        area_to_display = "unknown "
    return f"{area_to_display}km²"


class S1BackscatterOrfeo:
    """
    Collection loader that uses Orfeo pipeline to calculate Sentinel-1 Backscatter on the fly.

    This class is implementation version 1, which runs Orfeo once per geotrellis tile.
    This results in a large number of invocations, an is therefore only efficient for sparse sampling cases.
    For processing larger areas, the V2 version is more efficient because it groups all geotrellis tiles together and
    tries to minimize Orfeo invocations.
    """

    _DEFAULT_TILE_SIZE = 256
    _COPERNICUS_DEM_ROOT = "/eodata/auxdata/CopDEM_COG/copernicus-dem-30m/"
    _trackers = None

    # Mapping from legacy opensearch property keys to STAC equivalents
    _LEGACY_TO_STAC_PROPERTY_KEYS = {
        "polarisation": "sar:polarizations",
        "polarization": "sar:polarizations",
        "productType": "product:type",
        "processingLevel": "processing:level",
        "orbitDirection": "sat:orbit_state",  # TODO: Is there are a better related STAC property for this?
        "orbitNumber": "sat:absolute_orbit",
        "relativeOrbitNumber": "sat:relative_orbit",
        "timeliness": "product:timeliness_category",
        "missionTakeId": "eopf:datatake_id",
        "sat:orbit_state": "sat:orbit_state",
    }
    # Full mapping includes identity mappings for STAC keys (so users can pass either format)
    _PROPERTY_KEYS_MAPPING = {
        **_LEGACY_TO_STAC_PROPERTY_KEYS,
        **{v: v for v in _LEGACY_TO_STAC_PROPERTY_KEYS.values()},
    }

    # Supported product types for sar_backscatter
    _SUPPORTED_PRODUCT_TYPES = frozenset({"IW_GRDH_1S_B", "IW_GRDH_1S", "IW_GRDH_1S-COG"})

    def __init__(self, jvm: JVMView = None):
        self.jvm = jvm or get_jvm()

    @staticmethod
    def _normalize_filter_properties_to_stac(filter_properties: Dict[str, any]) -> Dict[str, any]:
        """Map opensearch property keys and values to STAC equivalents."""
        property_keys_mapping = S1BackscatterOrfeo._PROPERTY_KEYS_MAPPING
        property_values_mapping = {
            "polarisation": lambda v: v.split("&"),  # VV&VH -> [VV,VH]
            "polarization": lambda v: v.split("&"),  # VV&VH -> [VV,VH]
            "productType": lambda v: v.replace("-COG", "_B"),  # IW_GRDH_1S-COG -> IW_GRDH_1S_B
            "processingLevel": lambda v: v.replace("LEVEL", "L"),  # LEVEL1 -> L1
            "orbitDirection": lambda v: v.lower(),  # DESCENDING -> descending
            "orbitNumber": lambda v: v,
            "relativeOrbitNumber": lambda v: v,
            "timeliness": lambda v: v,
            "sat:orbit_state": lambda v: v.lower(),  # DESCENDING -> descending
        }

        mapped = {}
        for k, v in filter_properties.items():
            if k in property_keys_mapping:
                mapped_key = property_keys_mapping[k]
                mapped_value = (
                    property_values_mapping[k](v)
                    if k in property_values_mapping
                    else v
                )
                mapped[mapped_key] = mapped_value
                if k != mapped_key:
                    logger.warning(
                        f"sar_backscatter: Deprecated property {k!r}={v!r} was automatically "
                        f"converted to new STAC property {mapped_key!r}={mapped_value!r}. "
                        f"Please update your process graph to use the new property and value in the future."
                    )
            else:
                logger.warning(f"sar_backscatter: No mapping for attribute key {k!r}")
        return mapped

    @staticmethod
    def _filter_properties_to_pg_map(filter_properties: Dict[str, any]) -> PropertyFilterPGMap:
        """Convert filter properties to openEO style process graph property filters."""
        mapped = {}
        for k, v in filter_properties.items():
            mapped[k] = {
                "process_graph": {
                    "eq": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": v},
                        "result": True,
                    }
                }
            }
        return mapped

    def _build_stac_opensearch_client(
        self,
        filter_properties: Dict[str, any],
        spatial_extent: Union[Dict, BoundingBox, None],
        temporal_extent: Tuple[Optional[str], Optional[str]],
    ) -> JavaObject:
        """Build a FixedFeaturesOpenSearchClient populated with features from STAC API."""
        from openeogeotrellis.load_stac import _spatiotemporal_extent_from_load_params
        from openeogeotrellis.load_stac import construct_item_collection

        url = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd"

        # filter_properties are assumed to be in STAC only format.
        property_filter_pg_map = self._filter_properties_to_pg_map(filter_properties)

        # Build spatiotemporal extent
        spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
            spatial_extent=spatial_extent,
            temporal_extent=temporal_extent
        )

        # Query STAC API
        item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
            url=url,
            spatiotemporal_extent=spatiotemporal_extent,
            property_filter_pg_map=property_filter_pg_map,
        )

        # Build FixedFeaturesOpenSearchClient from STAC items
        jvm = get_jvm()
        opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

        for itm, band_assets in item_collection.iter_items_with_band_assets():
            builder = (
                jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
                .withNominalDate(itm.properties.get("datetime") or itm.properties["start_datetime"])
                .withGeometryFromWkt(str(shapely.geometry.shape(itm.geometry)))
            )
            if not itm.bbox:
                raise OpenEOApiException(f"Item {itm.id} has no bbox")
            latlon_bbox = BoundingBox.from_wsen_tuple(itm.bbox, 4326)
            builder = builder.withBBox(*map(float, latlon_bbox.as_wsen_tuple()))

            # Extract product ID from asset href
            # This product_id will be used as the creo_path (.SAFE file) later.
            product_id = None
            for asset_id, asset in band_assets.items():
                if asset_id == "vv" or asset_id == "vh":
                    href = asset.href
                    # s3://eodata/Sentinel-1/SAR/IW_GRDH_1S-COG/2020/06/06/S1B_IW_GRDH_1SDV_20200606T060615_20200606T060640_021909_029944_094C_COG.SAFE/measurement/s1b-iw-grd-vh-20200606t060615-20200606t060640-021909-029944-002-cog.tiff
                    href = href.replace("s3://", "/")
                    if not ".SAFE" in href:
                        raise OpenEOApiException(f"Expected .SAFE in Creodias href {href}")
                    product_id = href.split(".SAFE")[0] + ".SAFE"
                    break

            if product_id is None:
                raise OpenEOApiException(f"No 'vv' or 'vh' asset found in item {itm.id} with proper href for sar_backscatter")

            builder = builder.withId(product_id)
            opensearch_client.addFeature(builder.build())

        return opensearch_client

    def _build_legacy_opensearch_client(self) -> JavaObject:
        return self.jvm.org.openeo.opensearch.OpenSearchClient.apply(
            "https://catalogue.dataspace.copernicus.eu/resto", False, "", [], ""
        )

    def _load_feature_rdd(
            self, file_rdd_factory: JavaObject, projected_polygons, from_date: str, to_date: str, zoom: int,
            tile_size: int, datacubeParams=None
    ) -> Tuple[pyspark.RDD, JavaObject, JavaObject]:
        logger.info("Loading feature JSON RDD from {f}".format(f=file_rdd_factory))
        json_rdd_partitioner = file_rdd_factory.loadSpatialFeatureJsonRDD(projected_polygons, from_date, to_date, zoom, tile_size,datacubeParams)
        jrdd = json_rdd_partitioner._1()
        layer_metadata_sc = json_rdd_partitioner._2()

        # Decode/unwrap the JavaRDD of JSON blobs we built in Scala,
        # additionally pickle-serialized by the PySpark adaption layer.
        j2p_rdd = self.jvm.SerDe.javaToPython(jrdd)
        serializer = pyspark.serializers.PickleSerializer()
        pyrdd = geopyspark.create_python_rdd(j2p_rdd, serializer=serializer)
        pyrdd = pyrdd.map(json.loads)
        return pyrdd, layer_metadata_sc,json_rdd_partitioner._3()

    @staticmethod
    def _build_filter_properties(extra_properties: dict, use_stac_client: bool) -> Dict[str, any]:
        """
        Build filter properties from user-provided extra_properties.

        For STAC client: normalizes legacy keys to STAC format, returns STAC-formatted properties.
        For legacy client: keeps legacy format, returns opensearch-formatted properties.
        """
        if use_stac_client:
            # STAC client allows both legacy and STAC property keys.
            allowed_property_keys = set(S1BackscatterOrfeo._PROPERTY_KEYS_MAPPING.keys())
            user_props = {k: v for k, v in extra_properties.items() if k in allowed_property_keys}
            # Normalize legacy property keys to STAC format. So filter_properties are always in STAC format.
            normalized_props = S1BackscatterOrfeo._normalize_filter_properties_to_stac(user_props) if user_props else {}
            filter_properties: Dict[str, any] = {
                "product:type": "IW_GRDH_1S_B",
                "processing:level": "L1",
            }
            filter_properties.update(normalized_props)
            if extra_properties.get("COG") == "FALSE":
                non_cog_product_type = "IW_GRDH_1S"
                logger.info(f"sar_backscatter: Adjusting product:type to {non_cog_product_type!r} based on COG=FALSE")
                filter_properties["product:type"] = non_cog_product_type
        else:
            # For legacy opensearch API, only allow legacy property keys.
            allowed_property_keys = set(S1BackscatterOrfeo._LEGACY_TO_STAC_PROPERTY_KEYS.keys())
            user_props = {k: v for k, v in extra_properties.items() if k in allowed_property_keys}
            filter_properties: Dict[str, any] = {
                "productType": "IW_GRDH_1S-COG",
                "processingLevel": "LEVEL1",
            }
            if extra_properties.get("COG") == "FALSE":
                non_cog_product_type = "IW_GRDH_1S"
                logger.info(f"sar_backscatter: Adjusting product:type to {non_cog_product_type!r} based on COG=FALSE")
                filter_properties["productType"] = non_cog_product_type
            filter_properties.update(user_props)
            if "polarization" in extra_properties:
                # British vs US English: Sentinelhub + STAC use US variant
                filter_properties["polarisation"] = extra_properties["polarization"]

        product_type: Optional[str] = filter_properties.get("product:type", filter_properties.get("productType"))
        if product_type not in S1BackscatterOrfeo._SUPPORTED_PRODUCT_TYPES:
            raise OpenEOApiException(
                f"sar_backscatter: Unsupported product type {product_type!r}. "
                f"Only the following values are supported: {sorted(S1BackscatterOrfeo._SUPPORTED_PRODUCT_TYPES)}."
            )

        return filter_properties

    def _build_feature_rdd(
            self,
            collection_id, projected_polygons, from_date: str, to_date: str, extra_properties: dict,
            tile_size: int, zoom: int, correlation_id: str, datacubeParams=None, resolution = (10.0,10.0),
            spatial_extent=None, use_stac_client: bool = True
    ):
        """Build RDD of file metadata from Creodias catalog query."""
        filter_properties = self._build_filter_properties(extra_properties, use_stac_client)

        # Build opensearch client based on selection
        if use_stac_client:
            logger.info("Using STAC-based opensearch client (FixedFeaturesOpenSearchClient)")
            opensearch_client = self._build_stac_opensearch_client(
                filter_properties=filter_properties,
                spatial_extent=spatial_extent,
                temporal_extent=(from_date, to_date)
            )
        else:
            logger.info("Using legacy opensearch client")
            opensearch_client = self._build_legacy_opensearch_client()

        # Create FileRDDFactory
        # Note that for the STAC client, the filter_properties were already applied and are ignored here.
        file_rdd_factory = self.jvm.org.openeo.geotrellis.file.FileRDDFactory(
            opensearch_client, collection_id, filter_properties, correlation_id,
            self.jvm.geotrellis.raster.CellSize(resolution[0], resolution[1])
        )

        # Load feature RDD
        feature_pyrdd, layer_metadata_sc, partitioner = self._load_feature_rdd(
            file_rdd_factory, projected_polygons=projected_polygons, from_date=from_date, to_date=to_date,
            zoom=zoom, tile_size=tile_size, datacubeParams=datacubeParams
        )

        layer_metadata_py = convert_scala_metadata(
            layer_metadata_sc, epoch_ms_to_datetime=_instant_ms_to_day, logger=logger
        )
        return feature_pyrdd, layer_metadata_py, partitioner

    # Mapping of `sar_backscatter` coefficient value to `SARCalibration` Lookup table value
    _coefficient_mapping = {
        "beta0": "beta",
        "sigma0-ellipsoid": "sigma",
        "gamma0-ellipsoid": "gamma",
    }

    @staticmethod
    def _get_sar_calibration_lut(coefficient: str) -> str:
        try:
            return S1BackscatterOrfeo._coefficient_mapping[coefficient]
        except KeyError:
            raise OpenEOApiException(
                f"Backscatter coefficient {coefficient!r} is not supported. "
                f"Use one of {list(S1BackscatterOrfeo._coefficient_mapping.keys())}.")

    def _debug_show_rdd_info(self, rdd):
        with TimingLogger(title="Collect RDD info", logger=logger):
            record_count = rdd.count()
            key_ranges = {
                k: rdd.map(lambda f: f["key"][k]).distinct().collect()
                for k in ["col", "row", "instant"]
            }
            paths = rdd.map(lambda f: f["feature"]["id"]).distinct().count()
            logger.info(f"RDD info: {record_count} records, {paths} creo paths, key_ranges: {key_ranges}")

    @staticmethod
    def _creo_scan_for_band_tiffs(creo_path: pathlib.Path, log_prefix: str) -> Dict[str, pathlib.Path]:
        """
        Scan given creodias path for TIFF files
        :param creo_path: path to product root folder
        :param log_prefix: prefix for logging
        :return: dictionary mapping band name (vv, vh, ...) to tiff path
        """
        with TimingLogger(title=f"{log_prefix} Scan {creo_path}", logger=logger):
            # We expect the desired geotiff files under `creo_path` at location like
            #       measurements/s1a-iw-grd-vh-20200606t063717-20200606t063746-032893-03cf5f-002.tiff
            # TODO Get tiff path from manifest instead of assuming this `measurement` file structure?
            band_regex = re.compile(r"^s1[abcdefgh]-iw-grd-([hv]{2})-", flags=re.IGNORECASE)
            band_tiffs = {}
            for tiff in creo_path.glob("measurement/*.tiff"):
                match = band_regex.match(tiff.name)
                if match:
                    band_tiffs[match.group(1).lower()] = tiff
            if not band_tiffs:
                logger.error(f"{log_prefix} sar_backscatter: No tiffs found in ${str(creo_path)}")
                return {}
            logger.info(f"{log_prefix} Detected band tiffs: {band_tiffs}")
        return band_tiffs

    @staticmethod
    def _get_dem_dir_context(sar_backscatter_arguments: SarBackscatterArgs, extent: dict, epsg: int):
        """
        Build context manager that sets up temporary dir with digital elevation model files
        for given spatial extent.
        """
        elevation_model = sar_backscatter_arguments.elevation_model
        if elevation_model:
            elevation_model = elevation_model.lower()

        if elevation_model in [None, "srtmgl1"]:
            dem_dir_context = S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
                bbox=(extent["xmin"], extent["ymin"], extent["xmax"], extent["ymax"]), bbox_epsg=epsg,
                srtm_root="/eodata/auxdata/SRTMGL1/dem",
            )
        elif elevation_model in ["geotiff", "mapzen"]:
            dem_dir_context = S1BackscatterOrfeo._creodias_dem_subset_geotiff(
                bbox=(extent["xmin"], extent["ymin"], extent["xmax"], extent["ymax"]), bbox_epsg=epsg,
                zoom=sar_backscatter_arguments.options.get("dem_zoom_level", 10),
                dem_tile_size=512,
                dem_path_tpl="/eodata/auxdata/Elevation-Tiles/geotiff/{z}/{x}/{y}.tif"
            )
        elif elevation_model in ["copernicus_30"]:
            dem_dir_context = (
                S1BackscatterOrfeo._creodias_dem_subset_copernicus30_geotiff(
                    bbox=(
                        extent["xmin"],
                        extent["ymin"],
                        extent["xmax"],
                        extent["ymax"],
                    ),
                    bbox_epsg=epsg,
                    copernicus_root=S1BackscatterOrfeo._COPERNICUS_DEM_ROOT,
                )
            )
        elif elevation_model in ["off"]:
            # Context that returns None when entering
            dem_dir_context = nullcontext()
        else:
            raise FeatureUnsupportedException(
                f"Unsupported elevation model {sar_backscatter_arguments.elevation_model!r}"
            )
        return dem_dir_context

    @staticmethod
    def _orfeo_pipeline(
            input_tiff: pathlib.Path,
            extent: dict,
            extent_epsg: int,
            dem_dir: Union[str, None],
            extent_width_px: int,
            extent_height_px: int,
            sar_calibration_lut: str,
            noise_removal: bool,
            elev_geoid: str,
            elev_default: float = None,
            log_prefix: str = "",
            orfeo_memory:int = 512,
            trackers=None,
            max_soft_errors_ratio = 0.0,
            target_resolution = (10.0,10.0)
    ):
        logger.info(f"{log_prefix} Input tiff {input_tiff}")
        logger.info(f"{log_prefix} extent {extent} EPSG {extent_epsg})")

        if trackers is not None:
            trackers[0].add(1)
            trackers[2].add(extent_width_px * extent_height_px)

        tempdir = tempfile.mkdtemp()
        out_path = os.path.join(tempdir, input_tiff.name)
        write_to_numpy = extent_height_px < 2500 and extent_width_px < 2500


        with TimingLogger(title=f"{log_prefix} Orfeo processing pipeline on {input_tiff}", logger=logger):
            arr = None
            if write_to_numpy:
                arr = multiprocessing.Array(ctypes.c_float, extent_width_px*extent_height_px, lock=False)
            error_counter = multiprocessing.Value('i', 0, lock=False)
            ortho_rect = S1BackscatterOrfeo.configure_pipeline(dem_dir, elev_default, elev_geoid, input_tiff,
                                                               log_prefix, noise_removal, orfeo_memory,
                                                               sar_calibration_lut, epsg=extent_epsg, target_resolution=target_resolution)

            def run():
                ortho_rect.SetParameterInt("outputs.sizex", extent_width_px)
                ortho_rect.SetParameterInt("outputs.sizey", extent_height_px)
                ortho_rect.SetParameterInt("outputs.ulx", int(extent["xmin"]))
                ortho_rect.SetParameterInt("outputs.uly", int(extent["ymax"]))
                try:
                    if(not write_to_numpy):

                        logger.info(f"{log_prefix} Write orfeo pipeline output to temporary {out_path}")
                        ortho_rect.SetParameterString("io.out", out_path)
                        ortho_rect.ExecuteAndWriteOutput()
                    else:
                        ortho_rect.Execute()
                        # ram = ortho_rect.PropagateRequestedRegion("io.out", myRegion)
                        localdata = ortho_rect.GetImageAsNumpyArray('io.out')
                        np.copyto(dst=np.frombuffer(arr, dtype=np.float32).reshape((extent_height_px, extent_width_px)),
                              src=localdata, casting="same_kind")
                except RuntimeError as e:
                    error_counter.value += 1
                    msg = f"Error while running Orfeo toolbox. {input_tiff}, {e}   {extent} EPSG {extent_epsg} {sar_calibration_lut}"
                    logger.error(msg,exc_info=True)

            # TODO: Set these env vars at executor-level (for all clusters).
            gdal_http_max_retry: Optional[str] = os.environ.get("GDAL_HTTP_MAX_RETRY")
            gdal_http_retry_delay: Optional[str] = os.environ.get("GDAL_HTTP_RETRY_DELAY")
            os.environ["GDAL_HTTP_MAX_RETRY"] = "10"
            os.environ["GDAL_HTTP_RETRY_DELAY"] = "60"
            p = Process(target=run, args=())
            p.start()
            p.join()
            if p.exitcode == -signal.SIGSEGV:
                error_counter.value += 1
                msg = f"Segmentation fault while running Orfeo toolbox. {input_tiff} {extent} EPSG {extent_epsg} {sar_calibration_lut}"
                logger.error(msg)
            del os.environ["GDAL_HTTP_MAX_RETRY"]
            del os.environ["GDAL_HTTP_RETRY_DELAY"]
            if gdal_http_max_retry is not None:
                os.environ["GDAL_HTTP_MAX_RETRY"] = gdal_http_max_retry
            if gdal_http_retry_delay is not None:
                os.environ["GDAL_HTTP_RETRY_DELAY"] = gdal_http_retry_delay
            # Check soft error ratio.
            if trackers is not None and error_counter.value > 0:
                if max_soft_errors_ratio == 0.0:
                    msg = f"sar_backscatter: Orfeo error can be found in the logs. Errors can happen due to corrupted input products. Setting the 'soft-errors' job option allows you to skip these products and continue processing."
                    raise RuntimeError(msg)
                else:
                    context = TaskContext.get()
                    if context is not None and context.attemptNumber() == 0:
                        raise RuntimeError(f"sar_backscatter: First attempt for {input_tiff} failed with an error, will retry.")
                    else:
                        trackers[1].add(1)

                    # TODO: #302 Implement singleton for batch jobs, to check soft errors after collect.
                    logger.warning(f"ignoring soft errors, max_soft_errors_ratio={max_soft_errors_ratio}")

            if write_to_numpy:
                data = np.reshape(np.frombuffer(arr,dtype=np.float32), (extent_height_px, extent_width_px))

                logger.info(
                    f"{log_prefix} Final orfeo pipeline result: shape {data.shape},"
                    f" min {numpy.nanmin(data)}, max {numpy.nanmax(data)}"
                )
                return data, 0
            else:
                return out_path, 0

    @staticmethod
    def has_too_many_NoData(image, threshold: int, nodata: Union[float, int]) -> bool:
        """
        Analyses whether an image contains NO DATA.

            :param image:     np.array image to analyse
            :param threshold: number of NoData searched
            :param nodata:    no data value
            :return:          whether the number of no-data pixel > threshold
        """
        nbNoData = len(np.argwhere(image == nodata))
        return nbNoData > threshold

    @staticmethod
    @functools.lru_cache(10,False)
    def configure_pipeline(dem_dir, elev_default, elev_geoid, input_tiff: pathlib.Path, log_prefix, noise_removal, orfeo_memory,
                           sar_calibration_lut, epsg:int, target_resolution = (10.0,10.0)):
        otb = _import_orfeo_toolbox()

        def otb_param_dump(app):
            return {
                p: str(v) if app.GetParameterType(p) == otb.ParameterType_Choice else v
                for (p, v) in app.GetParameters().items()
            }

        import rasterio
        from rasterio.windows import Window
        crop1 = False
        crop2 = False

        if input_tiff.exists():
            with rasterio.open(input_tiff, driver="GTiff") as ds:

                cut_overlap_range = 1000  # Number of columns to cut on the sides. Here 500pixels = 5km
                cut_overlap_azimuth = 1600  # Number of lines to cut at the top or the bottom
                thr_nan_for_cropping = cut_overlap_range * 2  # When testing we having cut the NaN yet on the border hence this threshold.

                north = ds.read(1,window=Window(col_off=0,row_off=100,width=ds.width + 1,height=1))
                south = ds.read(1,window=Window(col_off=0,row_off=ds.height-100,width=ds.width +1,height=1))
                crop1 = S1BackscatterOrfeo.has_too_many_NoData(north, thr_nan_for_cropping, 0)
                crop2 = S1BackscatterOrfeo.has_too_many_NoData(south, thr_nan_for_cropping, 0)
                del south
                del north

        thr_y_s = cut_overlap_azimuth if crop1 else 0
        thr_y_e = cut_overlap_azimuth if crop2 else 0

        # SARCalibration
        sar_calibration = otb.Registry.CreateApplication('SARCalibration')
        sar_calibration.SetParameterString("in", str(input_tiff))
        sar_calibration.SetParameterString("lut", sar_calibration_lut)
        sar_calibration.SetParameterValue('removenoise', noise_removal)
        sar_calibration.SetParameterInt('ram', orfeo_memory)
        logger.info(f"{log_prefix} SARCalibration params: {otb_param_dump(sar_calibration)}")

        # Cut away corrupt data, to work around this issue: https://gitlab.orfeo-toolbox.org/orfeotoolbox/otb/-/issues/2509
        # Approach and parameters inspired by S1-Tiling
        reset_margin = sar_calibration
        if crop1 or crop2:
            reset_margin = otb.Registry.CreateApplication('ResetMargin')
            reset_margin.ConnectImage("in", sar_calibration, "out")
            reset_margin.SetParameterInt('threshold.y.start', thr_y_s)
            reset_margin.SetParameterInt('threshold.y.end', thr_y_e)
            reset_margin.SetParameterString('mode', 'threshold')


        # OrthoRectification
        ortho_rect = otb.Registry.CreateApplication('OrthoRectification')
        ortho_rect.ConnectImage("io.in", reset_margin, "out")

        if dem_dir:
            ortho_rect.SetParameterString("elev.dem", dem_dir)
        if elev_geoid:
            if not pathlib.Path(elev_geoid).exists():
                raise OpenEOApiException(f"sar_backscatter: Geoid file {elev_geoid} does not exist on the cluster, a missing geoid causes shifted output data.")
            ortho_rect.SetParameterString("elev.geoid", elev_geoid)
        if elev_default is not None:
            ortho_rect.SetParameterFloat("elev.default", float(elev_default))
        ortho_rect.SetParameterString("map", "epsg")
        ortho_rect.SetParameterInt("map.epsg.code", epsg)

        ortho_rect.SetParameterDouble("outputs.spacingx", target_resolution[0])
        ortho_rect.SetParameterDouble("outputs.spacingy", -1.0 * target_resolution[1])
        ortho_rect.SetParameterString("interpolator", "linear")
        ortho_rect.SetParameterDouble("opt.gridspacing", 40.0)

        #ortho_rect.SetParameterString("outputs.mode", "autosize")
        #TODO autosize may not align perfectly with Sentinel-2 grid, need to realign


        ortho_rect.SetParameterInt("opt.ram", orfeo_memory)
        logger.info(f"{log_prefix} OrthoRectification params: {otb_param_dump(ortho_rect)}")

        return ortho_rect

    @staticmethod
    def _get_process_function(sar_backscatter_arguments, result_dtype, bands, trackers=None, max_soft_errors_ratio=0.0):

        # Tile size to use in the TiledRasterLayer.
        tile_size = sar_backscatter_arguments.options.get("tile_size", S1BackscatterOrfeo._DEFAULT_TILE_SIZE)
        noise_removal = bool(sar_backscatter_arguments.noise_removal)

        # Geoid for orthorectification: get from options, fallback on config.
        elev_geoid = (
            sar_backscatter_arguments.options.get("elev_geoid") or get_backend_config().s1backscatter_elev_geoid
        )
        elev_default = sar_backscatter_arguments.options.get("elev_default")
        logger.info(f"elev_geoid: {elev_geoid!r}")

        sar_calibration_lut = S1BackscatterOrfeo._get_sar_calibration_lut(sar_backscatter_arguments.coefficient)

        @ensure_executor_logging
        @TimingLogger( title="process_feature", logger=logger)
        def process_feature( product: Tuple[str, List[dict]]):
            import faulthandler
            faulthandler.enable()
            creo_path, features = product

            prod_id = re.sub(r"[^A-Z0-9]", "", creo_path.upper())[-10:]
            log_prefix = f"p{os.getpid()}-prod{prod_id}"
            logger.info(f"{log_prefix} creo path {creo_path}")
            logger.info(f"{log_prefix} sar_backscatter_arguments: {sar_backscatter_arguments!r}")

            layout_extent = get_total_extent(features)
            key_epsgs = set(f["key_epsg"] for f in features)
            assert len(key_epsgs) == 1, f"Multiple key CRSs {key_epsgs}"
            layout_epsg = key_epsgs.pop()

            dem_dir_context = S1BackscatterOrfeo._get_dem_dir_context(
                sar_backscatter_arguments=sar_backscatter_arguments,
                extent=layout_extent,
                epsg=layout_epsg
            )

            creo_path = pathlib.Path(creo_path)

            band_tiffs = S1BackscatterOrfeo._creo_scan_for_band_tiffs(creo_path, log_prefix)

            resultlist = []

            with dem_dir_context as dem_dir:

                for feature in features:
                    col, row, instant = (feature["key"][k] for k in ["col", "row", "instant"])

                    key_ext = feature["key_extent"]
                    key_epsg = layout_epsg

                    logger.info(f"{log_prefix} Feature creo path: {creo_path}, key {key_ext} (EPSG {key_epsg})")
                    logger.info(f"{log_prefix} sar_backscatter_arguments: {sar_backscatter_arguments!r}")
                    if not creo_path.exists():
                        if max_soft_errors_ratio == 0.0:
                            raise OpenEOApiException(f"sar_backscatter: path to SAR product ${str(creo_path)} does not exist on the cluster.")
                        else:
                            logger.warning(f"sar_backscatter: path to SAR product ${str(creo_path)} does not exist on the cluster.")

                    msg = f"{log_prefix} Process {creo_path} and load into geopyspark Tile"
                    with TimingLogger(title=msg, logger=logger):
                        # Allocate numpy array tile
                        tile_data = numpy.zeros((len(bands), tile_size, tile_size), dtype=result_dtype)

                        for b, band in enumerate(bands):
                            if band.lower() not in band_tiffs:
                                raise OpenEOApiException(f"No tiff for band {band}")
                            data, nodata = S1BackscatterOrfeo._orfeo_pipeline(
                                input_tiff=band_tiffs[band.lower()],
                                extent=key_ext, extent_epsg=key_epsg,
                                dem_dir=dem_dir,
                                extent_width_px=tile_size, extent_height_px=tile_size,
                                sar_calibration_lut=sar_calibration_lut,
                                noise_removal=noise_removal,
                                elev_geoid=elev_geoid, elev_default=elev_default,
                                log_prefix=f"{log_prefix}-{band}",
                                trackers=trackers,
                                max_soft_errors_ratio=max_soft_errors_ratio,
                            )
                            if isinstance(data,str):
                                import rasterio
                                ds = rasterio.open(data,driver="GTiff")
                                tile_data[b] = ds.read(1)
                            else:
                                tile_data[b] = data

                        if sar_backscatter_arguments.options.get("to_db", False):
                            # TODO: keep this "to_db" shortcut feature or drop it
                            #       and require user to use standard openEO functionality (`apply` based conversion)?
                            logger.info(f"{log_prefix} Converting backscatter intensity to decibel")
                            tile_data = 10 * numpy.log10(tile_data)

                        key = geopyspark.SpaceTimeKey(row=row, col=col, instant=_instant_ms_to_day(instant))
                        cell_type = geopyspark.CellType(tile_data[0].dtype.name)
                        logger.debug(f"{log_prefix} Create Tile for key {key} from {tile_data.shape}")
                        tile = geopyspark.Tile(tile_data, cell_type, no_data_value=nodata)
                        resultlist.append((key, tile))

            return resultlist

        return process_feature

    def creodias(
            self,
            projected_polygons,
            from_date: str, to_date: str,
            collection_id: str = "Sentinel1",
            correlation_id: str = "NA",
            sar_backscatter_arguments: SarBackscatterArgs = SarBackscatterArgs(),
            bands=None,
            zoom=0,  # TODO: what to do with zoom? It is not used at the moment.
            result_dtype="float32",
            extra_properties={},
            datacubeParams=None,
            max_soft_errors_ratio=0.0,
            spatial_extent=None,
            use_stac_client: bool = True
    ) -> Dict[int, geopyspark.TiledRasterLayer]:
        """
        Implementation of S1 backscatter calculation with Orfeo in Creodias environment
        """
        logger.info(f"{self.__class__.__name__}.creodias()")
        # Initial argument checking
        bands = bands or ["VH", "VV"]

        if sar_backscatter_arguments.mask:
            raise FeatureUnsupportedException("sar_backscatter: mask band is not supported for "
                                              "collection {c}".format(c=collection_id))
        if sar_backscatter_arguments.contributing_area:
            raise FeatureUnsupportedException("sar_backscatter: contributing_area band is not supported for "
                                              "collection {c}".format(c=collection_id))
        if sar_backscatter_arguments.local_incidence_angle:
            raise FeatureUnsupportedException("sar_backscatter: local_incidence_angle band is not supported for "
                                              "collection {c}".format(c=collection_id))
        if sar_backscatter_arguments.ellipsoid_incidence_angle:
            raise FeatureUnsupportedException("sar_backscatter: ellipsoid_incidence_angle band is not supported for "
                                              "collection {c}".format(c=collection_id))

        # Tile size to use in the TiledRasterLayer.
        tile_size = sar_backscatter_arguments.options.get("tile_size", self._DEFAULT_TILE_SIZE)

        geopyspark.get_spark_context().setLocalProperty(
            "callSite.short",
            f"load_collection: SENTINEL1_GRD {from_date}-{to_date} Area: {get_area_in_square_kilometers(projected_polygons)}",
        )

        debug_mode = smart_bool(sar_backscatter_arguments.options.get("debug"))

        feature_pyrdd, layer_metadata_py,partitioner = self._build_feature_rdd(
            collection_id=collection_id, projected_polygons=projected_polygons,
            from_date=from_date, to_date=to_date, extra_properties=extra_properties,
            tile_size=tile_size, zoom=zoom, correlation_id=correlation_id,
            datacubeParams=datacubeParams, spatial_extent=spatial_extent,
            use_stac_client=use_stac_client
        )
        if debug_mode:
            self._debug_show_rdd_info(feature_pyrdd)

        prefix = ""
        if pathlib.Path("/vsis3").exists() and extra_properties.get("vsis3", "TRUE") != "FALSE":
            prefix = "/vsis3"

        # Group multiple tiles by product id
        def process_feature(feature: dict) -> Tuple[str, dict]:
            creo_path = prefix + feature["feature"]["id"]
            return creo_path, {
                "key": feature["key"],
                "key_extent": feature["key_extent"],
                "bbox": feature["feature"]["bbox"],
                "key_epsg": feature["metadata"]["crs_epsg"]
            }

        per_product = feature_pyrdd.map(process_feature).groupByKey().mapValues(list)
        all_keys = feature_pyrdd.map(lambda f:f["key"]).collect()

        paths = list(per_product.keys().collect())
        def partitionByPath(tuple):
            try:
                return paths.index(tuple)
            except Exception as e:
                hashPartitioner = pyspark.rdd.portable_hash
                return hashPartitioner(tuple)
        grouped = per_product.partitionBy(per_product.count(),partitionByPath)

        geopyspark.get_spark_context().setLocalProperty(
            "callSite.short",
            f"sar_backscatter: {sar_backscatter_arguments.coefficient} {from_date}-{to_date} Area: {get_area_in_square_kilometers(projected_polygons)}",
        )
        #local = grouped.collect()

        #print(local)
        orfeo_function = S1BackscatterOrfeo._get_process_function(
            sar_backscatter_arguments, result_dtype, bands, S1BackscatterOrfeo._get_trackers(per_product.context),
            max_soft_errors_ratio
        )

        tile_rdd = grouped.flatMap(orfeo_function)
        #tile_rdd = list(map(orfeo_function,local))
        if result_dtype:
            layer_metadata_py.cell_type = geopyspark.CellType.create_user_defined_celltype(result_dtype,0)
        logger.info("Constructing TiledRasterLayer from numpy rdd, with metadata {m!r}".format(m=layer_metadata_py))
        tile_layer = geopyspark.TiledRasterLayer.from_numpy_rdd(
            layer_type=geopyspark.LayerType.SPACETIME,
            numpy_rdd=tile_rdd,
            metadata=layer_metadata_py
        )
        jvm = get_jvm()
        p = jvm.org.openeo.geotrellis.OpenEOProcesses()
        spk = jvm.geotrellis.layer.SpaceTimeKey

        indexReduction = datacubeParams.partitionerIndexReduction().get() if datacubeParams is not None and datacubeParams.partitionerIndexReduction().isDefined() else 8

        keys_geotrellis = [ spk(k["col"], k["row"], k["instant"]) for k in all_keys]
        result = p.applySparseSpacetimePartitioner(tile_layer.srdd.rdd(),
                                                   keys_geotrellis,
                                                   indexReduction)
        contextRDD = jvm.geotrellis.spark.ContextRDD(result, tile_layer.srdd.rdd().metadata())
        merged_rdd = jvm.org.openeo.geotrellis.OpenEOProcesses().mergeTiles(contextRDD)

        srdd = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer.apply(jvm.scala.Option.apply(zoom), merged_rdd)
        tile_layer = geopyspark.TiledRasterLayer(geopyspark.LayerType.SPACETIME, srdd)
        logger.info(f"Created {collection_id} backscatter cube with partitioner index: {str(merged_rdd.partitioner().get().index())}")
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
    def _creodias_dem_subset_copernicus30_geotiff(
        bbox: Tuple, bbox_epsg: int, copernicus_root
    ) -> tempfile.TemporaryDirectory:
        """
        Create subset of Creodias DEM symlinks covering the given lon-lat bbox to pass to Orfeo
        based on the geotiff DEM tiles at /eodata/auxdata/CopDEM_COG/copernicus-dem-30m/
        (e.g. Copernicus_DSM_COG_10_N80_00_W103_00_DEM.tif)
        (e.g. Copernicus_DSM_COG_10_S01_00_E006_00_DEM.tif)
        (e.g. Copernicus_DSM_COG_10_N00_00_E000_00_DEM.tif)

        :return: tempfile.TemporaryDirectory to be used as context manager (for automatic cleanup)
        """
        # Convert bbox to degrees (EPSG:4326).
        bbox_lonlat: shapely.geometry.Polygon = shapely.ops.transform(
            pyproj.Transformer.from_crs(
                crs_from=bbox_epsg, crs_to=4326, always_xy=True
            ).transform,
            shapely.geometry.box(*bbox),
        )
        # Divide bbox_lonlat.bounds into 1x1 degree tiles, get the indices (northing, easting) for each tile.
        tile_indices = []
        import math

        (xmin, ymin, xmax, ymax) = bbox_lonlat.bounds
        for lon in range(math.floor(xmin), math.ceil(xmax)):
            for lat in range(math.floor(ymin), math.ceil(ymax)):
                tile_indices.append((lat, lon))

        # Set up temp symlinks.
        temp_dir = tempfile.TemporaryDirectory(
            suffix="-openeo-dem-copernicus30-geotiff"
        )
        root = pathlib.Path(temp_dir.name)
        logger.info(
            "Creating temporary DEM tile subset directory for {b} (epsg {e}): {r!s} symlinking to {t}".format(
                b=bbox, e=bbox_epsg, r=root, t=copernicus_root
            )
        )
        for lat, lon in tile_indices:
            lat_char = "N" if lat >= 0 else "S"
            lon_char = "E" if lon >= 0 else "W"
            tile_name = "Copernicus_DSM_COG_10_{lat_char}{lat:02d}_00_{lon_char}{lon:03d}_00_DEM".format(
                lat_char=lat_char, lat=abs(lat), lon_char=lon_char, lon=abs(lon)
            )
            source_path = pathlib.Path(copernicus_root, tile_name, tile_name + ".tif")
            dest_path = pathlib.Path(root, tile_name + ".tif")
            if not source_path.exists():
                continue
            dest_path.symlink_to(source_path)
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
        to_lonlat = pyproj.Transformer.from_crs(
            crs_from=bbox_epsg, crs_to=4326, always_xy=True
        )
        bbox_lonlat = shapely.ops.transform(
            to_lonlat.transform, shapely.geometry.box(*bbox)
        ).bounds
        lon_min, lat_min, lon_max, lat_max = [int(b) for b in bbox_lonlat]

        # Unzip to temp dir
        temp_dir = tempfile.TemporaryDirectory(suffix="-openeo-dem-srtm")
        msg = f"Unzip SRTM tiles from {srtm_root} in range lon [{lon_min}:{lon_max}] x lat [{lat_min}:{lat_max}] to {temp_dir}"
        with TimingLogger(title=msg, logger=logger):
            for lon in range(lon_min, lon_max + 1):
                for lat in range(lat_min, lat_max + 1):
                    # Something like: N50E003.SRTMGL1.hgt.zip"
                    basename = "{ns}{lat:02d}{ew}{lon:03d}.SRTMGL1.hgt".format(
                        ew="E" if lon >= 0 else "W",
                        lon=abs(lon),
                        ns="N" if lat >= 0 else "S",
                        lat=abs(lat),
                    )
                    zip_filename = pathlib.Path(srtm_root) / (basename + ".zip")
                    with zipfile.ZipFile(zip_filename, "r") as z:
                        logger.info(f"{zip_filename}: {z.infolist()}")
                        z.extractall(temp_dir.name)

        return temp_dir


    @staticmethod
    def _get_trackers(spark_context):
        if in_batch_job_context():
            # Trackers are only used for batch jobs, and they are global to that job.
            if S1BackscatterOrfeo._trackers is None:
                from openeogeotrellis.metrics_tracking import global_tracker
                metrics_tracker = global_tracker()
                S1BackscatterOrfeo._trackers = (
                    metrics_tracker.register_counter(_EXECUTION_TRACKER_ID), # nr_execution_tracker
                    metrics_tracker.register_counter(_SOFT_ERROR_TRACKER_ID), # nr_error_tracker
                    metrics_tracker.register_counter(_INPUTPIXELS_TRACKER_ID),  # nr_error_tracker
                )
        return S1BackscatterOrfeo._trackers


class S1BackscatterOrfeoV2(S1BackscatterOrfeo):
    """
    EP-3730 optimization: instead of splitting input image in tiles and applying Orfeo pipeline to each tile,
    do Orfeo processing on input image once and split up in tiles afterwards.
    """

    def creodias(
            self,
            projected_polygons,
            from_date: str, to_date: str,
            collection_id: str = "Sentinel1",
            correlation_id: str = "NA",
            sar_backscatter_arguments: SarBackscatterArgs = SarBackscatterArgs(),
            bands=None,
            zoom=0,  # TODO: what to do with zoom? It is not used at the moment.
            result_dtype="float32",
            extra_properties={},
            datacubeParams=None,
            max_soft_errors_ratio=0.0,
            spatial_extent=None,
            use_stac_client: bool = True
    ) -> Dict[int, geopyspark.TiledRasterLayer]:
        """
        Implementation of S1 backscatter calculation with Orfeo in Creodias environment
        """

        # Initial argument checking
        bands = bands or ["VH", "VV"]
        sar_calibration_lut = self._get_sar_calibration_lut(sar_backscatter_arguments.coefficient)
        if sar_backscatter_arguments.mask:
            raise FeatureUnsupportedException("sar_backscatter: mask band is not supported for "
                                              "collection {c}".format(c=collection_id))
        if sar_backscatter_arguments.contributing_area:
            raise FeatureUnsupportedException("sar_backscatter: contributing_area band is not supported for "
                                              "collection {c}".format(c=collection_id))
        if sar_backscatter_arguments.local_incidence_angle:
            raise FeatureUnsupportedException("sar_backscatter: local_incidence_angle band is not supported for "
                                              "collection {c}".format(c=collection_id))
        if sar_backscatter_arguments.ellipsoid_incidence_angle:
            raise FeatureUnsupportedException("sar_backscatter: ellipsoid_incidence_angle band is not supported for "
                                              "collection {c}".format(c=collection_id))

        # Tile size to use in the TiledRasterLayer.
        tile_size = sar_backscatter_arguments.options.get("tile_size", self._DEFAULT_TILE_SIZE)
        max_processing_area_pixels = sar_backscatter_arguments.options.get("max_processing_area_pixels", 2048)
        orfeo_memory = sar_backscatter_arguments.options.get("otb_memory", 256)

        # Geoid for orthorectification: get from options, fallback on config.
        elev_geoid = (
            sar_backscatter_arguments.options.get("elev_geoid") or get_backend_config().s1backscatter_elev_geoid
        )
        elev_default = sar_backscatter_arguments.options.get("elev_default")
        logger.info(f"elev_geoid: {elev_geoid!r}")

        noise_removal = bool(sar_backscatter_arguments.noise_removal)
        debug_mode = smart_bool(sar_backscatter_arguments.options.get("debug"))

        geopyspark.get_spark_context().setLocalProperty(
            "callSite.short",
            f"load_collection: SENTINEL1_GRD {from_date}-{to_date} Area: {get_area_in_square_kilometers(projected_polygons)}",
        )

        # an RDD of Python objects (basically SpaceTimeKey + feature) with gps.Metadata
        target_resolution = sar_backscatter_arguments.options.get("resolution", (10.0, 10.0))
        feature_pyrdd, layer_metadata_py,partitioner = self._build_feature_rdd(
            collection_id=collection_id, projected_polygons=projected_polygons,
            from_date=from_date, to_date=to_date, extra_properties=extra_properties,
            tile_size=tile_size, zoom=zoom, correlation_id=correlation_id,
            datacubeParams=datacubeParams, resolution=target_resolution,
            spatial_extent=spatial_extent, use_stac_client=use_stac_client
        )
        if debug_mode:
            self._debug_show_rdd_info(feature_pyrdd)
        trackers = S1BackscatterOrfeo._get_trackers(feature_pyrdd.context)

        prefix = ""
        if pathlib.Path("/vsis3").exists() and extra_properties.get("vsis3","TRUE") != "FALSE":
            prefix = "/vsis3"

        # Group multiple tiles by product id
        def process_feature(feature: dict) -> Tuple[str, dict]:
            creo_path = prefix + feature["feature"]["id"]
            return creo_path, {
                "key": feature["key"],
                "key_extent": feature["key_extent"],
                "bbox": feature["feature"]["bbox"],
                "key_epsg": feature["metadata"]["crs_epsg"]
            }

        # a pair RDD of product -> tile
        per_product = feature_pyrdd.map(process_feature).groupByKey().mapValues(list)

        # TODO: still split if full layout extent is too large for processing as a whole?

        # Apply Orfeo processing over product files as whole and splice up in tiles after that
        @ensure_executor_logging
        @TimingLogger(title="process_product", logger=logger)
        def process_product(product: Tuple[str, List[dict]]):
            import faulthandler;
            faulthandler.enable()
            creo_path, features = product

            # Short ad-hoc product id for logging purposes.
            prod_id = re.sub(r"[^A-Z0-9]", "", creo_path.upper())[-10:]
            log_prefix = f"p{os.getpid()}-prod{prod_id}"
            logger.info(f"{log_prefix} creo path {creo_path}")
            logger.info(f"{log_prefix} sar_backscatter_arguments: {sar_backscatter_arguments!r}")

            creo_path = pathlib.Path(creo_path)
            if not creo_path.exists():
                raise OpenEOApiException(f"sar_backscatter: path {creo_path} does not exist on the cluster.")

            full_product_download = smart_bool(sar_backscatter_arguments.options.get("local_copy", False))

            if full_product_download:
                logger.debug(f"{log_prefix} Download full product {creo_path} to local temp dir for processing")
                tempdir = tempfile.mkdtemp()
                download_s3_directory("s3:/" + str(creo_path).replace("/vsis3/", "/"), tempdir)
                creo_path = pathlib.Path(tempdir).joinpath(*creo_path.parts[3:])


            # Get whole extent of tile layout
            col_min = min(f["key"]["col"] for f in features)
            col_max = max(f["key"]["col"] for f in features)
            cols = col_max - col_min + 1
            row_min = min(f["key"]["row"] for f in features)
            row_max = max(f["key"]["row"] for f in features)
            rows = row_max - row_min + 1

            MAX_KEYS = int(max_processing_area_pixels / tile_size)

            instants = set(f["key"]["instant"] for f in features)
            assert len(instants) == 1, f"Not single instant: {instants}"
            instant = instants.pop()
            logger.info(
                f"{log_prefix} Layout key extent: col[{col_min}:{col_max}] row[{row_min}:{row_max}]"
                f" ({cols}x{rows}={cols * rows} tiles) instant[{instant}]."
            )

            layout_extent = get_total_extent(features)

            key_epsgs = set(f["key_epsg"] for f in features)
            assert len(key_epsgs) == 1, f"Multiple key CRSs {key_epsgs}"
            layout_epsg = key_epsgs.pop()


            band_tiffs = S1BackscatterOrfeo._creo_scan_for_band_tiffs(creo_path, log_prefix)
            if not band_tiffs:
                return []


            dem_dir_context = S1BackscatterOrfeo._get_dem_dir_context(
                sar_backscatter_arguments=sar_backscatter_arguments,
                extent=layout_extent,
                epsg=layout_epsg
            )

            msg = f"{log_prefix} Process {creo_path} "

            tiles = []

            with dem_dir_context as dem_dir:
                for col_start in range(col_min, col_max+1, MAX_KEYS):
                    for row_start in range(row_min, row_max+1, MAX_KEYS):
                        col_end = min(col_start + MAX_KEYS - 1, col_max)
                        row_end = min(row_start + MAX_KEYS - 1, row_max)

                        tiles_subset = [f for f in features if
                                        f["key"]["col"] >= col_start and f["key"]["col"] <= col_end and f["key"][
                                            "row"] >= row_start and f["key"]["row"] <= row_end]

                        if len(tiles_subset) == 0:
                            continue

                        #it is possible that the bounds of subset are smaller than the iteration bounds
                        col_start_local = min(f["key"]["col"] for f in tiles_subset)
                        col_end_local   = max(f["key"]["col"] for f in tiles_subset)
                        row_start_local = min(f["key"]["row"] for f in tiles_subset)
                        row_end_local   = max(f["key"]["row"] for f in tiles_subset)

                        layout_subextent = get_total_extent(tiles_subset)

                        layout_width_px = tile_size * (col_end_local - col_start_local + 1)
                        layout_height_px = tile_size * (row_end_local - row_start_local + 1)
                        logger.info(
                            f"{log_prefix} Layout extent {layout_subextent} EPSG {layout_epsg}:"
                            f" {layout_width_px}x{layout_height_px}px"
                        )

                        with TimingLogger(title=msg, logger=logger):
                            # Allocate numpy array tile
                            orfeo_bands = []

                            for b, band in enumerate(bands):
                                if band.lower() not in band_tiffs:
                                    raise OpenEOApiException(f"{log_prefix} sar_backscatter: No tiff for band {band} in {creo_path}")
                                data, nodata = S1BackscatterOrfeoV2._orfeo_pipeline(
                                    input_tiff=band_tiffs[band.lower()],
                                    extent=layout_subextent, extent_epsg=layout_epsg,
                                    dem_dir=dem_dir,
                                    extent_width_px=layout_width_px, extent_height_px=layout_height_px,
                                    sar_calibration_lut=sar_calibration_lut,
                                    noise_removal=noise_removal,
                                    elev_geoid=elev_geoid, elev_default=elev_default,
                                    log_prefix=f"{log_prefix}-{band}",
                                    orfeo_memory=orfeo_memory,
                                    trackers=trackers,
                                    max_soft_errors_ratio = max_soft_errors_ratio,
                                    target_resolution = target_resolution
                                )
                                orfeo_bands.append(data)

                            if sar_backscatter_arguments.options.get("to_db", False):
                                # TODO: keep this "to_db" shortcut feature or drop it
                                #       and require user to use standard openEO functionality (`apply` based conversion)?
                                logger.info(f"{log_prefix} Converting backscatter intensity to decibel")
                                orfeo_bands = 10 * numpy.log10(orfeo_bands)

                            # Split orfeo output in tiles


                            if isinstance(orfeo_bands[0],str):
                                import rasterio
                                from rasterio.windows import Window

                                ds = [rasterio.open(filename,driver="GTiff") for filename in orfeo_bands if os.path.exists(filename)]
                                if len(ds) == len(bands):
                                    for f in tiles_subset:
                                        col = f["key"]["col"]
                                        row = f["key"]["row"]
                                        c = col - col_start_local
                                        r = row - row_start_local

                                        key = geopyspark.SpaceTimeKey(col=col, row=row, instant=_instant_ms_to_day(instant))
                                        numpy_tiles = numpy.array([band.read(1,window=Window(c * tile_size,r * tile_size,tile_size,tile_size))
                                                        for band in ds])
                                        cell_type = geopyspark.CellType(numpy_tiles[0].dtype.name)
                                        if not (numpy_tiles==nodata).all():
                                            if debug_mode:
                                                logger.info(f"{log_prefix} Create Tile for key {key} from {numpy_tiles.shape}")
                                            tile = geopyspark.Tile(numpy_tiles, cell_type, no_data_value=nodata)
                                            tiles.append((key, tile))
                                    ds = None
                                    for file in orfeo_bands:
                                        os.remove(file)

                            else:
                                orfeo_bands = numpy.array(orfeo_bands)
                                cell_type = geopyspark.CellType(orfeo_bands.dtype.name)
                                logger.info(f"{log_prefix} Split {orfeo_bands.shape} in tiles of {tile_size}")
                                for f in tiles_subset:
                                    col = f["key"]["col"]
                                    row = f["key"]["row"]
                                    c = col - col_start_local
                                    r = row - row_start_local
                                    key = geopyspark.SpaceTimeKey(col=col, row=row, instant=_instant_ms_to_day(instant))
                                    tile = orfeo_bands[:, r * tile_size:(r + 1) * tile_size, c * tile_size:(c + 1) * tile_size]
                                    if not (tile==nodata).all():
                                        if debug_mode:
                                            logger.info(f"{log_prefix} Create Tile for key {key} from {tile.shape}")
                                        tile = geopyspark.Tile(tile, cell_type, no_data_value=nodata)
                                        tiles.append((key, tile))

            if full_product_download:
                shutil.rmtree(creo_path)
            logger.info(f"{log_prefix} Layout extent split in {len(tiles)} tiles")
            return tiles

        paths = list(per_product.keys().collect())

        def partitionByPath(tuple):
            try:
                return paths.index(tuple)
            except Exception as e:
                hashPartitioner = pyspark.rdd.portable_hash
                return hashPartitioner(tuple)

        grouped = per_product.partitionBy(per_product.count(), partitionByPath)
        geopyspark.get_spark_context().setLocalProperty(
            "callSite.short",
            f"sar_backscatter: {sar_backscatter_arguments.coefficient} {from_date}-{to_date} Area: {get_area_in_square_kilometers(projected_polygons)}",
        )
        tile_rdd = grouped.flatMap(process_product)
        if result_dtype:
            layer_metadata_py.cell_type = geopyspark.CellType.create_user_defined_celltype(result_dtype,0)
        logger.info("Constructing TiledRasterLayer from numpy rdd, with metadata {m!r}".format(m=layer_metadata_py))
        tile_layer = geopyspark.TiledRasterLayer.from_numpy_rdd(
            layer_type=geopyspark.LayerType.SPACETIME,
            numpy_rdd=tile_rdd,
            metadata=layer_metadata_py
        )

        the_rdd = tile_layer.srdd

        logger.info(f"sar-backscatter: partitioning with {str(partitioner)}")
        if(partitioner is not None):
            the_rdd = the_rdd.partitionByPartitioner(partitioner)

        # Merge any keys that have more than one tile.
        contextRDD = self.jvm.org.openeo.geotrellis.OpenEOProcesses().mergeTiles(the_rdd.rdd())
        temporal_tiled_raster_layer = self.jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        srdd = temporal_tiled_raster_layer.apply(self.jvm.scala.Option.apply(zoom), contextRDD)
        merged_tile_layer = geopyspark.TiledRasterLayer(geopyspark.LayerType.SPACETIME, srdd)

        return {zoom: merged_tile_layer}


def get_implementation(version: str = "1", jvm=None) -> S1BackscatterOrfeo:
    jvm = jvm or get_jvm()
    if version == "1":
        logger.info("using S1BackscatterOrfeo")
        return S1BackscatterOrfeo(jvm=jvm)
    elif version == "2":
        logger.info("using S1BackscatterOrfeoV2")
        return S1BackscatterOrfeoV2(jvm=jvm)
    else:
        raise ValueError(version)
