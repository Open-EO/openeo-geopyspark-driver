import json
import logging
import os
import pathlib
import re
import tempfile
import zipfile
from datetime import datetime, date
from typing import List, Optional, Callable, Dict, Tuple, Union

import epsel
import geopyspark
import numpy
import pyproj
import pyspark
import shapely.geometry
import shapely.ops
from py4j.java_gateway import JavaGateway, JVMView, JavaObject
from shapely.geometry import box

from openeo.util import TimingLogger, dict_no_none, Rfc3339
from openeo_driver.backend import CollectionCatalog, LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import ProcessGraphComplexityException, OpenEOApiException, FeatureUnsupportedException
from openeo_driver.utils import read_json, EvalEnv
from openeogeotrellis._utm import auto_utm_epsg_for_geometry, utm_zone_from_epsg
from openeogeotrellis.catalogs.creo import CatalogClient
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.opensearch import OpenSearch
from openeogeotrellis.utils import kerberos, dict_merge_recursive, normalize_date, to_projected_polygons, \
    lonlat_to_mercator_tile_indices, nullcontext

logger = logging.getLogger(__name__)


def get_jvm() -> JVMView:
    pysc = geopyspark.get_spark_context()
    gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
    jvm = gateway.jvm
    return jvm


class GeoPySparkLayerCatalog(CollectionCatalog):

    def __init__(self, all_metadata: List[dict]):
        super().__init__(all_metadata=all_metadata)
        self._geotiff_pyramid_factories = {}

    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> GeopysparkDataCube:
        logger.info("Creating layer for {c} with load params {p}".format(c=collection_id, p=load_params))

        # TODO is it necessary to do this kerberos stuff here?
        kerberos()

        metadata = GeopysparkCubeMetadata(self.get_collection_metadata(collection_id))
        layer_source_info = metadata.get("_vito", "data_source", default={})
        layer_source_type = layer_source_info.get("type", "Accumulo").lower()
        native_crs = layer_source_info.get("native_crs","UTM")
        postprocessing_band_graph = metadata.get("_vito", "postprocessing_bands", default=None)
        logger.info("Layer source type: {s!r}".format(s=layer_source_type))

        temporal_extent = load_params.temporal_extent
        from_date, to_date = [normalize_date(d) for d in temporal_extent]
        metadata = metadata.filter_temporal(from_date, to_date)

        spatial_extent = load_params.spatial_extent
        west = spatial_extent.get("west", None)
        east = spatial_extent.get("east", None)
        north = spatial_extent.get("north", None)
        south = spatial_extent.get("south", None)
        srs = spatial_extent.get("crs", None)
        if isinstance(srs, int):
            srs = 'EPSG:%s' % str(srs)
        if srs is None:
            srs = 'EPSG:4326'

        bands = load_params.bands
        if bands:
            band_indices = [metadata.get_band_index(b) for b in bands]
            metadata = metadata.filter_bands(bands)
        else:
            band_indices = None
        logger.info("band_indices: {b!r}".format(b=band_indices))
        # TODO: avoid this `still_needs_band_filter` ugliness.
        #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        still_needs_band_filter = False

        correlation_id = env.get("correlation_id", '')
        logger.info("Correlation ID is '{cid}'".format(cid=correlation_id))

        experimental = load_params.get("featureflags",{}).get("experimental",False)

        jvm = get_jvm()

        extent = None
        spatial_bounds_present = all(b is not None for b in [west, south, east, north])
        if spatial_bounds_present:
            extent = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))
            metadata = metadata.filter_bbox(west=west, south=south, east=east, north=north, crs=srs)
        elif env.get('require_bounds', False):
            raise ProcessGraphComplexityException
        else:
            srs = "EPSG:4326"
            extent = jvm.geotrellis.vector.Extent(-180.0, -90.0, 180.0, 90.0)

        polygons = load_params.aggregate_spatial_geometries

        if not polygons:
            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, srs)
        else:
            projected_polygons = to_projected_polygons(jvm, polygons)

        if spatial_bounds_present:
            if( native_crs == 'UTM'):
                target_epsg_code = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
            else:
                target_epsg_code = int(native_crs.split(":")[-1])
            projected_polygons_native_crs = jvm.org.openeo.geotrellis.ProjectedPolygons.reproject(projected_polygons, target_epsg_code)

        single_level = env.get('pyramid_levels', 'all') != 'all'

        def accumulo_pyramid():
            pyramidFactory = jvm.org.openeo.geotrellisaccumulo.PyramidFactory("hdp-accumulo-instance",
                                                                              ','.join(ConfigParams().zookeepernodes))
            if layer_source_info.get("split", False):
                pyramidFactory.setSplitRanges(True)

            accumulo_layer_name = layer_source_info['data_id']
            nonlocal still_needs_band_filter
            still_needs_band_filter = bool(band_indices)

            polygons = load_params.aggregate_spatial_geometries

            if polygons:
                projected_polygons = to_projected_polygons(jvm, polygons)
                return pyramidFactory.pyramid_seq(accumulo_layer_name, projected_polygons.polygons(),
                                                  projected_polygons.crs(), from_date, to_date)
            else:
                return pyramidFactory.pyramid_seq(accumulo_layer_name, extent, srs, from_date, to_date)

        def s3_pyramid():
            endpoint = layer_source_info['endpoint']
            region = layer_source_info['region']
            bucket_name = layer_source_info['bucket_name']
            nonlocal still_needs_band_filter
            still_needs_band_filter = bool(band_indices)
            return jvm.org.openeo.geotrelliss3.PyramidFactory(endpoint, region, bucket_name) \
                .pyramid_seq(extent, srs, from_date, to_date)

        def s3_jp2_pyramid():
            endpoint = layer_source_info['endpoint']
            region = layer_source_info['region']

            return jvm.org.openeo.geotrelliss3.Jp2PyramidFactory(endpoint, region) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def file_s2_radiometry_pyramid():
            return jvm.org.openeo.geotrellis.file.Sentinel2RadiometryPyramidFactory() \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def file_s2_pyramid():
            return file_pyramid(lambda opensearch_endpoint, opensearch_collection_id, opensearch_link_titles, root_path:
                                jvm.org.openeo.geotrellis.file.Sentinel2PyramidFactory(opensearch_endpoint,
                                                                                       opensearch_collection_id,
                                                                                       opensearch_link_titles,
                                                                                       root_path,
                                                                                       jvm.geotrellis.raster.CellSize(
                                                                                           10.0,
                                                                                           10.0),
                                                                                       experimental
                                                                                       ))

        def file_s5p_pyramid():
            return file_pyramid(jvm.org.openeo.geotrellis.file.Sentinel5PPyramidFactory)

        def file_probav_pyramid():
            opensearch_endpoint = layer_source_info.get('opensearch_endpoint',
                                                        ConfigParams().default_opensearch_endpoint)

            return jvm.org.openeo.geotrellis.file.ProbaVPyramidFactory(opensearch_endpoint,
                layer_source_info.get('opensearch_collection_id'), layer_source_info.get('root_path')) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices, correlation_id)

        def file_pyramid(pyramid_factory):
            opensearch_endpoint = layer_source_info.get('opensearch_endpoint',
                                                        ConfigParams().default_opensearch_endpoint)
            opensearch_collection_id = layer_source_info['opensearch_collection_id']
            opensearch_link_titles = metadata.band_names
            root_path = layer_source_info['root_path']

            def extract_literal_match(condition) -> (str, object):
                # in reality, each of these conditions should be evaluated against elements (products) of this
                # collection = evaluated with the product's "value" parameter in the environment, to true (include)
                # or false (exclude)
                # however, this would require evaluating in the Sentinel2FileLayerProvider, because this is the one
                # that has access to this value (callers only get a MultibandTileLayerRDD[SpaceTimeKey])

                from openeo.internal.process_graph_visitor import ProcessGraphVisitor

                class LiteralMatchExtractingGraphVisitor(ProcessGraphVisitor):
                    def __init__(self):
                        super().__init__()
                        self.property_value = None

                    def enterProcess(self, process_id: str, arguments: dict):
                        if process_id != 'eq':
                            raise NotImplementedError("process %s is not supported" % process_id)

                    def enterArgument(self, argument_id: str, value):
                        assert value['from_parameter'] == 'value'

                    def constantArgument(self, argument_id: str, value):
                        if argument_id in ['x', 'y']:
                            self.property_value = value

                if isinstance(condition, dict) and 'process_graph' in condition:
                    predicate = condition['process_graph']
                    property_value = LiteralMatchExtractingGraphVisitor().accept_process_graph(predicate).property_value
                    return property_value
                else:
                    return condition

            layer_properties = metadata.get("_vito", "properties", default={})
            custom_properties = load_params.properties

            metadata_properties = {property_name: extract_literal_match(condition)
                                   for property_name, condition in {**layer_properties, **custom_properties}.items()}

            factory = pyramid_factory(opensearch_endpoint, opensearch_collection_id, opensearch_link_titles, root_path)

            if single_level:
                #TODO EP-3561 UTM is not always the native projection of a layer (PROBA-V), need to determine optimal projection
                return factory.datacube_seq(projected_polygons_native_crs, from_date, to_date, metadata_properties, correlation_id)
            else:
                if polygons:
                    return factory.pyramid_seq(projected_polygons.polygons(), projected_polygons.crs(), from_date,
                                               to_date, metadata_properties, correlation_id)
                else:
                    return factory.pyramid_seq(extent, srs, from_date, to_date, metadata_properties, correlation_id)

        def geotiff_pyramid():
            glob_pattern = layer_source_info['glob_pattern']
            date_regex = layer_source_info['date_regex']

            new_pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex)

            return self._geotiff_pyramid_factories.setdefault(collection_id, new_pyramid_factory) \
                .pyramid_seq(extent, srs, from_date, to_date)

        def sentinel_hub_pyramid():
            dependencies = env.get('dependencies', {})

            logger.info("Sentinel Hub pyramid from dependencies {ds}".format(ds=dependencies))

            if dependencies:
                batch_request_id = dependencies[collection_id]
                key_regex = r".*\.tif"
                date_regex = r".*_(\d{4})(\d{2})(\d{2}).tif"
                recursive = True
                interpret_as_cell_type = "float32ud0"

                pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_s3(
                    "s3://{b}/{i}/".format(b=ConfigParams().sentinel_hub_batch_bucket, i=batch_request_id),
                    key_regex,
                    date_regex,
                    recursive,
                    interpret_as_cell_type
                )

                return (pyramid_factory.datacube_seq(projected_polygons_native_crs, None, None) if single_level
                        else pyramid_factory.pyramid_seq(extent, srs, None, None))
            else:
                dataset_id = layer_source_info['dataset_id']
                client_id = layer_source_info['client_id']
                client_secret = layer_source_info['client_secret']
                sample_type = jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
                    layer_source_info.get('sample_type', 'UINT16'))

                pyramid_factory = jvm.org.openeo.geotrellissentinelhub.PyramidFactory(dataset_id, client_id, client_secret,
                                                                                      sample_type)

                return (
                    pyramid_factory.datacube_seq(projected_polygons_native_crs.polygons(), projected_polygons_native_crs.crs(), from_date,
                                                 to_date,metadata.band_names) if single_level
                    else pyramid_factory.pyramid_seq(extent, srs, from_date, to_date, metadata.band_names))

        def creo_pyramid():
            mission = layer_source_info['mission']
            level = layer_source_info['level']
            catalog = CatalogClient(mission, level)
            product_paths = catalog.query_product_paths(datetime.strptime(from_date[:10], "%Y-%m-%d"),
                                                        datetime.strptime(to_date[:10], "%Y-%m-%d"),
                                                        ulx=west, uly=north,
                                                        brx=east, bry=south)
            return jvm.org.openeo.geotrelliss3.CreoPyramidFactory(product_paths, metadata.band_names) \
                .datacube_seq(projected_polygons_native_crs, from_date, to_date,{},collection_id)

        def file_cgls_pyramid():
            if len(metadata.band_names) != 1:
                raise ValueError("expected a single band name for collection {cid}, got {bs} instead".format(
                    cid=collection_id, bs=metadata.band_names))

            data_glob = layer_source_info['data_glob']
            band_name = metadata.band_names[0].upper()
            date_regex = layer_source_info['date_regex']

            factory = jvm.org.openeo.geotrellis.file.CglsPyramidFactory(data_glob, band_name, date_regex)

            return (
                factory.datacube_seq(projected_polygons, from_date, to_date) if single_level
                else factory.pyramid_seq(projected_polygons.polygons(), projected_polygons.crs(), from_date, to_date)
            )

        def file_agera5_pyramid():
            data_glob = layer_source_info['data_glob']
            band_file_markers = metadata.band_names
            date_regex = layer_source_info['date_regex']

            factory = jvm.org.openeo.geotrellis.file.AgEra5PyramidFactory(data_glob, band_file_markers, date_regex)

            return (
                factory.datacube_seq(projected_polygons, from_date, to_date) if single_level
                else factory.pyramid_seq(projected_polygons.polygons(), projected_polygons.crs(), from_date, to_date)
            )

        logger.info("loading pyramid {s}".format(s=layer_source_type))
        if layer_source_type == 's3':
            pyramid = s3_pyramid()
        elif layer_source_type == 's3-jp2':
            pyramid = s3_jp2_pyramid()
        elif layer_source_type == 'file-s2-radiometry':
            pyramid = file_s2_radiometry_pyramid()
        elif layer_source_type == 'file-s2':
            pyramid = file_s2_pyramid()
        elif layer_source_type == 'file-s5p':
            pyramid = file_s5p_pyramid()
        elif layer_source_type == 'file-probav':
            pyramid = file_probav_pyramid()
        elif layer_source_type == 'geotiff':
            pyramid = geotiff_pyramid()
        elif layer_source_type == 'file-s1-coherence':
            pyramid = file_s2_pyramid()
        elif layer_source_type == 'sentinel-hub':
            pyramid = sentinel_hub_pyramid()
        elif layer_source_type == 'creo':
            pyramid = creo_pyramid()
        elif layer_source_type == 'file-cgls':
            pyramid = file_cgls_pyramid()
        elif layer_source_type == 'file-agera5':
            pyramid = file_agera5_pyramid()
        elif layer_source_type == 'creodias-s1-backscatter':
            pyramid = _S1BackscatterOrfeo(jvm=jvm).creodias(
                projected_polygons=projected_polygons_native_crs,
                from_date=from_date, to_date=to_date,
                correlation_id=correlation_id,
                sar_backscatter_arguments=load_params.sar_backscatter,
                bands=bands
            )
        else:
            pyramid = accumulo_pyramid()

        if isinstance(pyramid, dict):
            levels = pyramid
        else:
            temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
            option = jvm.scala.Option

            levels = {
                pyramid.apply(index)._1(): geopyspark.TiledRasterLayer(
                    geopyspark.LayerType.SPACETIME,
                    temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())
                )
                for index in range(0, pyramid.size())
            }

        if single_level:
            max_zoom = max(levels.keys())
            levels = {max_zoom: levels[max_zoom]}

        image_collection = GeopysparkDataCube(
            pyramid=geopyspark.Pyramid(levels),
            metadata=metadata
        )

        if (postprocessing_band_graph != None):
            from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
            visitor = GeotrellisTileProcessGraphVisitor()
            image_collection = image_collection.reduce_bands(visitor.accept_process_graph(postprocessing_band_graph))

        if still_needs_band_filter:
            # TODO: avoid this `still_needs_band_filter` ugliness.
            #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
            image_collection = image_collection.filter_bands(band_indices)

        return image_collection


class _S1BackscatterOrfeo:
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
                instant=datetime.utcfromtimestamp(key_sc.instant() // 1000)
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

        if sar_backscatter_arguments.backscatter_coefficient != "sigma0":
            raise OpenEOApiException(
                "Unsupported backscatter coefficient {c!r} (only 'sigma0' is supported).".format(
                    c=sar_backscatter_arguments.backscatter_coefficient))

        # Tile size to use in the TiledRasterLayer.
        tile_size = sar_backscatter_arguments.options.get("tile_size", 512)

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

        @epsel.ensure_info_logging
        def process_feature(feature):
            if not logging.root.handlers:
                logging.basicConfig(level=logging.INFO)

            col, row, instant = (feature["key"][k] for k in ["col", "row", "instant"])
            log_prefix = "p{p}-key({c},{r},{i}): ".format(p=os.getpid(), c=col, r=row, i=instant)

            key_ext = feature["key_extent"]
            key_epsg = feature["metadata"]["crs_epsg"]
            creo_path = pathlib.Path(feature["feature"]["id"])
            logger.info(log_prefix + f"Feature creo path: {creo_path}, key {key_ext} (EPSG {key_epsg})")
            if not creo_path.exists():
                raise OpenEOApiException("Creo path does not exist")

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

            if sar_backscatter_arguments.orthorectify:
                if sar_backscatter_arguments.elevation_model in [None, "SRTMGL1"]:
                    dem_dir_context = _S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
                        bbox=(key_ext["xmin"], key_ext["ymin"], key_ext["xmax"], key_ext["ymax"]), bbox_epsg=key_epsg,
                        srtm_root="/eodata/auxdata/SRTMGL1/dem",
                    )
                elif sar_backscatter_arguments.elevation_model in ["geotiff"]:
                    dem_dir_context = _S1BackscatterOrfeo._creodias_dem_subset_geotiff(
                        bbox=(key_ext["xmin"], key_ext["ymin"], key_ext["xmax"], key_ext["ymax"]), bbox_epsg=key_epsg,
                        zoom=sar_backscatter_arguments.options.get("dem_zoom_level", 10),
                        dem_tile_size=512,
                        dem_path_tpl="/eodata/auxdata/Elevation-Tiles/geotiff/{z}/{x}/{y}.tif"
                    )
                else:
                    raise FeatureUnsupportedException(
                        f"Unsupported elevation model {sar_backscatter_arguments.elevation_model!r}"
                    )

            else:
                # Context that returns None when entering
                dem_dir_context = nullcontext()

            with dem_dir_context as dem_dir:
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
                    logger.info(log_prefix + "Converting backscatter intensity to decibel")
                    tile_data = 10 * numpy.log10(tile_data)

                key = geopyspark.SpaceTimeKey(row=row, col=col, instant=datetime.utcfromtimestamp(instant // 1000))
                cell_type = geopyspark.CellType(tile_data.dtype.name)
                logger.info(log_prefix + f"Create Tile for key {key} from {tile_data.shape}")
                tile = geopyspark.Tile(tile_data, cell_type, no_data_value=nodata)
                return key, tile

        def orfeo_pipeline(
                input_tiff: pathlib.Path, key_extent, key_epsg, dem_dir: Union[str, None], tile_size: int = 512,
                log_prefix: str = ""
        ):
            logger.info(log_prefix + f"Input tiff {input_tiff}")
            logger.info(log_prefix + f"sar_backscatter_arguments: {sar_backscatter_arguments!r}")

            key_utm_zone, key_utm_northhem = utm_zone_from_epsg(key_epsg)
            logger.info(
                log_prefix + ("extent {e} (UTM {u}, EPSG {c})").format(e=key_extent, u=key_utm_zone, c=key_epsg))

            import otbApplication as otb

            def otb_param_dump(app):
                return {
                    p: str(v) if app.GetParameterType(p) == otb.ParameterType_Choice else v
                    for (p, v) in app.GetParameters().items()
                }

            with tempfile.TemporaryDirectory() as temp_dir:

                # SARCalibration
                sar_calibration = otb.Registry.CreateApplication('SARCalibration')
                sar_calibration.SetParameterString("in", str(input_tiff))
                sar_calibration.SetParameterValue('noise', True)
                sar_calibration.SetParameterInt('ram', 512)
                logger.info(log_prefix + f"SARCalibration params: {otb_param_dump(sar_calibration)}")
                sar_calibration.Execute()

                # OrthoRectification
                ortho_rect = otb.Registry.CreateApplication('OrthoRectification')
                ortho_rect.SetParameterInputImage("io.in", sar_calibration.GetParameterOutputImage("out"))
                if dem_dir:
                    ortho_rect.SetParameterString("elev.dem", dem_dir)
                if sar_backscatter_arguments.options.get("elev_geoid"):
                    ortho_rect.SetParameterString("elev.geoid", sar_backscatter_arguments.options.get("elev_geoid"))
                if sar_backscatter_arguments.options.get("elev_default"):
                    ortho_rect.SetParameterFloat(
                        "elev.default", float(sar_backscatter_arguments.options.get("elev_default"))
                    )
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
                logger.info(log_prefix + "Reading orfeo output tiff: {p}".format(p=out_path))
                with rasterio.open(out_path) as ds:
                    logger.info(log_prefix + "Output tiff metadata: {m}, bounds {b}".format(m=ds.meta, b=ds.bounds))
                    # TODO EP-3612: check band count. make sure we pick the right band.
                    # TODO EP-3612: also check projection/CRS...?
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
        logger.info(f"Unzip SRTM tiles from {srtm_root}"
                    f" in range lon [{lon_min}:{lon_max}] x lat [{lat_min}:{lat_max}] to {temp_dir}")
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

    def oscars(
            self,
            from_date: str, to_date: str,
            projected_polygons,
            collection_id: str = "urn:eop:VITO:CGS_S1_GRD_L1",
            correlation_id: str = "NA",
            # TODO: what to do with zoom? Highest level? lowest level?
            zoom=0,
    ):
        raise RuntimeError("WIP")

        # TODO openSearchLinkTitles?  attributeValues
        file_factory = self.jvm.org.openeo.geotrellis.file.FileRDDFactory.oscars(collection_id, [], {}, correlation_id)

        pyrdd, layer_metadata_sc = self._load_feature_rdd(
            file_factory, projected_polygons=projected_polygons, from_date=from_date, to_date=to_date, zoom=zoom,
            tile_size=tile_size
        )

        def load_data(metadata: str):
            # Oscars search response (passed as JSON dump)
            metadata = json.loads(metadata)

            # Get path to GRD zip file on disk
            grds = [link["href"]["file"] for link in metadata["feature"]["links"] if link["title"] == "GRD"]
            if len(grds) != 1:
                # TODO: raise exception?
                logger.error("One GRD link expected, but got {c}. Metadata: {m}".format(c=len(grds), m=metadata))
                return None
            grd_zip_path = grds[0]
            logger.info("GRD file: {g}".format(g=grd_zip_path))

            # Extract TIFF from zip
            with tempfile.TemporaryDirectory(suffix=".oeogps-s1bs") as work_dir:
                logger.info("Working in temp dir {t}".format(t=work_dir))

                with zipfile.ZipFile(grd_zip_path, 'r') as grd_zip:
                    regex = re.compile(r'.*/measurement/.*tiff?$')
                    tiffs = [p for p in grd_zip.infolist() if regex.match(p.filename)]
                    logger.info("{c} TIFF files in zip: {t}".format(c=len(tiffs), t=tiffs))
                    # TODO: use cube bands: VV/VH
                    tiff_name = tiffs[0]
                    with TimingLogger(title="Extract {t} from {z}".format(t=tiff_name, z=grd_zip_path), logger=logger):
                        tiff_path = grd_zip.extract(tiffs[0], path=work_dir)
                        raise RuntimeError("WIP")

        tile_layer = pyrdd.map(load_data)


def get_layer_catalog(get_opensearch: Callable[[str], OpenSearch] = None) -> GeoPySparkLayerCatalog:
    """
    Get layer catalog (from JSON files)
    """
    catalog_files = ConfigParams().layer_catalog_metadata_files
    logger.info("Reading layer catalog metadata from {f!r}".format(f=catalog_files[0]))
    local_metadata = read_json(catalog_files[0])

    if len(catalog_files) > 1:
        # Merge local metadata recursively
        metadata_by_layer_id = {layer["id"]: layer for layer in local_metadata}

        for path in catalog_files[1:]:
            logger.info("Updating layer catalog metadata from {f!r}".format(f=path))
            updates_by_layer_id = {layer["id"]: layer for layer in read_json(path)}
            metadata_by_layer_id = dict_merge_recursive(metadata_by_layer_id, updates_by_layer_id, overwrite=True)

        local_metadata = list(metadata_by_layer_id.values())

    if get_opensearch:
        opensearch_collections_cache = {}

        def get_opensearch_collections(endpoint: str) -> List[dict]:
            opensearch_collections = opensearch_collections_cache.get(endpoint)

            if opensearch_collections is None:
                opensearch = get_opensearch(endpoint)
                logger.info("Updating layer catalog metadata from {o!r}".format(o=opensearch))

                opensearch_collections = opensearch.get_collections()
                opensearch_collections_cache[endpoint] = opensearch_collections

            return opensearch_collections

        def derive_from_opensearch_collection_metadata(endpoint: str, collection_id: str) -> dict:
            rfc3339 = Rfc3339(propagate_none=True)
            collection = next((c for c in get_opensearch_collections(endpoint) if c["id"] == collection_id), None)

            if not collection:
                raise ValueError("unknown OSCARS collection {cid}".format(cid=collection_id))

            def transform_link(opensearch_link: dict) -> dict:
                return dict_no_none(
                    rel="alternate",
                    href=opensearch_link["href"],
                    title=opensearch_link.get("title")
                )

            def search_link(opensearch_link: dict) -> dict:
                from urllib.parse import urlparse, urlunparse

                def replace_endpoint(url: str) -> str:
                    components = urlparse(url)

                    return urlunparse(components._replace(
                        scheme="https",
                        netloc="services.terrascope.be",
                        path="/catalogue" + components.path
                    ))

                return dict_no_none(
                    rel="alternate",
                    href=replace_endpoint(opensearch_link["href"]),
                    title=opensearch_link.get("title")
                )

            def date_bounds() -> (date, Optional[date]):
                acquisition_information = collection["properties"]["acquisitionInformation"]
                earliest_start_date = None
                latest_end_date = None

                for info in acquisition_information:
                    start_datetime = rfc3339.parse_datetime(info["acquisitionParameters"]["beginningDateTime"])
                    end_datetime = rfc3339.parse_datetime(info["acquisitionParameters"].get("endingDateTime"))

                    if not earliest_start_date or start_datetime.date() < earliest_start_date:
                        earliest_start_date = start_datetime.date()

                    if end_datetime and (not latest_end_date or end_datetime.date() > latest_end_date):
                        latest_end_date = end_datetime.date()

                return earliest_start_date, latest_end_date

            earliest_start_date, latest_end_date = date_bounds()

            bands = collection["properties"].get("bands")

            return {
                "title": collection["properties"]["title"],
                "description": collection["properties"]["abstract"],
                "extent": {
                    "spatial": {"bbox": [collection["bbox"]]},
                    "temporal": {"interval": [
                        [earliest_start_date.isoformat(), latest_end_date.isoformat() if latest_end_date else None]
                    ]}
                },
                "links": [transform_link(l) for l in collection["properties"]["links"]["describedby"]] +
                         [search_link(l) for l in collection["properties"]["links"].get("search", [])],
                "cube:dimensions": {
                    "bands": {
                        "type": "bands",
                        "values": [band["title"] for band in bands] if bands else None
                    }
                },
                "summaries": {
                    "eo:bands": [dict(band, name=band["title"]) for band in bands] if bands else None
                }
            }

        opensearch_collection_sources = \
            {layer_id: collection_source for layer_id, collection_source in
             {l["id"]: l.get("_vito", {}).get("data_source", {}) for l in local_metadata}.items()
             if "opensearch_collection_id" in collection_source}

        opensearch_metadata_by_layer_id = {layer_id: derive_from_opensearch_collection_metadata(
            collection_source.get("opensearch_endpoint") or ConfigParams().default_opensearch_endpoint,
            collection_source["opensearch_collection_id"])
            for layer_id, collection_source in opensearch_collection_sources.items()}
    else:
        opensearch_metadata_by_layer_id = {}

    local_metadata_by_layer_id = {layer["id"]: layer for layer in local_metadata}

    return GeoPySparkLayerCatalog(
        all_metadata=list(dict_merge_recursive(
            opensearch_metadata_by_layer_id,
            local_metadata_by_layer_id,
            overwrite=True
        ).values()),
    )
