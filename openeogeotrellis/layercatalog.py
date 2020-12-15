import json
import logging
import os
import pathlib
import re
import tempfile
import zipfile
from datetime import datetime, date
from typing import List, Optional, Callable, Dict, Tuple

import geopyspark
import pyproj
import pyspark
from py4j.java_gateway import JavaGateway, JVMView, JavaObject
from shapely.geometry import box

from openeo.util import TimingLogger, dict_no_none, Rfc3339
from openeo_driver.backend import CollectionCatalog, LoadParameters
from openeo_driver.errors import ProcessGraphComplexityException, OpenEOApiException
from openeo_driver.utils import read_json, EvalEnv
from openeogeotrellis._utm import auto_utm_epsg_for_geometry
from openeogeotrellis.catalogs.creo import CatalogClient
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.opensearch import OpenSearch
from openeogeotrellis.utils import kerberos, dict_merge_recursive, normalize_date, to_projected_polygons

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
            # TODO: what to do with zoom? Highest level? lowest level?
            zoom=0,
            tile_size=512,
    ) -> Dict[int, geopyspark.TiledRasterLayer]:
        """
        Implementation of S1 backscatter
        :param projected_polygons:
        :param from_date:
        :param to_date:
        :param collection_id:
        :param correlation_id:
        :param zoom:
        :param tile_size:
        :return:
        """
        # TODO openSearchLinkTitles?
        attributeValues = {
            "productType": "GRD",
            "sensorMpde": "IW",
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

        def process_feature(feature):
            col, row, instant = (feature["key"][k] for k in ["col", "row", "instant"])

            key_extent = feature["key_extent"]
            key_crs = pyproj.CRS.from_epsg(feature["metadata"]["crs_epsg"])
            latlon_crs = pyproj.CRS.from_epsg(4326)
            south, west = pyproj.transform(key_crs, latlon_crs, x=key_extent["xmin"], y=key_extent["ymin"])
            north, east = pyproj.transform(key_crs, latlon_crs, x=key_extent["xmax"], y=key_extent["ymax"])

            creo_path = pathlib.Path(feature["feature"]["id"])
            logger.info("Feature creo path: {p}".format(p=creo_path))
            if not creo_path.exists():
                raise OpenEOApiException("Creo path does not exist")
            # TODO Get tiff path from manifest instead of assuming this subfolder format?
            tiffs = list(creo_path.glob("measurement/*.tiff"))
            if not tiffs:
                raise OpenEOApiException("No tiffs found")
            # TODO properly handle VV/VH bands
            input_tiff = tiffs[0]

            with tempfile.TemporaryDirectory() as temp_dir:
                import otbApplication as otb
                extractROI = otb.Registry.CreateApplication("ExtractROI")
                extractROI.SetParameterString("in", str(input_tiff))
                extractROI.SetParameterString("mode", "extent")
                extractROI.SetParameterString("mode.extent.unit", "lonlat")
                extractROI.SetParameterFloat("mode.extent.ulx", west)
                extractROI.SetParameterFloat("mode.extent.uly", south)
                extractROI.SetParameterFloat("mode.extent.lrx", east)
                extractROI.SetParameterFloat("mode.extent.lry", north)
                extractROI.Execute()

                # TODO: extract numpy array directly (instead of through on disk files)
                #       with GetImageAsNumpyArray (https://www.orfeo-toolbox.org/CookBook/PythonAPI.html#numpy-array-processing)
                #       but requires orfeo toolbox to be compiled with numpy support
                #       (numpy header files must be available at compile time I guess)

                out_path = os.path.join(temp_dir, "out.tiff")
                extractROI.SetParameterString("out", out_path)
                # TODO: add SARCalibration and OrthoRectification too
                extractROI.ExecuteAndWriteOutput()

                import rasterio
                with rasterio.open(out_path) as ds:
                    # TODO: check band count. make sure we pick the right band.
                    # TODO: also check projection/CRS...?
                    data = ds.read(1)
                    nodata = ds.nodata

            key = geopyspark.SpaceTimeKey(row=row, col=col, instant=datetime.utcfromtimestamp(instant // 1000))
            tile = geopyspark.Tile(data, geopyspark.CellType.FLOAT32, no_data_value=nodata)
            return key, tile

        tile_rdd = feature_pyrdd.map(process_feature)
        tile_layer = geopyspark.TiledRasterLayer.from_numpy_rdd(
            layer_type=geopyspark.LayerType.SPACETIME,
            numpy_rdd=tile_rdd,
            metadata=layer_metadata_py
        )
        return {zoom: tile_layer}

    def oscars(
            self,
            from_date: str, to_date: str,
            projected_polygons,
            collection_id: str = "urn:eop:VITO:CGS_S1_GRD_L1",
            correlation_id: str = "NA",
            # TODO: what to do with zoom? Highest level? lowest level?
            zoom=0,
            tile_size=256,
    ):
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
        raise RuntimeError("WIP")


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
