import logging
from typing import List

from geopyspark import TiledRasterLayer, LayerType
from openeo.metadata import CollectionMetadata
from openeo.util import TimingLogger
from openeo_driver.backend import CollectionCatalog
from openeo_driver.errors import ProcessGraphComplexityException
from openeo_driver.utils import read_json
from py4j.java_gateway import JavaGateway

from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.service_registry import InMemoryServiceRegistry, AbstractServiceRegistry
from openeogeotrellis.utils import kerberos, dict_merge_recursive, normalize_date, to_projected_polygons

logger = logging.getLogger(__name__)


class GeoPySparkLayerCatalog(CollectionCatalog):

    # TODO: eliminate the dependency/coupling with service registry

    def __init__(self, all_metadata: List[dict], service_registry: AbstractServiceRegistry):
        super().__init__(all_metadata=all_metadata)
        self._service_registry = service_registry
        self._geotiff_pyramid_factories = {}

    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(self, collection_id: str, viewing_parameters: dict) -> 'GeotrellisTimeSeriesImageCollection':
        logger.info("Creating layer for {c} with viewingParameters {v}".format(c=collection_id, v=viewing_parameters))

        # TODO is it necessary to do this kerberos stuff here?
        kerberos()

        metadata = CollectionMetadata(self.get_collection_metadata(collection_id))
        layer_source_info = metadata.get("_vito", "data_source", default={})
        layer_source_type = layer_source_info.get("type", "Accumulo").lower()
        logger.info("Layer source type: {s!r}".format(s=layer_source_type))

        import geopyspark as gps
        from_date = normalize_date(viewing_parameters.get("from", None))
        to_date = normalize_date(viewing_parameters.get("to", None))

        left = viewing_parameters.get("left", None)
        right = viewing_parameters.get("right", None)
        top = viewing_parameters.get("top", None)
        bottom = viewing_parameters.get("bottom", None)
        srs = viewing_parameters.get("srs", None)

        if isinstance(srs, int):
            srs = 'EPSG:%s'%srs
        if srs == None:
            srs='EPSG:4326'

        bands = viewing_parameters.get("bands", None)
        if bands:
            band_indices = [metadata.get_band_index(b) for b in bands]
            metadata = metadata.filter_bands(bands)
        else:
            band_indices = None
        logger.info("band_indices: {b!r}".format(b=band_indices))
        # TODO: avoid this `still_needs_band_filter` ugliness.
        #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        still_needs_band_filter = False
        pysc = gps.get_spark_context()
        extent = None

        gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
        jvm = gateway.jvm

        spatial_bounds_present = left is not None and right is not None and top is not None and bottom is not None

        if spatial_bounds_present:
            extent = jvm.geotrellis.vector.Extent(float(left), float(bottom), float(right), float(top))
        elif ConfigParams().require_bounds:
            raise ProcessGraphComplexityException
        else:
            srs = "EPSG:4326"
            extent = jvm.geotrellis.vector.Extent(-180.0, -90.0, 180.0, 90.0)

        def accumulo_pyramid():
            pyramidFactory = jvm.org.openeo.geotrellisaccumulo.PyramidFactory("hdp-accumulo-instance",
                                                                              ','.join(ConfigParams().zookeepernodes))
            if layer_source_info.get("split", False):
                pyramidFactory.setSplitRanges(True)

            accumulo_layer_name = layer_source_info['data_id']
            nonlocal still_needs_band_filter
            still_needs_band_filter = bool(band_indices)

            polygons = viewing_parameters.get('polygons')

            if polygons:
                projected_polygons = to_projected_polygons(jvm, polygons)
                return pyramidFactory.pyramid_seq(accumulo_layer_name, projected_polygons.polygons(), projected_polygons.crs(), from_date, to_date)
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
            oscars_collection_id = layer_source_info['oscars_collection_id']
            oscars_link_titles = metadata.band_names
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

                predicate = condition['process_graph']
                property_value = LiteralMatchExtractingGraphVisitor().accept_process_graph(predicate).property_value

                return property_value

            properties = viewing_parameters.get('properties', {})

            metadata_properties = {property_name: extract_literal_match(condition)
                               for property_name, condition in properties.items()}

            return jvm.org.openeo.geotrellis.file.Sentinel2PyramidFactory(
                oscars_collection_id,
                oscars_link_titles,
                root_path
            ).pyramid_seq(extent, srs, from_date, to_date, metadata_properties)

        def geotiff_pyramid():
            glob_pattern = layer_source_info['glob_pattern']
            date_regex = layer_source_info['date_regex']

            new_pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex)

            return self._geotiff_pyramid_factories.setdefault(collection_id, new_pyramid_factory) \
                .pyramid_seq(extent, srs, from_date, to_date)

        def file_s1_coherence_pyramid():
            return jvm.org.openeo.geotrellis.file.Sentinel1CoherencePyramidFactory() \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def sentinel_hub_s1_pyramid():
            return jvm.org.openeo.geotrellissentinelhub.S1PyramidFactory(layer_source_info.get('uuid')) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def sentinel_hub_s2_l1c_pyramid():
            return jvm.org.openeo.geotrellissentinelhub.S2L1CPyramidFactory(layer_source_info.get('uuid')) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def sentinel_hub_s2_l2a_pyramid():
            return jvm.org.openeo.geotrellissentinelhub.S2L2APyramidFactory(layer_source_info.get('uuid')) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def sentinel_hub_l8_pyramid():
            return jvm.org.openeo.geotrellissentinelhub.L8PyramidFactory(layer_source_info.get('uuid')) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        logger.info("loading pyramid {s}".format(s=layer_source_type))
        if layer_source_type == 's3':
            pyramid = s3_pyramid()
        elif layer_source_type == 's3-jp2':
            pyramid = s3_jp2_pyramid()
        elif layer_source_type == 'file-s2-radiometry':
            pyramid = file_s2_radiometry_pyramid()
        elif layer_source_type == 'file-s2':
            pyramid = file_s2_pyramid()
        elif layer_source_type == 'geotiff':
            pyramid = geotiff_pyramid()
        elif layer_source_type == 'file-s1-coherence':
            pyramid = file_s1_coherence_pyramid()
        elif layer_source_type == 'sentinel-hub-s1':
            pyramid = sentinel_hub_s1_pyramid()
        elif layer_source_type == 'sentinel-hub-s2-l1c':
            pyramid = sentinel_hub_s2_l1c_pyramid()
        elif layer_source_type == 'sentinel-hub-s2-l2a':
            pyramid = sentinel_hub_s2_l2a_pyramid()
        elif layer_source_type == 'sentinel-hub-l8':
            pyramid = sentinel_hub_l8_pyramid()
        else:
            pyramid = accumulo_pyramid()

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option
        startIndex = 0 if viewing_parameters.get('pyramid_levels', 'all') else pyramid.size() - 1
        levels = {
            pyramid.apply(index)._1(): TiledRasterLayer(
                LayerType.SPACETIME,
                temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())
            )
            for index in range(startIndex, pyramid.size())
        }

        image_collection = GeotrellisTimeSeriesImageCollection(
            pyramid=gps.Pyramid(levels),
            service_registry=self._service_registry,
            metadata=metadata
        )

        if still_needs_band_filter:
            # TODO: avoid this `still_needs_band_filter` ugliness.
            #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
            image_collection = image_collection.band_filter(band_indices)

        return image_collection


def get_layer_catalog(service_registry: AbstractServiceRegistry = None) -> GeoPySparkLayerCatalog:
    """
    Get layer catalog (from JSON files)
    """
    catalog_files = ConfigParams().layer_catalog_metadata_files
    logger.info("Reading layer catalog metadata from {f!r}".format(f=catalog_files[0]))
    metadata = read_json(catalog_files[0])
    if len(catalog_files) > 1:
        # Merge metadata recursively
        metadata = {l["id"]: l for l in metadata}
        for path in catalog_files[1:]:
            logger.info("Updating layer catalog metadata from {f!r}".format(f=path))
            updates = {l["id"]: l for l in read_json(path)}
            metadata = dict_merge_recursive(metadata, updates, overwrite=True)
        metadata = list(metadata.values())

    return GeoPySparkLayerCatalog(
        all_metadata=metadata,
        service_registry=service_registry or InMemoryServiceRegistry()
    )
