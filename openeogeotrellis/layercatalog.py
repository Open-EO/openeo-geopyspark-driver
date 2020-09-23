import logging
from datetime import datetime, date
from typing import List, Optional
from shapely.geometry import box

from geopyspark import TiledRasterLayer, LayerType
from openeo.metadata import CollectionMetadata
from openeo.util import TimingLogger, dict_no_none, Rfc3339
from openeo_driver.backend import CollectionCatalog
from openeo_driver.errors import ProcessGraphComplexityException
from openeo_driver.utils import read_json
from py4j.java_gateway import JavaGateway

from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.catalogs.creo import CatalogClient
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.service_registry import InMemoryServiceRegistry, AbstractServiceRegistry
from openeogeotrellis.utils import kerberos, dict_merge_recursive, normalize_date, to_projected_polygons
from openeogeotrellis._utm import auto_utm_epsg_for_geometry
from openeogeotrellis.oscars import Oscars

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
        postprocessing_band_graph = metadata.get("_vito", "postprocessing_bands", default=None)
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
            srs = 'EPSG:%s' % srs
        if srs == None:
            srs = 'EPSG:4326'

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
        elif viewing_parameters.get('require_bounds', False):
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
            return file_pyramid(jvm.org.openeo.geotrellis.file.Sentinel2PyramidFactory)

        def file_probav_pyramid():
            return jvm.org.openeo.geotrellis.file.ProbaVPyramidFactory(
                layer_source_info.get('oscars_collection_id'), layer_source_info.get('root_path')) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices)

        def file_pyramid(pyramid_factory):
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

                if isinstance(condition, dict) and 'process_graph' in condition:
                    predicate = condition['process_graph']
                    property_value = LiteralMatchExtractingGraphVisitor().accept_process_graph(predicate).property_value
                    return property_value
                else:
                    return condition

            layer_properties = metadata.get("_vito", "properties", default={})
            custom_properties = viewing_parameters.get('properties', {})

            metadata_properties = {property_name: extract_literal_match(condition)
                                   for property_name, condition in {**layer_properties, **custom_properties}.items()}

            polygons = viewing_parameters.get('polygons')

            factory = pyramid_factory(oscars_collection_id, oscars_link_titles, root_path)
            if viewing_parameters.get('pyramid_levels', 'all') != 'all':
                #TODO EP-3561 UTM is not always the native projection of a layer (PROBA-V), need to determine optimal projection
                target_epsg_code = auto_utm_epsg_for_geometry(box(left,bottom,right,top),srs)
                if not polygons:
                    projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent,srs)
                else:
                    projected_polygons = to_projected_polygons(jvm, polygons)
                projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.reproject(projected_polygons,target_epsg_code)
                return factory.datacube_seq(projected_polygons, from_date, to_date, metadata_properties)
            else:
                if polygons:
                    projected_polygons = to_projected_polygons(jvm, polygons)
                    return factory.pyramid_seq(projected_polygons.polygons(), projected_polygons.crs(), from_date,
                                               to_date, metadata_properties)
                else:
                    return factory.pyramid_seq(extent, srs, from_date, to_date, metadata_properties)

        def geotiff_pyramid():
            glob_pattern = layer_source_info['glob_pattern']
            date_regex = layer_source_info['date_regex']

            new_pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex)

            return self._geotiff_pyramid_factories.setdefault(collection_id, new_pyramid_factory) \
                .pyramid_seq(extent, srs, from_date, to_date)

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

        def creo_pyramid():
            mission = layer_source_info['mission']
            level = layer_source_info['level']
            catalog = CatalogClient(mission, level)
            product_paths = catalog.query_product_paths(datetime.strptime(from_date[:10], "%Y-%m-%d"),
                                                        datetime.strptime(to_date[:10], "%Y-%m-%d"),
                                                        ulx=left, uly=top,
                                                        brx=right, bry=bottom)
            return jvm.org.openeo.geotrelliss3.CreoPyramidFactory(product_paths, metadata.band_names) \
                .pyramid_seq(extent, srs, from_date, to_date)

        logger.info("loading pyramid {s}".format(s=layer_source_type))
        if layer_source_type == 's3':
            pyramid = s3_pyramid()
        elif layer_source_type == 's3-jp2':
            pyramid = s3_jp2_pyramid()
        elif layer_source_type == 'file-s2-radiometry':
            pyramid = file_s2_radiometry_pyramid()
        elif layer_source_type == 'file-s2':
            pyramid = file_s2_pyramid()
        elif layer_source_type == 'file-probav':
            pyramid = file_probav_pyramid()
        elif layer_source_type == 'geotiff':
            pyramid = geotiff_pyramid()
        elif layer_source_type == 'file-s1-coherence':
            pyramid = file_s2_pyramid()
        elif layer_source_type == 'sentinel-hub-s1':
            pyramid = sentinel_hub_s1_pyramid()
        elif layer_source_type == 'sentinel-hub-s2-l1c':
            pyramid = sentinel_hub_s2_l1c_pyramid()
        elif layer_source_type == 'sentinel-hub-s2-l2a':
            pyramid = sentinel_hub_s2_l2a_pyramid()
        elif layer_source_type == 'sentinel-hub-l8':
            pyramid = sentinel_hub_l8_pyramid()
        elif layer_source_type == 'creo':
            pyramid = creo_pyramid()
        else:
            pyramid = accumulo_pyramid()

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option

        levels = {
            pyramid.apply(index)._1(): TiledRasterLayer(
                LayerType.SPACETIME,
                temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())
            )
            for index in range(0, pyramid.size())
        }

        if viewing_parameters.get('pyramid_levels', 'all') != 'all':
            max_zoom = max(levels.keys())
            levels = {max_zoom: levels[max_zoom]}

        image_collection = GeotrellisTimeSeriesImageCollection(
            pyramid=gps.Pyramid(levels),
            service_registry=self._service_registry,
            metadata=metadata
        )

        if (postprocessing_band_graph != None):
            from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
            visitor = GeotrellisTileProcessGraphVisitor()
            image_collection = image_collection.reduce_bands(visitor.accept_process_graph(postprocessing_band_graph))

        if still_needs_band_filter:
            # TODO: avoid this `still_needs_band_filter` ugliness.
            #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
            image_collection = image_collection.band_filter(band_indices)

        return image_collection


def get_layer_catalog(oscars: Oscars = None) -> GeoPySparkLayerCatalog:
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

    # TODO: extract this into its own function
    if oscars:
        logger.info("Updating layer catalog metadata from {o!r}".format(o=oscars))

        oscars_collection_ids = \
            {layer_id: collection_id for layer_id, collection_id in
             {l["id"]: l.get("_vito", {}).get("data_source", {}).get("oscars_collection_id") for l in local_metadata}.items()
             if collection_id}

        oscars_collections = oscars.get_collections()

        def derive_from_oscars_collection_metadata(collection_id: str) -> dict:
            collection = next((c for c in oscars_collections if c["id"] == collection_id), None)
            rfc3339 = Rfc3339(propagate_none=True)

            if not collection:
                raise ValueError("unknown OSCARS collection {cid}".format(cid=collection_id))

            def transform_link(oscars_link: dict) -> dict:
                return dict_no_none(
                    rel="alternate",
                    href=oscars_link["href"],
                    title=oscars_link.get("title")
                )

            def search_link(oscars_link: dict) -> dict:
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
                    href=replace_endpoint(oscars_link["href"]),
                    title=oscars_link.get("title")
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
                         [search_link(l) for l in collection["properties"]["links"]["search"]],
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

        oscars_metadata_by_layer_id = {layer_id: derive_from_oscars_collection_metadata(collection_id)
                                       for layer_id, collection_id in oscars_collection_ids.items()}
    else:
        oscars_metadata_by_layer_id = {}

    local_metadata_by_layer_id = {layer["id"]: layer for layer in local_metadata}

    return GeoPySparkLayerCatalog(
        all_metadata=
        list(dict_merge_recursive(oscars_metadata_by_layer_id, local_metadata_by_layer_id, overwrite=True).values()),
        service_registry=None
    )
