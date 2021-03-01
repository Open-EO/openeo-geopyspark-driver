import logging
from datetime import datetime
from typing import List, Dict

import geopyspark
from openeogeotrellis import sentinel_hub
from shapely.geometry import box

from openeo.metadata import Band
from openeo.util import TimingLogger, deep_get
from openeo_driver.backend import CollectionCatalog, LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import ProcessGraphComplexityException, OpenEOApiException
from openeo_driver.utils import read_json, EvalEnv
from openeogeotrellis._utm import auto_utm_epsg_for_geometry
from openeogeotrellis.catalogs.creo import CatalogClient
from openeogeotrellis.collections.s1backscatter_orfeo import S1BackscatterOrfeo
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.opensearch import OpenSearchOscars, OpenSearchCreodias
from openeogeotrellis.utils import kerberos, dict_merge_recursive, normalize_date, to_projected_polygons, get_jvm

logger = logging.getLogger(__name__)


class GeoPySparkLayerCatalog(CollectionCatalog):

    def __init__(self, all_metadata: List[dict]):
        super().__init__(all_metadata=all_metadata)
        self._geotiff_pyramid_factories = {}

    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> GeopysparkDataCube:
        logger.info("Creating layer for {c} with load params {p}".format(c=collection_id, p=load_params))

        metadata = GeopysparkCubeMetadata(self.get_collection_metadata(collection_id))
        layer_source_info = metadata.get("_vito", "data_source", default={})

        if load_params.sar_backscatter is not None and not layer_source_info.get("sar_backscatter_compatible", False):
            raise OpenEOApiException(message="""Process "sar_backscatter" is not applicable for collection {c}."""
                                     .format(c=collection_id), status_code=400)

        # TODO is it necessary to do this kerberos stuff here?
        kerberos()

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
        tilesize = load_params.get("featureflags",{}).get("tilesize",256)

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

        datacubeParams = jvm.org.openeo.geotrellis.file.DataCubeParameters()
        datacubeParams.tileSize = tilesize
        datacubeParams.maskingStrategyParameters = load_params.custom_mask

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
                return factory.datacube_seq(projected_polygons_native_crs, from_date, to_date, metadata_properties, correlation_id,datacubeParams)
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
            # TODO: move the metadata manipulation out of this function and get rid of the nonlocal?
            nonlocal metadata

            dependencies = env.get('dependencies', {})

            if dependencies:
                subfolder = dependencies[collection_id]

                s3_uri = "s3://{b}/{f}/".format(b=ConfigParams().sentinel_hub_batch_bucket, f=subfolder)
                key_regex = r".+\.tif"
                # support original _20210223.tif as well as CARD4L s1_rtc_0446B9_S07E035_2021_02_03_MULTIBAND.tif
                date_regex = r".+_(\d{4})_?(\d{2})_?(\d{2}).*\.tif"
                recursive = True
                interpret_as_cell_type = "float32ud0"

                logger.info("Sentinel Hub pyramid from {u}".format(u=s3_uri))

                pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_s3(
                    s3_uri,
                    key_regex,
                    date_regex,
                    recursive,
                    interpret_as_cell_type
                )

                sar_backscatter_arguments = load_params.sar_backscatter or SarBackscatterArgs()

                if sar_backscatter_arguments.mask:
                    metadata = metadata.append_band(Band(name='mask', common_name=None, wavelength_um=None))

                if sar_backscatter_arguments.local_incidence_angle:
                    metadata = metadata.append_band(Band(name='local_incidence_angle', common_name=None,
                                                         wavelength_um=None))

                return (pyramid_factory.datacube_seq(projected_polygons_native_crs, None, None) if single_level
                        else pyramid_factory.pyramid_seq(extent, srs, None, None))
            else:
                dataset_id = layer_source_info['dataset_id']
                client_id = layer_source_info['client_id']
                client_secret = layer_source_info['client_secret']
                sample_type = jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
                    layer_source_info.get('sample_type', 'UINT16'))

                sar_backscatter_arguments = load_params.sar_backscatter or SarBackscatterArgs()

                shub_band_names = metadata.band_names

                if sar_backscatter_arguments.mask:
                    metadata = metadata.append_band(Band(name='mask', common_name=None, wavelength_um=None))
                    shub_band_names.append('dataMask')

                if sar_backscatter_arguments.local_incidence_angle:
                    metadata = metadata.append_band(Band(name='local_incidence_angle', common_name=None,
                                                         wavelength_um=None))
                    shub_band_names.append('localIncidenceAngle')

                # FIXME: support contributing_area (under investigation by Anze)

                pyramid_factory = jvm.org.openeo.geotrellissentinelhub.PyramidFactory(
                    dataset_id,
                    client_id,
                    client_secret,
                    sentinel_hub.processing_options(sar_backscatter_arguments),
                    sample_type
                )

                return (
                    pyramid_factory.datacube_seq(projected_polygons_native_crs.polygons(),
                                                 projected_polygons_native_crs.crs(), from_date, to_date,
                                                 shub_band_names) if single_level
                    else pyramid_factory.pyramid_seq(extent, srs, from_date, to_date, shub_band_names))

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
                factory.datacube_seq(projected_polygons, from_date, to_date,{},"",datacubeParams) if single_level
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
            pyramid = S1BackscatterOrfeo(jvm=jvm).creodias(
                projected_polygons=projected_polygons_native_crs,
                from_date=from_date, to_date=to_date,
                correlation_id=correlation_id,
                sar_backscatter_arguments=load_params.sar_backscatter or SarBackscatterArgs(),
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


def get_layer_catalog(opensearch_enrich=False) -> GeoPySparkLayerCatalog:
    """
    Get layer catalog (from JSON files)
    """
    metadata: Dict[str, dict] = {}

    def read_catalog_file(catalog_file) -> Dict[str, dict]:
        return {coll["id"]: coll for coll in read_json(catalog_file)}

    catalog_files = ConfigParams().layer_catalog_metadata_files
    for path in catalog_files:
        logger.info(f"Reading layer catalog metadata from {path}")
        metadata = dict_merge_recursive(metadata, read_catalog_file(path), overwrite=True)

    if opensearch_enrich:
        opensearch_metadata = {}
        for cid, collection_metadata in metadata.items():
            data_source = deep_get(collection_metadata, "_vito", "data_source", default={})
            os_cid = data_source.get("opensearch_collection_id")
            if os_cid:
                os_endpoint = data_source.get("opensearch_endpoint") or ConfigParams().default_opensearch_endpoint
                logger.info(f"Updating {cid} metadata from {os_endpoint}:{os_cid}")
                # TODO: move this to a OpenSearch factory?
                if "oscars" in os_endpoint.lower() or "terrascope" in os_endpoint.lower():
                    opensearch = OpenSearchOscars(endpoint=os_endpoint)
                elif "creodias" in os_endpoint.lower():
                    opensearch = OpenSearchCreodias(endpoint=os_endpoint)
                else:
                    raise ValueError(os_endpoint)
                opensearch_metadata[cid] = opensearch.get_metadata(collection_id=os_cid)
        if opensearch_metadata:
            metadata = dict_merge_recursive(opensearch_metadata, metadata, overwrite=True)

    return GeoPySparkLayerCatalog(all_metadata=list(metadata.values()))


