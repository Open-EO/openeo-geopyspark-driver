import dateutil.parser as dp
import logging
import requests
import traceback
from copy import deepcopy
from datetime import datetime
from dateutil.tz import tzutc
from typing import List, Dict, Optional

import geopyspark
from openeo_driver.dry_run import ProcessType
from shapely.geometry import box

from openeo.metadata import Band
from openeo.util import TimingLogger, deep_get
from openeo_driver import filter_properties
from openeo_driver.backend import CollectionCatalog, LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import ProcessGraphComplexityException, OpenEOApiException
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.utils import read_json, EvalEnv, to_hashable
from openeogeotrellis import sentinel_hub
from openeogeotrellis.catalogs.creo import CreoCatalogClient
from openeogeotrellis.collections.s1backscatter_orfeo import get_implementation as get_s1_backscatter_orfeo
from openeogeotrellis.collections.testing import load_test_collection
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.opensearch import OpenSearchOscars, OpenSearchCreodias
from openeogeotrellis.utils import dict_merge_recursive, to_projected_polygons, get_jvm, normalize_temporal_extent

logger = logging.getLogger(__name__)


class GeoPySparkLayerCatalog(CollectionCatalog):

    def __init__(self, all_metadata: List[dict]):
        super().__init__(all_metadata=all_metadata)
        self._geotiff_pyramid_factories = {}

    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> GeopysparkDataCube:
        logger.info("Creating layer for {c} with load params {p}".format(c=collection_id, p=load_params))

        metadata = GeopysparkCubeMetadata(self.get_collection_metadata(collection_id))

        if metadata.get("common_name") == collection_id:
            common_name_metadatas = [GeopysparkCubeMetadata(m) for m in self.get_collection_with_common_name(metadata.get("common_name"))]

            backend_provider = load_params.backend_provider
            if backend_provider:
                metadata = next(filter(lambda m: backend_provider.lower() == m.get("_vito", "data_source", "provider:backend"), common_name_metadatas), None)
            else:
                metadata = next(filter(lambda m: m.get("_vito", "data_source", "default_provider:backend"), common_name_metadatas), None)

            if not metadata:
                metadata = common_name_metadatas[0]

        layer_source_info = metadata.get("_vito", "data_source", default={})

        sar_backscatter_compatible = layer_source_info.get("sar_backscatter_compatible", False)

        if load_params.sar_backscatter is not None and not sar_backscatter_compatible:
            raise OpenEOApiException(message="""Process "sar_backscatter" is not applicable for collection {c}."""
                                     .format(c=collection_id), status_code=400)

        layer_source_type = layer_source_info.get("type", "Accumulo").lower()

        native_crs = layer_source_info.get("native_crs","UTM")
        postprocessing_band_graph = metadata.get("_vito", "postprocessing_bands", default=None)
        logger.info("Layer source type: {s!r}".format(s=layer_source_type))
        cell_width = float(metadata.get("cube:dimensions", "x", "step", default=10.0))
        cell_height = float(metadata.get("cube:dimensions", "y", "step", default=10.0))

        temporal_extent = load_params.temporal_extent
        from_date, to_date = normalize_temporal_extent(temporal_extent)
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
            metadata = metadata.rename_labels(metadata.band_dimension.name,bands,metadata.band_names)
        else:
            band_indices = None
        logger.info("band_indices: {b!r}".format(b=band_indices))
        # TODO: avoid this `still_needs_band_filter` ugliness.
        #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        still_needs_band_filter = False

        correlation_id = env.get("correlation_id", '')
        logger.info("Correlation ID is '{cid}'".format(cid=correlation_id))

        logger.info("Detected process types:" + str(load_params.process_types))
        default_temporal_resolution = "ByDay"
        default_indexReduction = 8
        if len(load_params.process_types)==1 and ProcessType.GLOBAL_TIME in load_params.process_types:
            #for pure timeseries processing, adjust partitioning strategy
            default_temporal_resolution = "None"
            default_indexReduction = 0


        experimental = load_params.get("featureflags",{}).get("experimental",False)
        tilesize = load_params.get("featureflags",{}).get("tilesize",256)
        indexReduction = load_params.get("featureflags", {}).get("indexreduction", default_indexReduction)
        temporalResolution = load_params.get("featureflags", {}).get("temporalresolution", default_temporal_resolution)
        globalbounds = load_params.get("featureflags", {}).get("global_bounds", False)

        jvm = get_jvm()

        extent = None
        spatial_bounds_present = all(b is not None for b in [west, south, east, north])

        if not spatial_bounds_present:
            if env.get('require_bounds', False):
                raise OpenEOApiException("Your request does not seem to specify any spatial filtering, which is mandatory for this type of request. A spatial extent can be specified for instance in load_collection.",status_code=400,code="NoSpatialFilter")
            else:
                #in this case, there's some debate on whether we should really try to process the whole world...
                srs = "EPSG:4326"
                west = -180.0
                south = -90
                east = 180
                north = 90
                spatial_bounds_present=True
                #raise OpenEOApiException(code="MissingSpatialFilter", status_code=400,
                #                         message="No spatial filter could be derived to load this collection: {c} . Please specify a bounding box, or polygons to define your area of interest.".format(
                #                             c=collection_id))

        extent = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))
        metadata = metadata.filter_bbox(west=west, south=south, east=east, north=north, crs=srs)

        polygons = load_params.aggregate_spatial_geometries

        if not polygons:
            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, srs)
        else:
            projected_polygons = to_projected_polygons(jvm, polygons)

        single_level = env.get('pyramid_levels', 'all') != 'all'

        if( native_crs == 'UTM' ):
            target_epsg_code = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
        else:
            target_epsg_code = int(native_crs.split(":")[-1])
        projected_polygons_native_crs = (getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$")
                                         .reproject(projected_polygons, target_epsg_code))

        datacubeParams = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        #WTF simple assignment to a var in a scala class doesn't work??
        getattr(datacubeParams, "tileSize_$eq")(tilesize)
        getattr(datacubeParams, "maskingStrategyParameters_$eq")(load_params.custom_mask)
        if(load_params.data_mask is not None and isinstance(load_params.data_mask,GeopysparkDataCube)):
            datacubeParams.setMaskingCube(load_params.data_mask.get_max_level().srdd.rdd())
        datacubeParams.setPartitionerIndexReduction(indexReduction)
        datacubeParams.setPartitionerTemporalResolution(temporalResolution)

        if globalbounds:
            ge = load_params.global_extent
            datacubeParams.setGlobalExtent(ge["west"],ge["south"],ge["east"],ge["north"],ge["crs"])

        if single_level:
            getattr(datacubeParams, "layoutScheme_$eq")("FloatingLayoutScheme")

        def metadata_properties() -> Dict[str, object]:
            layer_properties = metadata.get("_vito", "properties", default={})
            custom_properties = load_params.properties

            return {property_name: filter_properties.extract_literal_match(condition)
                    for property_name, condition in {**layer_properties, **custom_properties}.items()}

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

        def file_oscars_pyramid():
            return file_pyramid(lambda opensearch_endpoint, opensearch_collection_id, opensearch_link_titles, root_path:
                                jvm.org.openeo.geotrellis.file.Sentinel2PyramidFactory(opensearch_endpoint,
                                                                                       opensearch_collection_id,
                                                                                       opensearch_link_titles,
                                                                                       root_path,
                                                                                       jvm.geotrellis.raster.CellSize(
                                                                                           cell_width,
                                                                                           cell_height),
                                                                                       experimental
                                                                                       ))

        def file_s5p_pyramid():
            return file_pyramid(jvm.org.openeo.geotrellis.file.Sentinel5PPyramidFactory)

        def file_probav_pyramid():
            opensearch_endpoint = layer_source_info.get('opensearch_endpoint',
                                                        ConfigParams().default_opensearch_endpoint)

            return jvm.org.openeo.geotrellis.file.ProbaVPyramidFactory(opensearch_endpoint,
                                                                       layer_source_info.get('opensearch_collection_id'), layer_source_info.get('root_path'),jvm.geotrellis.raster.CellSize(cell_width, cell_height)) \
                .pyramid_seq(extent, srs, from_date, to_date, band_indices, correlation_id)

        def file_pyramid(pyramid_factory):
            opensearch_endpoint = layer_source_info.get('opensearch_endpoint',
                                                        ConfigParams().default_opensearch_endpoint)
            opensearch_collection_id = layer_source_info['opensearch_collection_id']
            opensearch_link_titles = metadata.opensearch_link_titles
            root_path = layer_source_info['root_path']

            factory = pyramid_factory(opensearch_endpoint, opensearch_collection_id, opensearch_link_titles, root_path)

            if single_level:
                #TODO EP-3561 UTM is not always the native projection of a layer (PROBA-V), need to determine optimal projection
                return factory.datacube_seq(projected_polygons_native_crs, from_date, to_date, metadata_properties(),
                                            correlation_id,datacubeParams)
            else:
                if polygons:
                    return factory.pyramid_seq(projected_polygons.polygons(), projected_polygons.crs(), from_date,
                                               to_date, metadata_properties(), correlation_id)
                else:
                    return factory.pyramid_seq(extent, srs, from_date, to_date, metadata_properties(), correlation_id)

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
            sar_backscatter_arguments: Optional[SarBackscatterArgs] = (
                (load_params.sar_backscatter or SarBackscatterArgs()) if sar_backscatter_compatible
                else None
            )

            if dependencies:
                source_location, card4l = dependencies[(collection_id, to_hashable(metadata_properties()))]

                # date_regex supports:
                #  - original: _20210223.tif
                #  - CARD4L: s1_rtc_0446B9_S07E035_2021_02_03_MULTIBAND.tif
                #  - tiles assembled from cache: 31UDS_7_2-20190921.tif
                date_regex = r".+(\d{4})_?(\d{2})_?(\d{2}).*\.tif"
                interpret_as_cell_type = "float32ud0"
                lat_lon = card4l

                if source_location.startswith("file:"):
                    assembled_uri = source_location
                    glob_pattern = f"{assembled_uri}/*.tif"

                    logger.info(f"Sentinel Hub pyramid from {glob_pattern}")

                    pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(
                        glob_pattern,
                        date_regex,
                        interpret_as_cell_type,
                        lat_lon
                    )
                else:
                    s3_uri = source_location
                    key_regex = r".+\.tif"
                    recursive = True

                    logger.info(f"Sentinel Hub pyramid from {s3_uri}")

                    pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_s3(
                        s3_uri,
                        key_regex,
                        date_regex,
                        recursive,
                        interpret_as_cell_type,
                        lat_lon
                    )

                if sar_backscatter_arguments and sar_backscatter_arguments.mask:
                    metadata = metadata.append_band(Band(name='mask', common_name=None, wavelength_um=None))

                if sar_backscatter_arguments and sar_backscatter_arguments.local_incidence_angle:
                    metadata = metadata.append_band(Band(name='local_incidence_angle', common_name=None,
                                                         wavelength_um=None))

                return (pyramid_factory.datacube_seq(projected_polygons_native_crs, None, None) if single_level
                        else pyramid_factory.pyramid_seq(extent, srs, None, None))
            else:
                endpoint = layer_source_info['endpoint']
                shub_collection_id = layer_source_info['collection_id']
                dataset_id = layer_source_info['dataset_id']
                client_id = layer_source_info['client_id']
                client_secret = layer_source_info['client_secret']
                sample_type = jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
                    layer_source_info.get('sample_type', 'UINT16'))

                shub_band_names = metadata.band_names

                if sar_backscatter_arguments and sar_backscatter_arguments.mask:
                    metadata = metadata.append_band(Band(name='mask', common_name=None, wavelength_um=None))
                    shub_band_names.append('dataMask')

                if sar_backscatter_arguments and sar_backscatter_arguments.local_incidence_angle:
                    metadata = metadata.append_band(Band(name='local_incidence_angle', common_name=None,
                                                         wavelength_um=None))
                    shub_band_names.append('localIncidenceAngle')

                pyramid_factory = jvm.org.openeo.geotrellissentinelhub.PyramidFactory.rateLimited(
                    endpoint,
                    shub_collection_id,
                    dataset_id,
                    client_id,
                    client_secret,
                    sentinel_hub.processing_options(sar_backscatter_arguments) if sar_backscatter_arguments else {},
                    sample_type,
                    jvm.geotrellis.raster.CellSize(cell_width, cell_height)
                )

                return (
                    pyramid_factory.datacube_seq(projected_polygons_native_crs.polygons(),
                                                 projected_polygons_native_crs.crs(), from_date, to_date,
                                                 shub_band_names, metadata_properties(), datacubeParams) if single_level
                    else pyramid_factory.pyramid_seq(extent, srs, from_date, to_date, shub_band_names,
                                                     metadata_properties()))

        def creo_pyramid():
            mission = layer_source_info['mission']
            level = layer_source_info['level']
            catalog = CreoCatalogClient(mission=mission, level=level)
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
        elif layer_source_type == 'file-oscars':
            pyramid = file_oscars_pyramid()
        elif layer_source_type == 'creodias-s1-backscatter':
            sar_backscatter_arguments = load_params.sar_backscatter or SarBackscatterArgs()
            s1_backscatter_orfeo = get_s1_backscatter_orfeo(
                version=sar_backscatter_arguments.options.get("implementation_version", "1"),
                jvm=jvm
            )
            pyramid = s1_backscatter_orfeo.creodias(
                projected_polygons=projected_polygons_native_crs,
                from_date=from_date, to_date=to_date,
                correlation_id=correlation_id,
                sar_backscatter_arguments=sar_backscatter_arguments,
                bands=bands,
                extra_properties=metadata_properties()
            )
        elif layer_source_type == 'accumulo':
            pyramid = accumulo_pyramid()
        elif layer_source_type == 'testing':
            pyramid = load_test_collection(
                collection_id=collection_id, collection_metadata=metadata,
                extent=extent, srs=srs,
                from_date=from_date, to_date=to_date,
                bands=bands,
                correlation_id=correlation_id
            )
        else:
            raise OpenEOApiException(message="Invalid layer source type {t!r}".format(t=layer_source_type))

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
                if "oscars" in os_endpoint.lower() or "terrascope" in os_endpoint.lower() or "vito.be" in os_endpoint.lower():
                    opensearch = OpenSearchOscars(endpoint=os_endpoint)
                elif "creodias" in os_endpoint.lower():
                    opensearch = OpenSearchCreodias(endpoint=os_endpoint)
                else:
                    raise ValueError(os_endpoint)
                try:
                    opensearch_metadata[cid] = opensearch.get_metadata(collection_id=os_cid)
                except Exception as e:
                    logger.error(traceback.format_exc())
            elif data_source.get("type") == "sentinel-hub":
                sh_cid = data_source.get("collection_id")
                try:
                    sh_collections = requests.get("https://collections.eurodatacube.com/stac/index.json").json()
                    sh_collection = next(filter(lambda c: c["id"] == sh_cid, sh_collections))
                    opensearch_metadata[cid] = requests.get(sh_collection["link"]).json()
                    if not data_source.get("endpoint"):
                        endpoint = opensearch_metadata[cid]["providers"][0]["url"]
                        endpoint = endpoint if endpoint.startswith("http") else "https://{}".format(endpoint)
                        data_source["endpoint"] = endpoint
                    data_source["dataset_id"] = data_source.get("dataset_id") or opensearch_metadata[cid]["datasource_type"]
                except StopIteration:
                    logger.error(f"No STAC data available for collection with id {sh_cid}")

        if opensearch_metadata:
            metadata = dict_merge_recursive(opensearch_metadata, metadata, overwrite=True)

    metadata = _merge_layers_with_common_name(metadata)

    return GeoPySparkLayerCatalog(all_metadata=list(metadata.values()))


def _merge_layers_with_common_name(metadata):
    common_names = set(map(lambda f: f["common_name"], filter(lambda m: m.get("common_name"), metadata.values())))
    for common_name in common_names:
        common_name_metadatas = list(filter(lambda c: c.get("common_name") == common_name, metadata.values()))
        default_metadata = next(filter(lambda m: deep_get(m, "_vito", "data_source", "default_provider:backend", default=False), common_name_metadatas))
        default_metadata = default_metadata or common_name_metadatas[0]
        new_metadata = deepcopy(default_metadata)
        default_metadata["_vito"]["data_source"].pop("default_provider:backend", None)
        new_metadata["_vito"]["data_source"]["provider:backend"] = [new_metadata["_vito"]["data_source"]["provider:backend"]]
        for common_name_metadata in common_name_metadatas:
            if not common_name_metadata["id"] == new_metadata["id"]:
                new_metadata["_vito"]["data_source"]["provider:backend"] += [common_name_metadata["_vito"]["data_source"]["provider:backend"]]
                new_metadata["providers"] += common_name_metadata["providers"]
                new_metadata["links"] += common_name_metadata["links"]
                for b in common_name_metadata["cube:dimensions"]["bands"]["values"]:
                    if b not in new_metadata["cube:dimensions"]["bands"]["values"]:
                        new_metadata["cube:dimensions"]["bands"]["values"] += [b]
                        new_metadata["summaries"]["eo:bands"] += list(filter(lambda m: m["name"] == b, common_name_metadata["summaries"]["eo:bands"]))
                    else:
                        new_metadata_band = next(filter(lambda m: m["name"] == b, new_metadata["summaries"]["eo:bands"]))
                        common_metadata_band = next(filter(lambda m: m["name"] == b, common_name_metadata["summaries"]["eo:bands"]))
                        new_metadata_band["aliases"] = (new_metadata_band.get("aliases") or []) + \
                                                       (common_metadata_band.get("aliases") or [])

                new_metadata_spatial_extent = new_metadata["extent"]["spatial"]["bbox"]
                common_name_metadata_spatial_extent = common_name_metadata["extent"]["spatial"]["bbox"]
                new_metadata["extent"]["spatial"]["bbox"] = [[min(new_metadata_spatial_extent[0][0], common_name_metadata_spatial_extent[0][0]),
                                                              min(new_metadata_spatial_extent[0][1], common_name_metadata_spatial_extent[0][1]),
                                                              max(new_metadata_spatial_extent[0][2], common_name_metadata_spatial_extent[0][2]),
                                                              max(new_metadata_spatial_extent[0][3], common_name_metadata_spatial_extent[0][3])]]
                new_metadata_temporal_extent = new_metadata["extent"]["temporal"]["interval"]
                common_name_metadata_temporal_extent = common_name_metadata["extent"]["temporal"]["interval"]
                default_date = datetime(2017, 1, 1, tzinfo=tzutc())
                new_start = min(dp.parse(new_metadata_temporal_extent[0][0], default=default_date), dp.parse(common_name_metadata_temporal_extent[0][0], default=default_date)).isoformat()
                if not new_metadata_temporal_extent[0][1]:
                    new_end = common_name_metadata_temporal_extent[0][1]
                elif not common_name_metadata_temporal_extent[0][1]:
                    new_end = new_metadata_temporal_extent[0][1]
                else:
                    new_end = max(dp.parse(new_metadata_temporal_extent[0][1], default=default_date), dp.parse(common_name_metadata_temporal_extent[0][1], default=default_date))
                if new_end:
                    new_end = new_end.isoformat()
                new_metadata["extent"]["temporal"]["interval"] = [[new_start, new_end]]

        new_metadata["id"] = common_name

        metadata[common_name] = new_metadata

    return metadata
