import dataclasses
import logging
import re
from copy import deepcopy
from typing import Any, Callable, Dict, Optional

import py4j.protocol
from openeo.metadata import Band
from openeo_driver.backend import LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException
from openeo_driver.utils import EvalEnv

import openeogeotrellis.collections.s1backscatter_orfeo
from openeogeotrellis import sentinel_hub
from openeogeotrellis.catalog.collection_metadata import GeopysparkCubeMetadata
from openeogeotrellis.catalog.load_request import CollectionLoadRequest
from openeogeotrellis.catalogs.creo import CreoCatalogClient
from openeogeotrellis.collections.testing import load_test_collection
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.load_stac import load_stac
from openeogeotrellis.utils import to_projected_polygons

# Note: intentionally NOT importing `datetime` here — see build_creo_pyramid() below.

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class JvmLoadContext:
    """The things pyramid builders need that only exist once a JVM is up, plus the
    two pieces of catalog-instance state (Sentinel Hub credentials, geotiff pyramid
    factory cache) they used to reach through `self`/closures for."""

    jvm: Any
    extent: Any
    geometries: Any
    projected_polygons: Any
    projected_polygons_native_crs: Any
    datacube_params: Any
    single_level: bool
    load_params: LoadParameters
    env: EvalEnv
    pg_node_id: Optional[str]
    metadata_properties: Callable[..., Dict[str, object]]
    get_sar_backscatter_arguments: Callable[[], SarBackscatterArgs]
    sentinel_hub_client_id: Optional[str]
    sentinel_hub_client_secret: Optional[str]
    vault: Any
    geotiff_pyramid_factories: Dict[str, Any]


@dataclasses.dataclass
class PyramidSourceResult:
    pyramid: Any  # dict of levels, or a Scala Seq
    metadata: Optional[GeopysparkCubeMetadata] = None  # sentinel-hub appends bands
    still_needs_band_filter: bool = False  # accumulo


def _create_pyramid(factory, request: CollectionLoadRequest, ctx: JvmLoadContext):
    try:
        if ctx.single_level:
            # TODO EP-3561 UTM is not always the native projection of a layer (PROBA-V), need to determine optimal projection
            return factory.datacube_seq(
                ctx.projected_polygons_native_crs, request.from_date, request.to_date,
                ctx.metadata_properties(), request.correlation_id, ctx.datacube_params
            )
        else:
            if ctx.geometries:
                return factory.pyramid_seq(
                    ctx.projected_polygons.polygons(), ctx.projected_polygons.crs(), request.from_date, request.to_date,
                    ctx.metadata_properties(), request.correlation_id
                )
            else:
                return factory.pyramid_seq(
                    ctx.extent, request.srs, request.from_date, request.to_date,
                    ctx.metadata_properties(), request.correlation_id
                )
    except Exception as e:
        if isinstance(e, py4j.protocol.Py4JJavaError):
            msg = e.java_exception.getMessage()
        else:
            msg = str(e)
        if msg and "Could not find data for your load_collection request with catalog ID" in msg:
            logger.error(f"create_pyramid failed: {msg}", exc_info=True)
            raise OpenEOApiException(
                code="NoDataAvailable", status_code=400,
                message=f"There is no data available for the given extents. {msg}",
            )
        raise


def _file_pyramid(pyramid_factory_builder, request: CollectionLoadRequest, ctx: JvmLoadContext):
    opensearch_collection_id = request.source_info['opensearch_collection_id']
    opensearch_link_titles = request.metadata.opensearch_link_titles
    root_path = request.source_info.get('root_path', None)
    factory = pyramid_factory_builder(
        request.opensearch_endpoint, opensearch_collection_id, opensearch_link_titles, root_path
    )
    return _create_pyramid(factory, request, ctx)


def build_accumulo_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    pyramidFactory = jvm.org.openeo.geotrellisaccumulo.PyramidFactory(
        "hdp-accumulo-instance", ','.join(ConfigParams().zookeepernodes)
    )
    if request.source_info.get("split", False):
        pyramidFactory.setSplitRanges(True)

    accumulo_layer_name = request.source_info['data_id']
    still_needs_band_filter = bool(request.band_indices)

    polygons = ctx.load_params.aggregate_spatial_geometries

    if polygons:
        projected_polygons = to_projected_polygons(jvm, polygons)
        pyramid = pyramidFactory.pyramid_seq(
            accumulo_layer_name, projected_polygons.polygons(), projected_polygons.crs(),
            request.from_date, request.to_date
        )
    else:
        pyramid = pyramidFactory.pyramid_seq(accumulo_layer_name, ctx.extent, request.srs, request.from_date, request.to_date)

    return PyramidSourceResult(pyramid=pyramid, still_needs_band_filter=still_needs_band_filter)


def build_file_s2_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm

    def pyramid_factory(
        opensearch_endpoint: str,
        opensearch_collection_id: str,
        opensearch_link_titles,
        root_path: str,
    ):
        opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
            opensearch_endpoint,
            request.is_utm,
            "",
            request.metadata.band_names,
            request.catalog_type,
            request.metadata.parallel_query(),
            request.metadata.select_one_orbit_per_day(),
        )

        return jvm.org.openeo.geotrellis.file.PyramidFactory(
            opensearch_client,
            opensearch_collection_id,
            opensearch_link_titles,
            root_path,
            jvm.geotrellis.raster.CellSize(request.cell_width, request.cell_height),
            request.experimental,
            request.max_soft_errors_ratio,
        )

    return PyramidSourceResult(pyramid=_file_pyramid(pyramid_factory, request, ctx))


def build_file_probav_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    metadata = request.metadata
    cell_width = float(metadata.get("cube:dimensions", "x", "step", default=10.0))
    cell_height = float(metadata.get("cube:dimensions", "y", "step", default=10.0))
    factory = jvm.org.openeo.geotrellis.file.ProbaVPyramidFactory(
        request.opensearch_endpoint,
        request.source_info.get('opensearch_collection_id'),
        metadata.opensearch_link_titles,
        request.source_info.get('root_path'),
        jvm.geotrellis.raster.CellSize(cell_width, cell_height)
    )
    pyramid = factory.pyramid_seq(ctx.extent, request.srs, request.from_date, request.to_date, request.correlation_id)
    return PyramidSourceResult(pyramid=pyramid)


def build_geotiff_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    glob_pattern = request.source_info['glob_pattern']
    date_regex = request.source_info['date_regex']

    new_pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex)

    pyramid = ctx.geotiff_pyramid_factories.setdefault(request.collection_id, new_pyramid_factory) \
        .pyramid_seq(ctx.extent, request.srs, request.from_date, request.to_date)
    return PyramidSourceResult(pyramid=pyramid)


def build_sentinel_hub_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    metadata = request.metadata
    layer_source_info = request.source_info
    collection_id = request.collection_id
    feature_flags = request.feature_flags
    load_params = ctx.load_params

    dependencies = ctx.env.get(EVAL_ENV_KEY.DEPENDENCIES, [])
    sar_backscatter_arguments: Optional[SarBackscatterArgs] = (
        ctx.get_sar_backscatter_arguments() if request.sar_backscatter_compatible else None
    )

    if dependencies:
        dependency = dependencies.pop(0)
        source_location = dependency['source_location']
        card4l = dependency['card4l']

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

        pyramid = (
            pyramid_factory.datacube_seq(
                ctx.projected_polygons_native_crs, request.from_date, request.to_date,
                ctx.metadata_properties(), collection_id, ctx.datacube_params
            ) if ctx.single_level
            else pyramid_factory.pyramid_seq(ctx.extent, request.srs, request.from_date, request.to_date)
        )
        return PyramidSourceResult(pyramid=pyramid, metadata=metadata)
    else:
        shub_band_names = metadata.band_names

        if collection_id == 'SENTINEL_5P_L2':
            if shub_band_names == ["dataMask"]:
                raise OpenEOApiException(
                    f"Can not load collection '{collection_id}' with only 'dataMask' band. Add 1 other band to make it work.",
                    status_code=400)
            pruned_bands = shub_band_names.copy()
            if "dataMask" in pruned_bands:
                pruned_bands.remove("dataMask")
            if len(pruned_bands) != 1:
                raise OpenEOApiException(
                    f"Collection '{collection_id}' got requested with multiple bands: {pruned_bands}. Only one band is supported, with or without the 'dataMask' band.",
                    status_code=400)

        if collection_id == 'PLANETSCOPE':
            if 'byoc_collection_id' in feature_flags:
                shub_collection_id = dataset_id = feature_flags['byoc_collection_id']
            else:
                (condition, byoc_id) = ctx.metadata_properties(flatten_eqs=False).get('byoc_id', (None, None))
                if condition == "eq":
                    # note: "byoc-" prefix is optional for the collection ID but dataset ID requires it
                    shub_collection_id = dataset_id = byoc_id
                    del load_params.properties['byoc_id']
                else:
                    raise OpenEOApiException(code="MissingByocId", status_code=400,
                                             message="Collection id is PLANETSCOPE but properties parameter does "
                                                     "not specify a byoc id.")
        else:
            shub_collection_id = layer_source_info.get('collection_id')
            dataset_id = layer_source_info['dataset_id']

        endpoint = layer_source_info['endpoint']
        sample_type = jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
            layer_source_info.get('sample_type', 'UINT16'))

        if sar_backscatter_arguments and sar_backscatter_arguments.mask:
            metadata = metadata.append_band(Band(name='mask', common_name=None, wavelength_um=None))
            shub_band_names.append('dataMask')

        if sar_backscatter_arguments and sar_backscatter_arguments.local_incidence_angle:
            metadata = metadata.append_band(Band(name='local_incidence_angle', common_name=None,
                                                 wavelength_um=None))
            shub_band_names.append('localIncidenceAngle')

        cell_size = jvm.geotrellis.raster.CellSize(request.cell_width, request.cell_height)
        no_data_value = metadata.get_nodata_value(load_params.bands, 0.0)

        if ConfigParams().is_kube_deploy:
            access_token = ctx.env[EVAL_ENV_KEY.USER].internal_auth_data["access_token"]

            pyramid_factory = jvm.org.openeo.geotrellissentinelhub.PyramidFactory.withFixedAccessToken(
                endpoint,
                shub_collection_id,
                dataset_id,
                access_token,
                sentinel_hub.processing_options(collection_id,
                                                sar_backscatter_arguments) if sar_backscatter_arguments else {},
                sample_type,
                cell_size,
                request.max_soft_errors_ratio,
                no_data_value,
            )
        else:
            sentinel_hub_client_alias = ctx.env.get(EVAL_ENV_KEY.SENTINEL_HUB_CLIENT_ALIAS, "default")
            logger.debug(f"Sentinel Hub client alias: {sentinel_hub_client_alias}")

            if sentinel_hub_client_alias == 'default':
                sentinel_hub_client_id = ctx.sentinel_hub_client_id
                sentinel_hub_client_secret = ctx.sentinel_hub_client_secret
            else:
                vault_token = ctx.env[EVAL_ENV_KEY.VAULT_TOKEN]
                sentinel_hub_client_id, sentinel_hub_client_secret = (
                    ctx.vault.get_sentinel_hub_credentials(sentinel_hub_client_alias, vault_token))

            if not sentinel_hub_client_id or not sentinel_hub_client_secret:
                raise ValueError(
                    f"Sentinel Hub credentials for alias '{sentinel_hub_client_alias}' are not configured."
                )

            zookeeper_connection_string = ','.join(ConfigParams().zookeepernodes)
            zookeeper_access_token_path = f"/openeo/rlguard/access_token_{sentinel_hub_client_alias}"

            pyramid_factory = jvm.org.openeo.geotrellissentinelhub.PyramidFactory.withoutGuardedRateLimiting(
                endpoint,
                shub_collection_id,
                dataset_id,
                sentinel_hub_client_id,
                sentinel_hub_client_secret,
                zookeeper_connection_string,
                zookeeper_access_token_path,
                sentinel_hub.processing_options(collection_id,
                                                sar_backscatter_arguments) if sar_backscatter_arguments else {},
                sample_type,
                cell_size,
                request.max_soft_errors_ratio,
                no_data_value,
            )

        unflattened_metadata_properties = ctx.metadata_properties(flatten_eqs=False)
        sentinel_hub.assure_polarization_from_sentinel_bands(metadata, unflattened_metadata_properties)

        pyramid = (
            pyramid_factory.datacube_seq(ctx.projected_polygons_native_crs.polygons(),
                                         ctx.projected_polygons_native_crs.crs(), request.from_date, request.to_date,
                                         shub_band_names, unflattened_metadata_properties,
                                         ctx.datacube_params, request.correlation_id) if ctx.single_level
            else pyramid_factory.pyramid_seq(ctx.extent, request.srs, request.from_date, request.to_date, shub_band_names,
                                             unflattened_metadata_properties, request.correlation_id))
        return PyramidSourceResult(pyramid=pyramid, metadata=metadata)


def build_creo_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    mission = request.source_info['mission']
    level = request.source_info['level']
    catalog = CreoCatalogClient(mission=mission, level=level)
    product_paths = catalog.query_product_paths(datetime.strptime(request.from_date[:10], "%Y-%m-%d"),
                                                datetime.strptime(request.to_date[:10], "%Y-%m-%d"),
                                                ulx=request.west, uly=request.north,
                                                brx=request.east, bry=request.south)
    # TODO: geotrelliss3.CreoPyramidFactory no longer exists.
    pyramid = ctx.jvm.org.openeo.geotrelliss3.CreoPyramidFactory(product_paths, request.metadata.band_names) \
        .datacube_seq(ctx.projected_polygons_native_crs, request.from_date, request.to_date, {}, request.collection_id)
    return PyramidSourceResult(pyramid=pyramid)


def build_globspatialonly_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    metadata = request.metadata
    if len(metadata.band_names) != 1:
        raise ValueError("expected a single band name for collection {cid}, got {bs} instead".format(
            cid=request.collection_id, bs=metadata.band_names))

    data_glob = request.source_info['data_glob']
    band_names = metadata.band_names
    client_type = request.catalog_type if request.catalog_type != "" else "globspatialonly"
    opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
        data_glob, False, None, band_names, client_type
    )
    factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
        opensearch_client,
        "",
        band_names,
        "",
        jvm.geotrellis.raster.CellSize(request.cell_width, request.cell_height),
        False,
        request.max_soft_errors_ratio,
    )
    return PyramidSourceResult(pyramid=_create_pyramid(factory, request, ctx))


def build_file_cgls_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    data_glob = request.source_info['data_glob']
    date_regex = request.source_info['date_regex']
    band_names = request.metadata.band_names

    client_type = request.catalog_type if request.catalog_type != "" else "cgls"
    opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
        data_glob, False, date_regex, band_names, client_type
    )
    factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
        opensearch_client,
        "",
        band_names,
        "",
        jvm.geotrellis.raster.CellSize(request.cell_width, request.cell_height),
        False,
        request.max_soft_errors_ratio,
    )
    return PyramidSourceResult(pyramid=_create_pyramid(factory, request, ctx))


def build_file_agera5_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    data_glob = request.source_info['data_glob']
    date_regex = request.source_info['date_regex']
    band_marker = request.source_info.get('band_marker', 'dewpoint-temperature')
    band_names = request.metadata.band_names

    opensearch_client = jvm.org.openeo.opensearch.backends.Agera5SearchClient.apply(
        data_glob, False, date_regex, band_names, band_marker
    )
    factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
        opensearch_client,
        "",
        band_names,
        "",
        jvm.geotrellis.raster.CellSize(request.cell_width, request.cell_height),
        False,
        request.max_soft_errors_ratio,
    )
    return PyramidSourceResult(pyramid=_create_pyramid(factory, request, ctx))


def build_creodias_s1_backscatter_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    sar_backscatter_arguments = ctx.get_sar_backscatter_arguments()
    # make a copy before modifying: it is used as a cache key
    sar_backscatter_arguments = deepcopy(sar_backscatter_arguments)
    sar_backscatter_arguments.options["resolution"] = (request.cell_width, request.cell_height)
    s1_backscatter_orfeo = openeogeotrellis.collections.s1backscatter_orfeo.get_implementation(
        version=sar_backscatter_arguments.options.get("implementation_version", "2"),
        jvm=ctx.jvm
    )
    pyramid = s1_backscatter_orfeo.creodias(
        projected_polygons=ctx.projected_polygons_native_crs,
        from_date=request.from_date, to_date=request.to_date,
        collection_id=request.collection_id,
        correlation_id=request.correlation_id,
        sar_backscatter_arguments=sar_backscatter_arguments,
        bands=request.bands,
        extra_properties=ctx.metadata_properties(),
        datacubeParams=ctx.datacube_params,
        max_soft_errors_ratio=request.max_soft_errors_ratio,
        spatial_extent=ctx.load_params.spatial_extent,
        use_stac_client=request.source_info.get("use_stac_client", False),
        feature_flags=request.source_info.get("load_stac_feature_flags", {}),
    )
    return PyramidSourceResult(pyramid=pyramid)


def build_file_s3_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    metadata = request.metadata
    native_cell_size = jvm.geotrellis.raster.CellSize(
        float(metadata.get("cube:dimensions", "x", "step")),
        float(metadata.get("cube:dimensions", "y", "step"))
    )
    # Local import to save some RAM and avoid potential confusing error:
    from openeogeotrellis.collections import sentinel3

    pyramid = sentinel3.pyramid(
        ctx.metadata_properties(),
        ctx.projected_polygons_native_crs,
        request.from_date,
        request.to_date,
        metadata.opensearch_link_titles,
        ctx.datacube_params,
        native_cell_size,
        {**request.feature_flags, "load_stac_feature_flags": request.source_info.get("load_stac_feature_flags", {})},
        jvm,
        spatial_extent=ctx.load_params.spatial_extent,
        use_stac_client=request.source_info.get("use_stac_client", False),
    )
    return PyramidSourceResult(pyramid=pyramid)


def build_file_s5p_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    jvm = ctx.jvm
    metadata = request.metadata
    native_cell_size = jvm.geotrellis.raster.CellSize(
        float(metadata.get("cube:dimensions", "x", "step")), float(metadata.get("cube:dimensions", "y", "step"))
    )
    # Local import to save some RAM and avoid potential confusing error:
    from openeogeotrellis.collections.load_sentinel5p import pyramid as s5p_pyramid

    load_stac_feature_flags = request.source_info.get("load_stac_feature_flags", {})
    if not "url" in load_stac_feature_flags and request.source_info.get("opensearch_endpoint"):
        logger.warning(
            "Using legacy opensearch_endpoint for S5P collection. Please use load_stac_feature_flags.url instead."
        )
        load_stac_feature_flags["url"] = request.source_info["opensearch_endpoint"]
    pyramid = s5p_pyramid(
        ctx.metadata_properties(),
        ctx.projected_polygons_native_crs,
        request.from_date,
        request.to_date,
        request.normalized_band_selection,
        ctx.datacube_params,
        native_cell_size,
        {**request.feature_flags, "load_stac_feature_flags": load_stac_feature_flags},
        jvm,
        spatial_extent=ctx.load_params.spatial_extent,
        collection_id=request.collection_id,
    )
    return PyramidSourceResult(pyramid=pyramid)


def build_stac_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    cube = load_stac(
        url=request.source_info["url"],
        load_params=ctx.load_params,
        env=ctx.env,
        layer_properties=request.metadata.get("_vito", "properties", default={}),
        batch_jobs=None,
        normalized_band_selection=request.normalized_band_selection,
        feature_flags=request.source_info.get("load_stac_feature_flags", {}),
        data_cube_parameters=ctx.datacube_params,
        pg_node_id=ctx.pg_node_id,
    )
    return PyramidSourceResult(pyramid=cube.pyramid.levels, metadata=cube.metadata)


def build_testing_pyramid(request: CollectionLoadRequest, ctx: JvmLoadContext) -> PyramidSourceResult:
    tile_cols, tile_rows = map(int, re.match(r".*?(\d+)x(\d+)", request.collection_id).groups())
    assert tile_cols == tile_rows

    pyramid = load_test_collection(
        tile_size=tile_cols,
        collection_metadata=request.metadata,
        extent=ctx.extent,
        srs=request.srs,
        from_date=request.from_date,
        to_date=request.to_date,
        bands=request.bands,
        correlation_id=request.correlation_id
    )
    return PyramidSourceResult(pyramid=pyramid)


SOURCE_BUILDERS: Dict[str, Callable[[CollectionLoadRequest, JvmLoadContext], PyramidSourceResult]] = {
    "file-s2": build_file_s2_pyramid,
    "file-s1-coherence": build_file_s2_pyramid,
    "file-oscars": build_file_s2_pyramid,
    "cgls_oscars": build_file_s2_pyramid,
    "file-probav": build_file_probav_pyramid,
    "geotiff": build_geotiff_pyramid,
    "sentinel-hub": build_sentinel_hub_pyramid,
    "creo": build_creo_pyramid,
    "file-cgls2": build_file_cgls_pyramid,
    "file-agera5": build_file_agera5_pyramid,
    "file-glob": build_file_agera5_pyramid,
    "file-globspatialonly": build_globspatialonly_pyramid,
    "creodias-s1-backscatter": build_creodias_s1_backscatter_pyramid,
    "file-s3": build_file_s3_pyramid,
    "file-s5p": build_file_s5p_pyramid,
    "stac": build_stac_pyramid,
    "accumulo": build_accumulo_pyramid,
    "testing": build_testing_pyramid,
}
