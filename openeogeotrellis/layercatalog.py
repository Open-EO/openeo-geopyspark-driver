import logging
import math
from copy import deepcopy, copy
from functools import lru_cache
from typing import List, Dict, Optional, Tuple, Union

import dateutil.parser
import geopyspark
import py4j.protocol
import pyproj
import pytz
import flask

from openeo.metadata import Band
from openeo.util import TimingLogger, deep_get, str_truncate
from openeo_driver import filter_properties
from openeo_driver.backend import CollectionCatalog, LoadParameters, QueryablesListing
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, ProcessGraphComplexityException
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv, WhiteListEvalEnv, smart_bool
from shapely.geometry import box

from openeogeotrellis import sentinel_hub, datacube_parameters
from openeogeotrellis.catalog.files import dump_layer_catalog, load_catalog_files
from openeogeotrellis.catalog.validation import check_missing_products, extra_validation_load_collection
from openeogeotrellis._backend import post_dry_run
from openeogeotrellis.catalogs.creo import CreoCatalogClient
import openeogeotrellis.collections.s1backscatter_orfeo
from openeogeotrellis.collections.testing import load_test_collection
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.constants import EVAL_ENV_KEY, WHITELIST
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.load_stac import load_stac
from openeogeotrellis.processgraphvisiting import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.util.datetime import normalize_temporal_extent, parse_approximate_isoduration
from openeogeotrellis.utils import (
    to_projected_polygons,
    get_jvm,
)
from openeogeotrellis.vault import Vault

logger = logging.getLogger(__name__)


class GeoPySparkLayerCatalog(CollectionCatalog):
    def __init__(self, all_metadata: List[dict], vault: Vault = None):
        super().__init__(all_metadata=all_metadata)
        self._geotiff_pyramid_factories = {}
        self._default_sentinel_hub_client_id = None
        self._default_sentinel_hub_client_secret = None
        self._vault = vault

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self._default_sentinel_hub_client_id = client_id
        self._default_sentinel_hub_client_secret = client_secret

    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(
        self, collection_id: str, load_params: LoadParameters, env: EvalEnv, pg_node_id: Optional[str] = None
    ) -> GeopysparkDataCube:

        if smart_bool(env.get(EVAL_ENV_KEY.DO_EXTENT_CHECK, True)):
            env_validate = env.push({
                "allow_check_missing_products": False,
            })
            try:
                issues = extra_validation_load_collection(
                    collection_id, load_params, env_validate, global_extent_provider=post_dry_run.get_global_extent
                )
            except Exception as e:
                issues = [{"code": "Internal", "message": str(e)}]
                logger.warning(f"Error during extra_validation_load_collection: {e!r}")
            # Only care for certain errors and make list of strings:
            issues = [e["message"] for e in issues if e["code"] == "ExtentTooLarge"]
            if issues:
                if env.get("sync_job", False):
                    raise ProcessGraphComplexityException(
                        ProcessGraphComplexityException.message + f" Reasons: {' '.join(issues)}"
                    )
                else:
                    raise ProcessGraphComplexityException(
                        "The process graph is computationally too heavy and will likely time out. Disable this check with 'job_options.do_extent_check': "
                        + " ".join(issues)
                    )

        return self._load_collection_cached(
            collection_id, load_params, WhiteListEvalEnv(env, WHITELIST), pg_node_id=pg_node_id
        )

    @lru_cache(maxsize=40)
    def _load_collection_cached(
        self, collection_id: str, load_params: LoadParameters, env: EvalEnv, pg_node_id: Optional[str] = None
    ) -> GeopysparkDataCube:
        logger.info(
            f"load_collection: Creating raster datacube for {collection_id=} ({pg_node_id=}) with {load_params=}, {env=}"
        )

        from_date, to_date = temporal_extent = normalize_temporal_extent(load_params.temporal_extent)
        spatial_extent = load_params.spatial_extent

        west = spatial_extent.get("west", None)
        east = spatial_extent.get("east", None)
        north = spatial_extent.get("north", None)
        south = spatial_extent.get("south", None)
        srs = spatial_extent.get("crs", 'EPSG:4326')
        if isinstance(srs, int):
            srs = 'EPSG:%s' % str(srs)

        spatial_bounds_present = all(b is not None for b in [west, south, east, north])
        if not spatial_bounds_present:
            if env.get(EVAL_ENV_KEY.REQUIRE_BOUNDS, False):
                raise OpenEOApiException(code="MissingSpatialFilter", status_code=400,
                                         message="No spatial filter could be derived to load this collection: {c} . Please specify a bounding box, or polygons to define your area of interest.".format(
                                             c=collection_id))
            else:
                #whole world processing, for instance in viewing services
                srs = "EPSG:4326"
                west = -180.0
                south = -90
                east = 180
                north = 90
                spatial_bounds_present=True

        metadata = GeopysparkCubeMetadata(self.get_collection_metadata(collection_id))
        layer_source_info = metadata.get("_vito", "data_source", default={})

        if layer_source_info.get("type") == "merged_by_common_name":
            logger.info(f"Resolving 'merged_by_common_name' collection {metadata.get('id')}")
            metadata = self._resolve_merged_by_common_name(
                collection_id=collection_id, metadata=metadata, load_params=load_params,
                temporal_extent=temporal_extent, spatial_extent=spatial_extent
            )
            collection_id = metadata.get("id")
            layer_source_info = metadata.get("_vito", "data_source", default={})
            logger.info(f"Resolved 'merged_by_common_name' to collection {metadata.get('id')}")

        sar_backscatter_compatible = layer_source_info.get("sar_backscatter_compatible", False)
        if load_params.sar_backscatter is not None and not sar_backscatter_compatible:
            raise OpenEOApiException(message="""Process "sar_backscatter" is not applicable for collection {c}."""
                                     .format(c=collection_id), status_code=400)

        layer_source_type = layer_source_info.get("type", "Accumulo").lower()
        is_utm = layer_source_info.get("is_utm", False)
        catalog_type = layer_source_info.get("catalog_type", "")  # E.g. STAC, Opensearch, Creodias

        postprocessing_band_graph = metadata.get("_vito", "postprocessing_bands", default=None)
        logger.debug("Cube source type: {s!r}".format(s=layer_source_type))
        cell_width = float(metadata.get("cube:dimensions", "x", "step", default=10.0))
        cell_height = float(metadata.get("cube:dimensions", "y", "step", default=10.0))

        bands = load_params.bands
        if bands:
            band_indices = [metadata.get_band_index(b) for b in bands]
            metadata = metadata.filter_bands(bands)
            # Note: the `metadata.filter_bands()` includes resolving band naming ("common_name" and aliases)
            #       which we want to preserve in `normalized_band_selection`,
            #       before `metadata.rename_labels()` changes it back to the originally requested band names.
            normalized_band_selection = metadata.band_names
            metadata = metadata.rename_labels(metadata.band_dimension.name, target=bands, source=metadata.band_names)
        else:
            band_indices = None
            # Use ordered bands selection from metadata
            normalized_band_selection = metadata.band_names if metadata.has_band_dimension() else None

        logger.debug("band_indices: {b!r}".format(b=band_indices))
        # TODO: avoid this `still_needs_band_filter` ugliness.
        #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        still_needs_band_filter = False

        #band specific gsd can override collection default
        band_gsds = [band.gsd['value'] for band in metadata.bands if band.gsd is not None]
        if len(band_gsds) > 0:

            def smallest_cell_size(band_gsd, coordinate_index):
                return (
                    min(size[coordinate_index] for size in band_gsd) if isinstance(band_gsd[0], list)
                    else band_gsd[coordinate_index]
                )

            cell_width = float(min(smallest_cell_size(band_gsd, coordinate_index=0) for band_gsd in band_gsds))
            cell_height = float(min(smallest_cell_size(band_gsd, coordinate_index=1) for band_gsd in band_gsds))

        native_crs = self._native_crs(metadata)

        metadata = metadata.filter_temporal(from_date, to_date)

        correlation_id = env.get(EVAL_ENV_KEY.CORRELATION_ID, "")
        logger.info("Correlation ID is '{cid}'".format(cid=correlation_id))

        logger.info("Detected process types:" + str(load_params.process_types))


        feature_flags = load_params.get("featureflags", {})
        experimental = feature_flags.get("experimental", False)

        pysc = geopyspark.get_spark_context()
        description = f"load_collection_{collection_id}"
        if bands:
            description += f"_{'-'.join(bands)}"
        pysc.setJobDescription(description)

        jvm = get_jvm()

        extent = None


        extent = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))
        metadata = metadata.filter_bbox(west=west, south=south, east=east, north=north, crs=srs)

        geometries = load_params.aggregate_spatial_geometries
        empty_geometries = isinstance(geometries, DriverVectorCube) and len(geometries.get_geometries()) == 0
        geometries = None if empty_geometries else geometries  # TODO: ensure that driver vector cube can not have empty geometries.
        if not geometries:
            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, srs)
        else:
            projected_polygons = to_projected_polygons(
                jvm, geometries, crs=srs, buffer_points=True
            )

        if native_crs == 'UTM':
            target_epsg_code = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
        else:
            target_epsg_code = int(native_crs.split(":")[-1])


        if (load_params.target_resolution is not None ):
            if load_params.target_resolution[0] != 0.0 and load_params.target_resolution[1] != 0.0:
                cell_width = float(load_params.target_resolution[0])
                cell_height = float(load_params.target_resolution[1])


        if (load_params.target_crs is not None ):
            if load_params.target_resolution is not None and load_params.target_resolution[0] != 0.0 and load_params.target_resolution[1] != 0.0:
                if isinstance(load_params.target_crs,int):
                    target_epsg_code = load_params.target_crs
                elif isinstance(load_params.target_crs,dict) and load_params.target_crs.get("id",{}).get("code") == 'Auto42001':
                    target_epsg_code = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
                else:
                    target_epsg_code = pyproj.CRS.from_user_input(load_params.target_crs).to_epsg()

        projected_polygons_native_crs = (getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$")
                                         .reproject(projected_polygons, target_epsg_code))
        logger.debug(projected_polygons_native_crs)
        logger.debug(projected_polygons_native_crs.geometries())
        logger.debug(projected_polygons_native_crs.extent())
        logger.debug(projected_polygons_native_crs.polygons()[0].toString())

        datacubeParams, single_level = datacube_parameters.create(load_params, env, jvm)
        opensearch_endpoint = layer_source_info.get(
            "opensearch_endpoint", get_backend_config().default_opensearch_endpoint
        )
        max_soft_errors_ratio = env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0)
        if feature_flags.get("no_resample_on_read", False):
            logger.info("Setting NoResampleOnRead to true")
            datacubeParams.setNoResampleOnRead(True)

        val = smart_bool(feature_flags.get("use_new_feature_extent_intersection", False))
        datacubeParams.setUseNewFeatureExtentIntersection(val)

        if "use_new_feature_extent_intersection_2" in feature_flags:
            val = smart_bool(feature_flags.get("use_new_feature_extent_intersection_2"))
            logger.info(f"Setting useNewFeatureExtentIntersection2 to {val}")
            datacubeParams.setUseNewFeatureExtentIntersection2(val)

        def metadata_properties(flatten_eqs=True) -> Dict[str, object]:
            layer_properties = metadata.get("_vito", "properties", default={})
            custom_properties = load_params.properties

            all_properties = {
                property_name: filter_properties.extract_literal_match(condition, env)
                for property_name, condition in {**layer_properties, **custom_properties}.items()
            }

            def eq_value(criterion: Dict[str, object]) -> object:
                if len(criterion) != 1:
                    raise ValueError(f'expected a single "eq" criterion, was {criterion}')

                #TODO https://github.com/Open-EO/openeo-geotrellis-extensions/issues/39
                return list(criterion.values())[0]

            return ({property_name: eq_value(criterion) for property_name, criterion in all_properties.items()}
                    if flatten_eqs else all_properties)

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


        def file_s2_pyramid():
            def pyramid_factory(
                opensearch_endpoint: str,
                opensearch_collection_id: str,
                opensearch_link_titles,
                root_path: str,
            ):
                opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
                    opensearch_endpoint,
                    is_utm,
                    "",
                    metadata.band_names,
                    catalog_type,
                    metadata.parallel_query(),
                    metadata.select_one_orbit_per_day(),
                )

                return jvm.org.openeo.geotrellis.file.PyramidFactory(
                    opensearch_client,
                    opensearch_collection_id,
                    opensearch_link_titles,
                    root_path,
                    jvm.geotrellis.raster.CellSize(cell_width, cell_height),
                    experimental,
                    max_soft_errors_ratio,
                )

            return file_pyramid(pyramid_factory)


        def file_probav_pyramid():
            cell_width = float(metadata.get("cube:dimensions", "x", "step", default=10.0))
            cell_height = float(metadata.get("cube:dimensions", "y", "step", default=10.0))
            factory = jvm.org.openeo.geotrellis.file.ProbaVPyramidFactory(
                opensearch_endpoint,
                layer_source_info.get('opensearch_collection_id'),
                metadata.opensearch_link_titles,
                layer_source_info.get('root_path'),
                jvm.geotrellis.raster.CellSize(cell_width, cell_height)
            )
            return factory.pyramid_seq(extent, srs, from_date, to_date, correlation_id)


        def create_pyramid(factory):
            try:
                if single_level:
                    # TODO EP-3561 UTM is not always the native projection of a layer (PROBA-V), need to determine optimal projection
                    return factory.datacube_seq(
                        projected_polygons_native_crs, from_date, to_date,
                        metadata_properties(), correlation_id, datacubeParams
                    )
                else:
                    if geometries:
                        return factory.pyramid_seq(
                            projected_polygons.polygons(), projected_polygons.crs(), from_date, to_date,
                            metadata_properties(), correlation_id
                        )
                    else:
                        return factory.pyramid_seq(
                            extent, srs, from_date, to_date,
                            metadata_properties(), correlation_id
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


        def file_pyramid(pyramid_factory):
            opensearch_collection_id = layer_source_info['opensearch_collection_id']
            opensearch_link_titles = metadata.opensearch_link_titles
            root_path = layer_source_info.get('root_path',None)
            factory = pyramid_factory(opensearch_endpoint, opensearch_collection_id, opensearch_link_titles, root_path)
            return create_pyramid(factory)


        def geotiff_pyramid():
            glob_pattern = layer_source_info['glob_pattern']
            date_regex = layer_source_info['date_regex']

            new_pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex)

            return self._geotiff_pyramid_factories.setdefault(collection_id, new_pyramid_factory) \
                .pyramid_seq(extent, srs, from_date, to_date)

        def sentinel_hub_pyramid():
            # TODO: move the metadata manipulation out of this function and get rid of the nonlocal?
            nonlocal metadata

            dependencies = env.get(EVAL_ENV_KEY.DEPENDENCIES, [])
            sar_backscatter_arguments: Optional[SarBackscatterArgs] = (
                _get_sar_backscatter_arguments(load_params=load_params, env=env) if sar_backscatter_compatible else None
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

                return (pyramid_factory.datacube_seq(projected_polygons_native_crs, from_date, to_date,metadata_properties(),collection_id,datacubeParams) if single_level
                        else pyramid_factory.pyramid_seq(extent, srs, from_date, to_date))
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
                        (condition, byoc_id) = metadata_properties(flatten_eqs=False).get('byoc_id', (None, None))
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

                cell_size = jvm.geotrellis.raster.CellSize(cell_width, cell_height)
                no_data_value = metadata.get_nodata_value(load_params.bands, 0.0)

                if ConfigParams().is_kube_deploy:
                    access_token = env[EVAL_ENV_KEY.USER].internal_auth_data["access_token"]

                    pyramid_factory = jvm.org.openeo.geotrellissentinelhub.PyramidFactory.withFixedAccessToken(
                        endpoint,
                        shub_collection_id,
                        dataset_id,
                        access_token,
                        sentinel_hub.processing_options(collection_id,
                                                        sar_backscatter_arguments) if sar_backscatter_arguments else {},
                        sample_type,
                        cell_size,
                        max_soft_errors_ratio,
                        no_data_value,
                    )
                else:
                    sentinel_hub_client_alias = env.get(EVAL_ENV_KEY.SENTINEL_HUB_CLIENT_ALIAS, "default")
                    logger.debug(f"Sentinel Hub client alias: {sentinel_hub_client_alias}")

                    if sentinel_hub_client_alias == 'default':
                        sentinel_hub_client_id = self._default_sentinel_hub_client_id
                        sentinel_hub_client_secret = self._default_sentinel_hub_client_secret
                    else:
                        vault_token = env[EVAL_ENV_KEY.VAULT_TOKEN]
                        sentinel_hub_client_id, sentinel_hub_client_secret = (
                            self._vault.get_sentinel_hub_credentials(sentinel_hub_client_alias, vault_token))

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
                        max_soft_errors_ratio,
                        no_data_value,
                    )

                unflattened_metadata_properties = metadata_properties(flatten_eqs=False)
                sentinel_hub.assure_polarization_from_sentinel_bands(metadata, unflattened_metadata_properties)

                return (
                    pyramid_factory.datacube_seq(projected_polygons_native_crs.polygons(),
                                                 projected_polygons_native_crs.crs(), from_date, to_date,
                                                 shub_band_names, unflattened_metadata_properties,
                                                 datacubeParams, correlation_id) if single_level
                    else pyramid_factory.pyramid_seq(extent, srs, from_date, to_date, shub_band_names,
                                                     unflattened_metadata_properties, correlation_id))

        def creo_pyramid():
            mission = layer_source_info['mission']
            level = layer_source_info['level']
            catalog = CreoCatalogClient(mission=mission, level=level)
            product_paths = catalog.query_product_paths(datetime.strptime(from_date[:10], "%Y-%m-%d"),
                                                        datetime.strptime(to_date[:10], "%Y-%m-%d"),
                                                        ulx=west, uly=north,
                                                        brx=east, bry=south)
            # TODO: geotrelliss3.CreoPyramidFactory no longer exists.
            return jvm.org.openeo.geotrelliss3.CreoPyramidFactory(product_paths, metadata.band_names) \
                .datacube_seq(projected_polygons_native_crs, from_date, to_date,{},collection_id)


        def globspatialonly_pyramid():
            if len(metadata.band_names) != 1:
                raise ValueError("expected a single band name for collection {cid}, got {bs} instead".format(
                    cid=collection_id, bs=metadata.band_names))

            data_glob = layer_source_info['data_glob']
            band_names = metadata.band_names
            client_type = catalog_type if catalog_type != "" else "globspatialonly"
            opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
                data_glob, False, None, band_names, client_type
            )
            factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
                opensearch_client,
                "",
                band_names,
                "",
                jvm.geotrellis.raster.CellSize(cell_width, cell_height),
                False,
                max_soft_errors_ratio,
            )
            return create_pyramid(factory)

        def file_cgls_pyramid():
            data_glob = layer_source_info['data_glob']
            date_regex = layer_source_info['date_regex']
            band_names = metadata.band_names

            client_type = catalog_type if catalog_type != "" else "cgls"
            opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
                data_glob, False, date_regex, band_names, client_type
            )
            factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
                opensearch_client,
                "",
                band_names,
                "",
                jvm.geotrellis.raster.CellSize(cell_width, cell_height),
                False,
                max_soft_errors_ratio,
            )
            return create_pyramid(factory)

        def file_agera5_pyramid():
            data_glob = layer_source_info['data_glob']
            date_regex = layer_source_info['date_regex']
            band_marker = layer_source_info.get('band_marker','dewpoint-temperature')
            band_names = metadata.band_names

            opensearch_client = jvm.org.openeo.opensearch.backends.Agera5SearchClient.apply(
                data_glob, False, date_regex, band_names, band_marker
            )
            factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
                opensearch_client,
                "",
                band_names,
                "",
                jvm.geotrellis.raster.CellSize(cell_width, cell_height),
                False,
                max_soft_errors_ratio,
            )
            return create_pyramid(factory)


        if layer_source_type == 'file-s2':
            pyramid = file_s2_pyramid()
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
        elif layer_source_type == "file-cgls2":
            pyramid = file_cgls_pyramid()
        elif layer_source_type == 'file-agera5' or layer_source_type == 'file-glob':
            pyramid = file_agera5_pyramid()
        elif layer_source_type == 'file-globspatialonly':
            pyramid = globspatialonly_pyramid()
        elif layer_source_type == 'file-oscars'  or layer_source_type == "cgls_oscars":
            pyramid = file_s2_pyramid()
        elif layer_source_type == 'creodias-s1-backscatter':
            sar_backscatter_arguments = _get_sar_backscatter_arguments(load_params=load_params, env=env)
            #make a copy before modifying: it is used as a cache key
            sar_backscatter_arguments = deepcopy(sar_backscatter_arguments)
            sar_backscatter_arguments.options["resolution"] = (cell_width, cell_height)
            s1_backscatter_orfeo = openeogeotrellis.collections.s1backscatter_orfeo.get_implementation(
                version=sar_backscatter_arguments.options.get("implementation_version", "2"),
                jvm=jvm
            )
            pyramid = s1_backscatter_orfeo.creodias(
                projected_polygons=projected_polygons_native_crs,
                from_date=from_date, to_date=to_date,
                collection_id=collection_id,
                correlation_id=correlation_id,
                sar_backscatter_arguments=sar_backscatter_arguments,
                bands=bands,
                extra_properties=metadata_properties(),
                datacubeParams = datacubeParams,
                max_soft_errors_ratio=max_soft_errors_ratio,
                spatial_extent=load_params.spatial_extent,
                use_stac_client=layer_source_info.get("use_stac_client", False),
                feature_flags=layer_source_info.get("load_stac_feature_flags", {}),
            )
        elif layer_source_type == 'file-s3':
            native_cell_size = jvm.geotrellis.raster.CellSize(
                float(metadata.get("cube:dimensions", "x", "step")),
                float(metadata.get("cube:dimensions", "y", "step"))
            )
            # Local import to save some RAM and avoid potential confusing error:
            from openeogeotrellis.collections import sentinel3

            pyramid = sentinel3.pyramid(
                metadata_properties(),
                projected_polygons_native_crs,
                from_date,
                to_date,
                metadata.opensearch_link_titles,
                datacubeParams,
                native_cell_size,
                {**feature_flags, "load_stac_feature_flags": layer_source_info.get("load_stac_feature_flags", {})},
                jvm,
                spatial_extent=load_params.spatial_extent,
                use_stac_client=layer_source_info.get("use_stac_client", False),
            )
        elif layer_source_type == "file-s5p":
            native_cell_size = jvm.geotrellis.raster.CellSize(
                float(metadata.get("cube:dimensions", "x", "step")), float(metadata.get("cube:dimensions", "y", "step"))
            )
            # Local import to save some RAM and avoid potential confusing error:
            from openeogeotrellis.collections.load_sentinel5p import pyramid as s5p_pyramid

            load_stac_feature_flags = layer_source_info.get("load_stac_feature_flags", {})
            if not "url" in load_stac_feature_flags and layer_source_info.get("opensearch_endpoint"):
                logger.warning(
                    "Using legacy opensearch_endpoint for S5P collection. Please use load_stac_feature_flags.url instead."
                )
                load_stac_feature_flags["url"] = layer_source_info["opensearch_endpoint"]
            pyramid = s5p_pyramid(
                metadata_properties(),
                projected_polygons_native_crs,
                from_date,
                to_date,
                normalized_band_selection,
                datacubeParams,
                native_cell_size,
                {**feature_flags, "load_stac_feature_flags": load_stac_feature_flags},
                jvm,
                spatial_extent=load_params.spatial_extent,
                collection_id=collection_id,
            )
        elif layer_source_type == "stac":
            cube = load_stac(
                url=layer_source_info["url"],
                load_params=load_params,
                env=env,
                layer_properties=metadata.get("_vito", "properties", default={}),
                batch_jobs=None,
                normalized_band_selection=normalized_band_selection,
                feature_flags=layer_source_info.get("load_stac_feature_flags", {}),
                data_cube_parameters=datacubeParams,
                pg_node_id=pg_node_id,
            )
            pyramid = cube.pyramid.levels
            metadata = cube.metadata
        elif layer_source_type == 'accumulo':
            pyramid = accumulo_pyramid()
        elif layer_source_type == 'testing':
            import re

            tile_cols, tile_rows = map(int, re.match(r".*?(\d+)x(\d+)", collection_id).groups())
            assert tile_cols == tile_rows

            pyramid = load_test_collection(
                tile_size=tile_cols,
                collection_metadata=metadata,
                extent=extent,
                srs=srs,
                from_date=from_date,
                to_date=to_date,
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

        if postprocessing_band_graph != None:
            visitor = GeotrellisTileProcessGraphVisitor()
            image_collection = image_collection.apply_dimension(
                process=visitor.accept_process_graph(postprocessing_band_graph),
                dimension=image_collection.metadata.band_dimension.name,
                context={},
                env=EvalEnv(),
            )

        if still_needs_band_filter:
            # TODO: avoid this `still_needs_band_filter` ugliness.
            #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
            image_collection = image_collection.filter_bands(band_indices)

        pysc.setJobDescription("")

        return image_collection


    def _resolve_merged_by_common_name(
            self, collection_id: str, metadata: GeopysparkCubeMetadata, load_params: LoadParameters,
            temporal_extent: Tuple[str, str], spatial_extent: dict
    ) -> GeopysparkCubeMetadata:
        upstream_metadatas = [GeopysparkCubeMetadata(self.get_collection_metadata(cid))
                              for cid in metadata.get("_vito", "data_source", "merged_collections")]
        # Check sources in order of priority and skip ones where we can detect missing products.
        for m in sorted(upstream_metadatas, key=lambda m: m.common_name_priority(), reverse=True):
            if m.get("_vito", "data_source", "check_missing_products"):
                missing = check_missing_products(
                    collection_metadata=m,
                    temporal_extent=temporal_extent, spatial_extent=spatial_extent,
                    properties=load_params.properties,
                )
                if missing:
                    logger.info(
                        f"(common_name) {collection_id!r}: skipping {m.provider_backend()!r} because of {len(missing)} missing products: {str_truncate(repr(missing), 1000)}"
                    )
                    continue
            logger.info(f"(common_name) {collection_id!r}: using {m.provider_backend()!r}.")
            return m

        raise OpenEOApiException(message=f"No fitting provider:backend found for {collection_id!r}")

    def _native_crs(self, metadata: GeopysparkCubeMetadata) -> str:
        dimension_crss = [d.crs for d in metadata.spatial_dimensions]

        if len(dimension_crss) > 0:
            crs = dimension_crss[0]
            if isinstance(crs, dict):  # PROJJSON
                crs_id = crs['id']
                authority: str = crs_id['authority']
                code: str = crs_id['code']

                if authority.lower() == 'ogc' and code.lower() == 'auto42001':
                    return "UTM"

                if authority.lower() == 'epsg':
                    return f"EPSG:{code}"

                raise NotImplementedError(f"unsupported CRS: {crs}")

            if isinstance(crs, int):  # EPSG code
                return f"EPSG:{crs}"

            raise NotImplementedError(f"unsupported CRS format: {crs} in cube:dimension, provide an int for epsg codes or a projjson dict.")

        return "UTM"  # LANDSAT7_ETM_L2 doesn't have any, for example

    def derive_temporal_extent(
        self, collection_id: str, load_params: LoadParameters
    ) -> Tuple[Optional[str], Optional[str]]:
        metadata_json = self.get_collection_metadata(collection_id=collection_id)
        metadata = GeopysparkCubeMetadata(metadata_json)

        temporal_extent_constraints = load_params.temporal_extent

        # The first temporal interval should encompass the other temporal intervals.
        # The outer bounds are still calculated just in case.
        # https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#temporal-extent-object
        catalog_temporal_extent = metadata.get("extent", "temporal", "interval", default=None)
        outer_bounds = [None, None]
        if catalog_temporal_extent:
            for extent in catalog_temporal_extent:
                if extent[0]:
                    if outer_bounds[0] is None:
                        outer_bounds[0] = extent[0]
                    else:
                        outer_bounds[0] = min(outer_bounds[0], extent[0])
                if extent[1]:
                    if outer_bounds[1] is None:
                        outer_bounds[1] = extent[1]
                    else:
                        outer_bounds[1] = max(outer_bounds[1], extent[1])
        if temporal_extent_constraints is None:
            temporal_extent = outer_bounds
        else:
            # take the intersection of outer_bounds and temporal_extent
            beginnings = []
            if outer_bounds[0]:
                beginnings.append(outer_bounds[0])
            if temporal_extent_constraints[0]:
                beginnings.append(temporal_extent_constraints[0])
            if not beginnings:
                beginnings.append(None)

            ends = []
            if outer_bounds[1]:
                ends.append(outer_bounds[1])
            if temporal_extent_constraints[1]:
                ends.append(temporal_extent_constraints[1])
            if not ends:
                ends.append(None)

            temporal_extent = (
                max(beginnings),  # ISO date is sortable like a string
                min(ends),
            )
        return temporal_extent

    def estimate_number_of_temporal_observations(self,
                                                 collection_id: str,
                                                 load_params: LoadParameters,
                                                 ) -> int:
        temporal_extent = self.derive_temporal_extent(collection_id, load_params)

        metadata_json = self.get_collection_metadata(collection_id=collection_id)
        metadata = GeopysparkCubeMetadata(metadata_json)

        consider_as_singular_time_step = deep_get(metadata_json, "_vito", "data_source",
                                                  "consider_as_singular_time_step", default=False)
        if consider_as_singular_time_step:
            return 1

        # step could be explicitly 'None', so we use 'or' to specify the default
        temporal_step = metadata.get("cube:dimensions", "t", "step", default=None) or "P10D"

        # https://github.com/stac-extensions/datacube?tab=readme-ov-file#temporal-dimension-object
        temporal_step = parse_approximate_isoduration(temporal_step)
        temporal_step = temporal_step.total_seconds()

        from_date, to_date = normalize_temporal_extent((temporal_extent[0], temporal_extent[1]))
        to_date_parsed = dateutil.parser.parse(to_date).replace(tzinfo=pytz.UTC)
        from_date_parsed = dateutil.parser.parse(from_date).replace(tzinfo=pytz.UTC)
        number_of_temporal_observations = (to_date_parsed - from_date_parsed).total_seconds() / temporal_step
        number_of_temporal_observations = max(math.floor(number_of_temporal_observations), 1)
        return number_of_temporal_observations

    def get_collection_queryables(self, collection_id: Union[str, None]) -> Union[QueryablesListing, flask.Response]:
        metadata = self.get_collection_metadata(collection_id)
        data_source = deep_get(metadata, "_vito", "data_source", default={})
        if data_source.get("type") == "stac" and (url := data_source.get("url")):
            # TODO: for now (experimental phase), we just do naive redirect here.
            #       Instead: proxy+cache this document.
            #       Or include it in (precompiled) layercatalog (#1175)?
            return flask.redirect(location=f"{url}/queryables")

        return super().get_collection_queryables(collection_id=collection_id)


def get_layer_catalog(
    vault: Vault = None,
    # TODO: just call this arg `enrich_metadata` is this is about more than just OpenSearch
    opensearch_enrich: Optional[bool] = None,
) -> GeoPySparkLayerCatalog:
    backend_config = get_backend_config()
    enrich_metadata = opensearch_enrich if opensearch_enrich is not None else backend_config.opensearch_enrich
    metadata = load_catalog_files(
        catalog_files=backend_config.layer_catalog_files,
        enrich_metadata=enrich_metadata,
        default_opensearch_endpoint=backend_config.default_opensearch_endpoint,
    )
    return GeoPySparkLayerCatalog(
        all_metadata=list(metadata.values()),
        vault=vault,
    )


def _get_sar_backscatter_arguments(load_params: LoadParameters, env: EvalEnv) -> SarBackscatterArgs:
    """
    Get SarBackscatterArgs from LoadParameters if available,
    otherwise: look in process registry schema to pick defaults that
    are possibly overridden in deployment configuration.
    """
    if load_params.sar_backscatter:
        sar_backscatter_arguments = load_params.sar_backscatter
    else:
        try:
            # TODO: is it possible to avoid hardcoding `GpsProcessing` here?
            #       Note that `env.get("backend_implementation").processing` is not available here anymore
            #       because of that WhiteListEvalEnv caching business.
            #       Also note that this requires a local import to break an import cycle between
            #       openeogeotrellis.backend and openeogeotrellis.layercatalog
            import openeogeotrellis.backend
            processing = openeogeotrellis.backend.GpsProcessing()
            api_version = env.openeo_api_version()
            sar_backscatter_arguments = processing.get_default_sar_backscatter_arguments(api_version=api_version)
        except Exception as e:
            logger.warning(f"_get_sar_backscatter_arguments failed: {e!r}")
            sar_backscatter_arguments = SarBackscatterArgs()
    return sar_backscatter_arguments


if __name__ == "__main__":
    dump_layer_catalog()
