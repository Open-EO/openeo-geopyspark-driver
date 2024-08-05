import argparse
import datetime as dt
import functools
import json
import logging
import math
import sys
from copy import deepcopy
from datetime import datetime
from functools import lru_cache
from typing import List, Dict, Iterable, Optional, Tuple, Union

import dateutil.parser
import geopyspark
import py4j.protocol
import pyproj
import pyspark.sql.utils
import pytz

from openeo.metadata import Band
from openeo.util import TimingLogger, deep_get, str_truncate
from openeo_driver import filter_properties
from openeo_driver.backend import CollectionCatalog, LoadParameters
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, InternalException, ProcessGraphComplexityException
from openeo_driver.filter_properties import extract_literal_match
from openeo_driver.util.geometry import reproject_bounding_box
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.util.http import requests_with_retry
from openeo_driver.utils import read_json, EvalEnv, WhiteListEvalEnv, smart_bool
from shapely.geometry import box
from shapely.geometry.base import BaseGeometry

from openeogeotrellis import sentinel_hub
from openeogeotrellis.catalogs.creo import CreoCatalogClient
from openeogeotrellis.collections.s1backscatter_orfeo import get_implementation as get_s1_backscatter_orfeo
from openeogeotrellis.collections import sentinel3
from openeogeotrellis.collections.testing import load_test_collection
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.load_stac import load_stac
from openeogeotrellis.opensearch import OpenSearch, OpenSearchOscars, OpenSearchCreodias, OpenSearchCdse
from openeogeotrellis.processgraphvisiting import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.utils import (
    dict_merge_recursive,
    to_projected_polygons,
    get_jvm,
    normalize_temporal_extent,
    calculate_rough_area,
    parse_approximate_isoduration,
    reproject_cellsize,
)
from openeogeotrellis.vault import Vault

VAULT_TOKEN = 'vault_token'
SENTINEL_HUB_CLIENT_ALIAS = 'sentinel_hub_client_alias'
MAX_SOFT_ERRORS_RATIO = 'max_soft_errors_ratio'
DEPENDENCIES = 'dependencies'
PYRAMID_LEVELS = 'pyramid_levels'
REQUIRE_BOUNDS = 'require_bounds'
CORRELATION_ID = 'correlation_id'
USER = 'user'
ALLOW_EMPTY_CUBES = "allow_empty_cubes"
DO_EXTENT_CHECK = "do_extent_check"
WHITELIST = [
    VAULT_TOKEN,
    SENTINEL_HUB_CLIENT_ALIAS,
    MAX_SOFT_ERRORS_RATIO,
    DEPENDENCIES,
    PYRAMID_LEVELS,
    REQUIRE_BOUNDS,
    CORRELATION_ID,
    USER,
    ALLOW_EMPTY_CUBES,
    DO_EXTENT_CHECK,
]
LARGE_LAYER_THRESHOLD_IN_PIXELS = pow(10, 11)

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

    def create_datacube_parameters(self, load_params, env):
        jvm = get_jvm()
        feature_flags = load_params.get("featureflags", {})
        tilesize = feature_flags.get("tilesize", 256)
        default_temporal_resolution = "ByDay"
        default_indexReduction = 6
        #if len(load_params.process_types) == 1 and ProcessType.GLOBAL_TIME in load_params.process_types:
            # for pure timeseries processing, adjust partitioning strategy
            #default_temporal_resolution = "None"
            #default_indexReduction = 0
        indexReduction = feature_flags.get("indexreduction", default_indexReduction)
        temporalResolution = feature_flags.get("temporalresolution", default_temporal_resolution)
        datacubeParams = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        # WTF simple assignment to a var in a scala class doesn't work??
        getattr(datacubeParams, "tileSize_$eq")(tilesize)
        getattr(datacubeParams, "maskingStrategyParameters_$eq")(load_params.custom_mask)
        logger.debug(f"Using load_params.data_mask {load_params.data_mask!r}")
        if isinstance(load_params.data_mask, GeopysparkDataCube):
            datacubeParams.setMaskingCube(load_params.data_mask.get_max_level().srdd.rdd())
        datacubeParams.setPartitionerIndexReduction(indexReduction)
        datacubeParams.setPartitionerTemporalResolution(temporalResolution)

        datacubeParams.setAllowEmptyCube(feature_flags.get("allow_empty_cube", env.get(ALLOW_EMPTY_CUBES, False)))

        globalbounds = feature_flags.get("global_bounds", True)
        if globalbounds and load_params.global_extent is not None and len(load_params.global_extent) > 0:
            ge = load_params.global_extent
            datacubeParams.setGlobalExtent(float(ge["west"]), float(ge["south"]), float(ge["east"]), float(ge["north"]),
                                           ge["crs"])
        single_level = env.get(PYRAMID_LEVELS, 'all') != 'all'
        if single_level:
            getattr(datacubeParams, "layoutScheme_$eq")("FloatingLayoutScheme")

        if load_params.pixel_buffer is not None:
            datacubeParams.setPixelBuffer(load_params.pixel_buffer[0],load_params.pixel_buffer[1])

        load_per_product = feature_flags.get("load_per_product",None)
        if load_per_product is not None:
            datacubeParams.setLoadPerProduct(load_per_product)
        elif(get_backend_config().default_reading_strategy == "load_per_product"):
            datacubeParams.setLoadPerProduct(True)

        if (get_backend_config().default_tile_size is not None):
            if "tilesize" not in feature_flags:
                getattr(datacubeParams, "tileSize_$eq")(get_backend_config().default_tile_size)


        datacubeParams.setResampleMethod(GeopysparkDataCube._get_resample_method(load_params.resample_method))

        if load_params.filter_temporal_labels is not None:
            from openeogeotrellis.backend import GeoPySparkBackendImplementation
            labels_filter = GeoPySparkBackendImplementation.accept_process_graph(load_params.filter_temporal_labels["process_graph"])
            datacubeParams.setTimeDimensionFilter(labels_filter.builder)
        return datacubeParams, single_level


    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> GeopysparkDataCube:

        if smart_bool(env.get(DO_EXTENT_CHECK, True)):
            env_validate = env.push({
                "allow_check_missing_products": False,
            })
            issues = extra_validation_load_collection(collection_id, load_params, env_validate)
            # Only care for certain errors and make list of strings:
            issues = [e["message"] for e in issues if e["code"] == "ExtentTooLarge"]
            if issues:
                if env.get("sync_job", False):
                    raise ProcessGraphComplexityException(
                        ProcessGraphComplexityException.message + f" Reasons: {' '.join(issues)}"
                    )
                else:
                    raise ProcessGraphComplexityException(
                        "Found errors in process graph. Disable this check with 'job_options.do_extent_check': " +
                        " ".join(issues))

        return self._load_collection_cached(collection_id, load_params, WhiteListEvalEnv(env, WHITELIST))

    @lru_cache(maxsize=20)
    def _load_collection_cached(self, collection_id: str, load_params: LoadParameters, env: EvalEnv) -> GeopysparkDataCube:
        logger.info("Creating layer for {c} with load params {p}".format(c=collection_id, p=load_params))

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
            if env.get(REQUIRE_BOUNDS, False):
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
        logger.info("Layer source type: {s!r}".format(s=layer_source_type))
        cell_width = float(metadata.get("cube:dimensions", "x", "step", default=10.0))
        cell_height = float(metadata.get("cube:dimensions", "y", "step", default=10.0))

        bands = load_params.bands
        if bands:
            band_indices = [metadata.get_band_index(b) for b in bands]
            metadata = metadata.filter_bands(bands)
            metadata = metadata.rename_labels(metadata.band_dimension.name, bands, metadata.band_names)
        else:
            band_indices = None
        logger.info("band_indices: {b!r}".format(b=band_indices))
        # TODO: avoid this `still_needs_band_filter` ugliness.
        #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
        still_needs_band_filter = False

        #band specific gsd can override collection default
        band_gsds = [band.gsd['value'] for band in metadata.bands if band.gsd is not None]
        if len(band_gsds) > 0:
            def highest_resolution(band_gsd, coordinate_index):
                return (min(res[coordinate_index] for res in band_gsd) if isinstance(band_gsd[0], list)
                        else band_gsd[coordinate_index])

            cell_width = float(min(highest_resolution(band_gsd, coordinate_index=0) for band_gsd in band_gsds))
            cell_height = float(min(highest_resolution(band_gsd, coordinate_index=1) for band_gsd in band_gsds))

        native_crs = self._native_crs(metadata)

        metadata = metadata.filter_temporal(from_date, to_date)


        correlation_id = env.get(CORRELATION_ID, '')
        logger.info("Correlation ID is '{cid}'".format(cid=correlation_id))

        logger.info("Detected process types:" + str(load_params.process_types))


        feature_flags = load_params.get("featureflags", {})
        experimental = feature_flags.get("experimental", False)


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

        datacubeParams, single_level = self.create_datacube_parameters(load_params, env)
        opensearch_endpoint = layer_source_info.get(
            "opensearch_endpoint", get_backend_config().default_opensearch_endpoint
        )
        max_soft_errors_ratio = env.get(MAX_SOFT_ERRORS_RATIO, 0.0)
        no_data_value = metadata.get_nodata_value(load_params.bands, 0.0)
        if feature_flags.get("no_resample_on_read", False):
            logger.info("Setting NoResampleOnRead to true")
            datacubeParams.setNoResampleOnRead(True)

        def metadata_properties(flatten_eqs=True) -> Dict[str, object]:
            layer_properties = metadata.get("_vito", "properties", default={})
            custom_properties = load_params.properties

            all_properties = {property_name: filter_properties.extract_literal_match(condition)
                        for property_name, condition in {**layer_properties, **custom_properties}.items()}

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
                opensearch_endpoint,
                opensearch_collection_id,
                opensearch_link_titles,
                root_path,
            ):
                opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
                    opensearch_endpoint, is_utm, "", metadata.band_names, catalog_type, metadata.parallel_query()
                )
                return jvm.org.openeo.geotrellis.file.PyramidFactory(
                    opensearch_client,
                    opensearch_collection_id,
                    opensearch_link_titles,
                    root_path,
                    jvm.geotrellis.raster.CellSize(cell_width, cell_height),
                    experimental,
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
                elif isinstance(e, pyspark.sql.utils.IllegalArgumentException):
                    msg = e.desc
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

            dependencies = env.get(DEPENDENCIES, [])
            sar_backscatter_arguments: Optional[SarBackscatterArgs] = (
                (load_params.sar_backscatter or SarBackscatterArgs()) if sar_backscatter_compatible
                else None
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

                if ConfigParams().is_kube_deploy:
                    access_token = env[USER].internal_auth_data["access_token"]

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
                    sentinel_hub_client_alias = env.get(SENTINEL_HUB_CLIENT_ALIAS, 'default')
                    logger.debug(f"Sentinel Hub client alias: {sentinel_hub_client_alias}")

                    if sentinel_hub_client_alias == 'default':
                        sentinel_hub_client_id = self._default_sentinel_hub_client_id
                        sentinel_hub_client_secret = self._default_sentinel_hub_client_secret
                    else:
                        vault_token = env[VAULT_TOKEN]
                        sentinel_hub_client_id, sentinel_hub_client_secret = (
                            self._vault.get_sentinel_hub_credentials(sentinel_hub_client_alias, vault_token))

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
            )
            return create_pyramid(factory)

        logger.info("loading pyramid {s}".format(s=layer_source_type))

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
            sar_backscatter_arguments = load_params.sar_backscatter or SarBackscatterArgs()
            s1_backscatter_orfeo = get_s1_backscatter_orfeo(
                version=sar_backscatter_arguments.options.get("implementation_version", "2"),
                jvm=jvm
            )
            pyramid = s1_backscatter_orfeo.creodias(
                projected_polygons=projected_polygons_native_crs,
                from_date=from_date, to_date=to_date,
                correlation_id=correlation_id,
                sar_backscatter_arguments=sar_backscatter_arguments,
                bands=bands,
                extra_properties=metadata_properties(),
                datacubeParams = datacubeParams,
                max_soft_errors_ratio=max_soft_errors_ratio
            )
        elif layer_source_type == 'file-s3':
            pyramid = sentinel3.pyramid(metadata_properties(),
                                        projected_polygons_native_crs, from_date, to_date,
                                        metadata.opensearch_link_titles, datacubeParams,
                                        jvm.geotrellis.raster.CellSize(cell_width, cell_height), feature_flags, jvm,
                                        )
        elif layer_source_type == 'stac':
            cube = load_stac(layer_source_info["url"], load_params, env,
                             layer_properties=metadata.get("_vito", "properties", default={}),
                             batch_jobs=None, override_band_names=metadata.band_names)
            pyramid = cube.pyramid.levels
            metadata = cube.metadata
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

    def derive_temporal_extent(self, collection_id: str, load_params: LoadParameters) -> List[Optional[str]]:
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


# Type annotation aliases to make things more self-documenting
CollectionId = str
CollectionMetadataDict = Dict[str, Union[str, dict, list]]
CatalogDict = Dict[CollectionId, CollectionMetadataDict]


def _get_layer_catalog(
    catalog_files: Optional[List[str]] = None,
    opensearch_enrich: Optional[bool] = None,
) -> CatalogDict:
    """
    Get layer catalog (from JSON files)
    """
    if opensearch_enrich is None:
        opensearch_enrich = get_backend_config().opensearch_enrich
    if catalog_files is None:
        catalog_files = ConfigParams().layer_catalog_metadata_files

    metadata: CatalogDict = {}

    def read_catalog_file(catalog_file) -> CatalogDict:
        return {coll["id"]: coll for coll in read_json(catalog_file)}

    logger.info(f"_get_layer_catalog: {catalog_files=}")
    for path in catalog_files:
        logger.info(f"_get_layer_catalog: reading {path}")
        metadata = dict_merge_recursive(metadata, read_catalog_file(path), overwrite=True)
        logger.info(f"_get_layer_catalog: collected {len(metadata)} collections")

    logger.info(f"_get_layer_catalog: {opensearch_enrich=}")
    if opensearch_enrich:
        opensearch_metadata = {}
        sh_collection_metadatas = None

        @functools.lru_cache
        def opensearch_instance(endpoint: str, variant: Optional[str] = None) -> OpenSearch:
            endpoint = endpoint.lower()

            if "oscars" in endpoint or "terrascope" in endpoint or "vito.be" in endpoint or variant == "oscars":
                opensearch = OpenSearchOscars(endpoint=endpoint)
            elif "creodias" in endpoint or variant == "creodias":
                opensearch = OpenSearchCreodias(endpoint=endpoint)
            elif "dataspace.copernicus.eu" in endpoint or variant == "cdse":
                opensearch = OpenSearchCdse(endpoint=endpoint)
            else:
                raise ValueError(endpoint)

            return opensearch

        for cid, collection_metadata in metadata.items():
            data_source = deep_get(collection_metadata, "_vito", "data_source", default={})
            os_cid = data_source.get("opensearch_collection_id")
            os_endpoint = data_source.get("opensearch_endpoint") or get_backend_config().default_opensearch_endpoint
            os_variant = data_source.get("opensearch_variant")
            if os_cid and os_endpoint and os_variant != "disabled":
                try:
                    opensearch_metadata[cid] = opensearch_instance(
                        endpoint=os_endpoint, variant=os_variant
                    ).get_metadata(collection_id=os_cid)
                except Exception as e:
                    logger.warning(f"Failed to enrich collection metadata of {cid}: {e}", exc_info=True)
            elif data_source.get("type") == "stac":
                url = data_source.get("url")
                logger.info(f"Getting collection metadata from {url}")
                import requests
                try:
                    resp = requests.get(url=url)
                    resp.raise_for_status()
                    opensearch_metadata[cid] = resp.json()
                except Exception as e:
                    logger.warning(f"Failed to enrich collection metadata of {cid}: {e}", exc_info=True)

            elif data_source.get("type") == "sentinel-hub":
                sh_stac_endpoint = "https://collections.eurodatacube.com/stac/index.json"

                # TODO: improve performance by only fetching necessary STACs
                if sh_collection_metadatas is None:
                    sh_collections_session = requests_with_retry()
                    sh_collections_resp = sh_collections_session.get(sh_stac_endpoint, timeout=60)
                    sh_collections_resp.raise_for_status()
                    sh_collection_metadatas = {
                        c["id"]: sh_collections_session.get(c["link"], timeout=60).json()
                        for c in sh_collections_resp.json()
                    }

                enrichment_id = data_source.get("enrichment_id")

                # DEM collections have the same datasource_type "dem" so they need an explicit enrichment_id
                if enrichment_id:
                    sh_metadata = sh_collection_metadatas[enrichment_id]
                else:
                    sh_cid = data_source.get("dataset_id")

                    # PLANETSCOPE doesn't have one so don't try to enrich it
                    if sh_cid is None:
                        continue

                    sh_metadatas = [m for _, m in sh_collection_metadatas.items() if m["datasource_type"] == sh_cid]

                    if len(sh_metadatas) == 0:
                        logger.warning(f"No STAC data available for collection with id {sh_cid}")
                        continue
                    elif len(sh_metadatas) > 1:
                        logger.warning(f"{len(sh_metadatas)} candidates for STAC data for collection with id {sh_cid}")
                        continue

                    sh_metadata = sh_metadatas[0]

                opensearch_metadata[cid] = sh_metadata
                if not data_source.get("endpoint"):
                    endpoint = opensearch_metadata[cid]["providers"][0]["url"]
                    endpoint = endpoint if endpoint.startswith("http") else "https://{}".format(endpoint)
                    data_source["endpoint"] = endpoint
                data_source["dataset_id"] = data_source.get("dataset_id") or opensearch_metadata[cid]["datasource_type"]

        if opensearch_metadata:
            metadata = dict_merge_recursive(opensearch_metadata, metadata, overwrite=True)

    metadata = _merge_layers_with_common_name(metadata)

    return metadata


def get_layer_catalog(
    vault: Vault = None,
    opensearch_enrich: Optional[bool] = None,
) -> GeoPySparkLayerCatalog:
    metadata = _get_layer_catalog(opensearch_enrich=opensearch_enrich)
    return GeoPySparkLayerCatalog(
        all_metadata=list(metadata.values()),
        vault=vault,
    )


def dump_layer_catalog():
    """CLI tool to dump layer catalog"""
    cli = argparse.ArgumentParser()
    cli.add_argument("--opensearch-enrich", action="store_true", help="Enable OpenSearch based enriching.")
    cli.add_argument(
        "--catalog-file", action="append", help="Path to catalog JSON file. Can be specified multiple times."
    )
    cli.add_argument(
        "--container",
        choices=["list", "dict"],
        default="list",
        help="Top level container to list the collections in: a list like in openEO API, or a dict, keyed on collection id.",
    )
    cli.add_argument("--verbose", action="store_true")
    arguments = cli.parse_args()

    logging.basicConfig(level=logging.DEBUG if arguments.verbose else logging.DEBUG)

    metadata = _get_layer_catalog(catalog_files=arguments.catalog_file, opensearch_enrich=arguments.opensearch_enrich)
    if arguments.container == "list":
        metadata = list(metadata.values())
    json.dump(metadata, fp=sys.stdout, indent=2)


def _merge_layers_with_common_name(metadata: CatalogDict):
    """Merge collections with same common name. Updates metadata dict in place."""
    common_names = set(m["common_name"] for m in metadata.values() if "common_name" in m)
    logger.info(f"Creating merged collections for common names: {common_names}")
    for common_name in common_names:
        merged = {
            "id": common_name,
            "_vito": {"data_source": {
                "type": "merged_by_common_name",
                "common_name": common_name,
                "merged_collections": [],
            }},
            "providers": [],
            "links": [],
            "extent": {"spatial": {"bbox": []}, "temporal": {"interval": []}},
        }

        merge_sources = [m for m in metadata.values() if m.get("common_name") == common_name]
        # Give priority to (reference/override) values in the "virtual:merge-by-common-name" placeholder entry
        merge_sources = sorted(
            merge_sources,
            key=(lambda m: deep_get(m, "_vito", "data_source", "type", default=None) == "virtual:merge-by-common-name"),
            reverse=True,
        )
        eo_bands = {}
        logger.info(f"Merging {common_name} from {[m['id'] for m in merge_sources]}")
        for to_merge in merge_sources:
            if not deep_get(to_merge, "_vito", "data_source", "type", default="").startswith("virtual:"):
                merged["_vito"]["data_source"]["merged_collections"].append(to_merge["id"])
            # Fill some fields with first hit
            for field in ["title", "description", "keywords", "version", "license", "cube:dimensions", "summaries"]:
                if field not in merged and field in to_merge:
                    merged[field] = deepcopy(to_merge[field])
            # Fields to take union
            for field in ["providers", "links"]:
                if isinstance(to_merge.get(field), list):
                    merged[field] += deepcopy(to_merge[field])

            # Take union of bands
            for band_dim in [k for k, v in to_merge.get("cube:dimensions", {}).items() if v["type"] == "bands"]:
                if band_dim not in merged["cube:dimensions"]:
                    merged["cube:dimensions"][band_dim] = deepcopy(to_merge["cube:dimensions"][band_dim])
                else:
                    for b in to_merge["cube:dimensions"][band_dim]["values"]:
                        if b not in merged["cube:dimensions"][band_dim]["values"]:
                            merged["cube:dimensions"][band_dim]["values"].append(b)
            for b in deep_get(to_merge, "summaries", "eo:bands", default=[]):
                band_name = b["name"]
                if band_name not in eo_bands:
                    eo_bands[band_name] = b
                else:
                    # Merge some things
                    aliases = set(eo_bands[band_name].get("aliases", [])) | set(b.get("aliases", []))
                    if aliases:
                        eo_bands[band_name]["aliases"] = list(aliases)

            # Union of extents
            # TODO: make sure first bbox/interval is overall extent
            merged["extent"]["spatial"]["bbox"].extend(deep_get(to_merge, "extent", "spatial", "bbox", default=[]))
            merged["extent"]["temporal"]["interval"].extend(
                deep_get(to_merge, "extent", "temporal", "interval", default=[])
            )

        # Adapt band order under `eo:bands`, based on `cube:dimensions`
        band_dims = [k for k, v in merged.get("cube:dimensions", {}).items() if v["type"] == "bands"]
        if band_dims:
            (band_dim,) = band_dims
            merged["summaries"]["eo:bands"] = [eo_bands[b] for b in merged["cube:dimensions"][band_dim]["values"]]

        metadata[common_name] = merged

    return metadata


def query_jvm_opensearch_client(open_search_client, collection_id, _query_kwargs, processing_level=""):
    jvm = get_jvm()
    fromDate = jvm.java.time.ZonedDateTime.parse(str(_query_kwargs["start_date"]).replace(" ", "T") + "Z")
    toDate = jvm.java.time.ZonedDateTime.parse(str(_query_kwargs["end_date"]).replace(" ", "T") + "Z")
    dateRange = jvm.scala.Some(jvm.scala.Tuple2(fromDate, toDate))
    extent = jvm.geotrellis.vector.Extent(float(_query_kwargs["ulx"]), float(_query_kwargs["bry"]),
                                          float(_query_kwargs["brx"]), float(_query_kwargs["uly"]))
    crs = jvm.geotrellis.proj4.CRS.fromEpsgCode(4326)
    bbox = jvm.geotrellis.vector.ProjectedExtent(extent, crs)
    attribute_values_dict = {}
    if "cldPrcnt" in _query_kwargs:
        attribute_values_dict["eo:cloud_cover"] = _query_kwargs["cldPrcnt"]
    attributeV_values = jvm.PythonUtils.toScalaMap(attribute_values_dict)
    products = open_search_client.getProducts(
        collection_id, dateRange,
        bbox, attributeV_values, "", processing_level
    )
    return {
        (
            products.apply(i).tileID().getOrElse(None),
            products.apply(i).nominalDate().toLocalDate().toString().replace("-", "")
        )
        for i in range(products.length())
    }


def check_missing_products(
        collection_metadata: Union[GeopysparkCubeMetadata, dict],
        temporal_extent: Tuple[str, str], spatial_extent: dict,
        properties: Optional[dict] = None,
) -> Union[List[str], None]:
    """
    Query catalogs to figure out if the data provider/source does not fully cover the desired spatiotemporal extent.
    """
    if not isinstance(collection_metadata, GeopysparkCubeMetadata):
        collection_metadata = GeopysparkCubeMetadata(collection_metadata)
    check_data = collection_metadata.get("_vito", "data_source", "check_missing_products", default=None)

    if check_data:
        logger.info(f"Check missing products for {collection_metadata.get('id')} using {check_data}")
        temporal_extent = [dateutil.parser.parse(t) if t is not None else None for t in temporal_extent]

        if "crs" in spatial_extent and spatial_extent["crs"] != 4326 and spatial_extent["crs"] != "EPSG:4326":
            spatial_extent = reproject_bounding_box(spatial_extent,from_crs=spatial_extent["crs"],to_crs="EPSG:4326")
        # Merge given properties with global layer properties
        properties = {
            **collection_metadata.get("_vito", "properties", default={}),
            **(properties or {})
        }
        query_kwargs = {
            "start_date": dt.datetime.combine(temporal_extent[0], dt.time.min),
            "end_date": dt.datetime.combine(temporal_extent[1], dt.time.max) if temporal_extent[1] is not None else None,
            "ulx": spatial_extent["west"],
            "brx": spatial_extent["east"],
            "bry": spatial_extent["south"],
            "uly": spatial_extent["north"],
        }

        if "eo:cloud_cover" in properties:
            cloud_cover_condition = extract_literal_match(properties["eo:cloud_cover"])
            cloud_cover_op, = cloud_cover_condition.keys()
            if cloud_cover_op in {"lte", "eq"}:
                query_kwargs["cldPrcnt"] = cloud_cover_condition[cloud_cover_op]
            else:
                logger.error(f"Failed to handle cloud cover condition {properties['eo:cloud_cover']}")
                raise InternalException("Failed to handle cloud cover condition")

        method = check_data.get("method")
        if method == "creo":
            logger.warning("At the moment it is not supported to check missing products on creo.")
            # We could query with status="all" and see if some offline products survive the deduplication and log those.
            missing = []
        elif method == "terrascope":
            jvm = get_jvm()

            expected_tiles = query_jvm_opensearch_client(
                open_search_client=jvm.org.openeo.opensearch.backends.CreodiasClient.apply(),
                collection_id=check_data["creo_catalog"]["mission"],
                _query_kwargs=query_kwargs,
                processing_level=check_data["creo_catalog"]["level"],
            )

            logger.debug(f"Expected tiles ({len(expected_tiles)}): {str_truncate(repr(expected_tiles), 200)}")
            opensearch_collection_id = collection_metadata.get("_vito", "data_source", "opensearch_collection_id")

            url = jvm.java.net.URL("https://services.terrascope.be/catalogue")
            # Terrascope has a different calculation for cloudCover
            query_kwargs_no_cldPrcnt = query_kwargs.copy()
            if "cldPrcnt" in query_kwargs_no_cldPrcnt:
                del query_kwargs_no_cldPrcnt["cldPrcnt"]
            terrascope_tiles = query_jvm_opensearch_client(
                open_search_client=jvm.org.openeo.opensearch.OpenSearchClient.apply(url, False, ""),
                collection_id=opensearch_collection_id,
                _query_kwargs=query_kwargs_no_cldPrcnt,
            )

            logger.debug(f"Oscar tiles ({len(terrascope_tiles)}): {str_truncate(repr(terrascope_tiles), 200)}")
            return list(expected_tiles.difference(terrascope_tiles))
        else:
            logger.error(f"Invalid check_missing_products data {check_data}")
            raise InternalException("Invalid check_missing_products data")

        logger.info(
            f"check_missing_products ({method}) on {collection_metadata.get('id')} detected {len(missing)} missing products."
        )
        return missing


def extra_validation_load_collection(collection_id: str, load_params: LoadParameters, env: EvalEnv) -> Iterable[dict]:
    if "backend_implementation" not in env:
        yield {"code": "NoBackendImplementation", "message": "It seems like you are running in a test environment"}
        return
    catalog: GeoPySparkLayerCatalog = env.backend_implementation.catalog
    allow_check_missing_products = smart_bool(env.get("allow_check_missing_products", True))
    sync_job = smart_bool(env.get("sync_job", False))
    large_layer_threshold_in_pixels = int(
        float(env.get("large_layer_threshold_in_pixels", LARGE_LAYER_THRESHOLD_IN_PIXELS)))
    metadata_json = catalog.get_collection_metadata(collection_id=collection_id)
    metadata = GeopysparkCubeMetadata(metadata_json)
    temporal_extent = load_params.temporal_extent
    spatial_extent = load_params.spatial_extent
    if allow_check_missing_products and metadata.get("_vito", "data_source", "check_missing_products", default=None):
        properties = load_params.properties
        if temporal_extent is None:
            yield {"code": "UnlimitedExtent", "message": "No temporal extent given."}
        if spatial_extent is None:
            yield {"code": "UnlimitedExtent", "message": "No spatial extent given."}
        if temporal_extent is None or spatial_extent is None:
            return

        products = check_missing_products(
            collection_metadata=metadata,
            temporal_extent=temporal_extent,
            spatial_extent=spatial_extent,
            properties=properties,
        )
        if products:
            for p in products:
                yield {
                    "code": "MissingProduct",
                    "message": f"Tile {p!r} in collection {collection_id!r} is not available."
                }

    native_crs = metadata.get("cube:dimensions", "x", "reference_system", default="EPSG:4326")
    if isinstance(native_crs, dict):
        native_crs = native_crs.get("id", {}).get("code", None)
    if isinstance(native_crs, int):
        native_crs = f"EPSG:{native_crs}"
    if not isinstance(native_crs, str):
        yield {"code": "InvalidNativeCRS", "message": f"Invalid native CRS {native_crs!r} for "
                                                      f"collection {collection_id!r}"}
        return

    number_of_temporal_observations: int = catalog.estimate_number_of_temporal_observations(
        collection_id,
        load_params,
    )

    if spatial_extent and temporal_extent:
        band_names = load_params.bands
        if band_names:
            # Will convert aliases:
            band_names = metadata.filter_bands(band_names).band_names
        else:
            band_names = metadata.get("cube:dimensions", "bands", "values",
                                      default=["_at_least_assume_one_band_"])
        nr_bands = len(band_names)

        if collection_id == 'TestCollection-LonLat4x4':
            # This layer is always 4x4 pixels, adapt resolution accordingly
            bbox_width = abs(spatial_extent["east"] - spatial_extent["west"])
            bbox_height = abs(spatial_extent["north"] - spatial_extent["south"])
            cell_width_latlon = bbox_width / 4
            cell_height_latlon = bbox_height / 4
            cell_width, cell_height = reproject_cellsize(spatial_extent,
                                                         (cell_width_latlon, cell_height_latlon),
                                                         "EPSG:4326",
                                                         "Auto42001",
                                                         )
        else:
            # The largest GSD I encountered was 25km. Double it as very permissive guess:
            default_gsd = (50000, 50000)
            gsd_object = metadata.get_GSD_in_meters()
            if isinstance(gsd_object, dict):
                gsd_in_meter_list = list(map(lambda x: gsd_object.get(x), band_names))
                gsd_in_meter_list = list(filter(lambda x: x is not None, gsd_in_meter_list))
                if not gsd_in_meter_list:
                    gsd_in_meter_list = [default_gsd] * nr_bands
            elif isinstance(gsd_object, tuple):
                gsd_in_meter_list = [gsd_object] * nr_bands
            else:
                gsd_in_meter_list = [default_gsd] * nr_bands

            # We need to convert GSD to resolution in order to take an average:
            px_per_m2_average_band = sum(map(lambda x: 1 / (x[0] * x[1]), gsd_in_meter_list)) / len(gsd_in_meter_list)
            px_per_m_average_band = pow(px_per_m2_average_band, 0.5)
            m_per_px_average_band = 1 / px_per_m_average_band

            res = (m_per_px_average_band, m_per_px_average_band)
            # Auto42001 is in meter
            cell_width, cell_height = reproject_cellsize(spatial_extent, res, "Auto42001", native_crs)

        message = is_layer_too_large(
            load_params=load_params,
            number_of_temporal_observations=number_of_temporal_observations,
            nr_bands=nr_bands,
            cell_width=cell_width,
            cell_height=cell_height,
            native_crs=native_crs,
            threshold_pixels=large_layer_threshold_in_pixels,
            sync_job=sync_job,
        )
        if message:
            yield {
                "code": "ExtentTooLarge",
                "message": f"collection_id {collection_id!r}: {message}"
            }


def is_layer_too_large(
        load_params: LoadParameters,
        number_of_temporal_observations: int,
        nr_bands: int,
        cell_width: float,
        cell_height: float,
        native_crs: str,
        threshold_pixels: int = LARGE_LAYER_THRESHOLD_IN_PIXELS,
        sync_job: bool = False,
):
    """
    Estimates the number of pixels that will be required to load this layer
    and returns True if it exceeds the threshold.

    :param load_params: Requested load parameters.
    :param number_of_temporal_observations: Requested number of temporal observations.
    :param nr_bands: Requested number of bands.
    :param cell_width: Width of the cells/pixels.
    :param cell_height: Height of the cells/pixels.
    :param native_crs: Native CRS of the layer.
    :param threshold_pixels: Threshold in pixels.
    :param sync_job: Is sync job.

    :return: A message if the layer exceeds the threshold in pixels. None otherwise.
             Also returns the estimated number of pixels and the threshold.
    """
    geometries = load_params.aggregate_spatial_geometries
    spatial_extent = load_params.spatial_extent
    srs = spatial_extent.get("crs", 'EPSG:4326')
    if isinstance(srs, int):
        srs = 'EPSG:%s' % str(srs)
    elif isinstance(srs, dict):
        if srs["name"] == 'AUTO 42001 (Universal Transverse Mercator)':
            srs = 'Auto42001'

    # Resampling process overwrites native_crs and resolution from metadata.
    resample_target_crs = load_params.target_crs
    if resample_target_crs:
        native_crs = resample_target_crs
    resample_target_resolution = load_params.target_resolution
    if resample_target_resolution:
        resample_width, resample_height = resample_target_resolution
        if resample_width != 0 and resample_height != 0:
            # This can happen with e.g. resample_spatial(resolution=0, projection=4326)
            cell_width, cell_height = resample_width, resample_height

    # Reproject.
    if isinstance(native_crs, dict):
        native_crs = native_crs.get("id", {}).get("code", None)
    if native_crs is None:
        raise InternalException("No native CRS found during is_layer_too_large check.")
    if native_crs == "Auto42001":
        west, south = spatial_extent["west"], spatial_extent["south"]
        east, north = spatial_extent["east"], spatial_extent["north"]
        native_crs = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
    if srs != native_crs:
        spatial_extent = reproject_bounding_box(spatial_extent, from_crs=srs, to_crs=native_crs)

    bbox_width = abs(spatial_extent["east"] - spatial_extent["west"])
    bbox_height = abs(spatial_extent["north"] - spatial_extent["south"])

    pixels_width = bbox_width / cell_width
    pixels_height = bbox_height / cell_height
    if sync_job and (pixels_width > 20000 or pixels_height > 20000) and not geometries:
        return f"Requested spatial extent is too large for a sync job {pixels_width:.0f}x{pixels_height:.0f} pixels. Max size: (20000x20000)."

    estimated_pixels = (bbox_width * bbox_height) / (cell_width * cell_height) * number_of_temporal_observations * nr_bands
    logger.debug(
        f"is_layer_too_large {estimated_pixels=} {threshold_pixels=} ({bbox_width=} {bbox_height=} {cell_width=} {cell_height=} {number_of_temporal_observations=} {nr_bands=})"
    )
    if estimated_pixels > threshold_pixels:
        if geometries and not isinstance(geometries, dict):
            # Threshold is exceeded, but only the pixels in the geometries will be loaded if they are provided.
            # For performance, we estimate the area using a simple bounding box around each polygon.
            if isinstance(geometries, DriverVectorCube):
                geometries_area = calculate_rough_area([geometries.to_multipolygon()])
            elif isinstance(geometries, DelayedVector):
                geometries_area = calculate_rough_area(geometries.geometries)
            elif isinstance(geometries, BaseGeometry):
                geometries_area = calculate_rough_area([geometries])
            else:
                raise TypeError(f'Unsupported geometry type: {type(geometries)}')
            if native_crs != 'EPSG:4326':
                # Geojson is always in 4326. Reproject the cell bbox from native to 4326 so we can calculate the area.
                cell_bbox = {"west": 0, "east": cell_width, "south": 0, "north": cell_height, "crs": native_crs}
                cell_bbox = reproject_bounding_box(cell_bbox, from_crs=native_crs, to_crs='EPSG:4326')
                cell_width = abs(cell_bbox["east"] - cell_bbox["west"])
                cell_height = abs(cell_bbox["north"] - cell_bbox["south"])
            surface_area_pixels = geometries_area / (cell_width * cell_height)
            estimated_pixels = surface_area_pixels * number_of_temporal_observations * nr_bands
            logger.debug(
                f"is_layer_too_large {estimated_pixels=} {threshold_pixels=} ({geometries_area=} {cell_width=} {cell_height=} {number_of_temporal_observations=} {nr_bands=})"
            )
            if estimated_pixels <= threshold_pixels:
                return None
        return f"Requested extent is too large to process. Estimated number of pixels: {estimated_pixels:.2e}, " + \
            f"threshold: {threshold_pixels:.2e}."
    return None


if __name__ == "__main__":
    dump_layer_catalog()
