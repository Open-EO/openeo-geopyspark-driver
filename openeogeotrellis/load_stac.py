from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pystac
import pystac.stac_io
from geopyspark import LayerType
from openeo.metadata import _StacMetadataParser
from openeo.util import TimingLogger
from openeo_driver.backend import LoadParameters
import openeo_driver.backend
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.errors import (
    OpenEOApiException,
    ProcessParameterInvalidException,
)
from openeo_driver.users import User
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv

from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata
from openeogeotrellis.utils import get_jvm, map_optional, normalize_temporal_extent, to_projected_polygons

# STAC source resolution / item collection construction
from openeogeotrellis.stac.extents import (
    SpatialFilteringGeometries,
    SpatioTemporalExtent,
    TemporalExtent,
    spatiotemporal_extent_from_load_params,
)
from openeogeotrellis.stac.exceptions import LoadStacException
from openeogeotrellis.stac.item_collection import construct_item_collection
from openeogeotrellis.stac.property_filter import PropertyFilterPGMap

# Per-item/per-asset analysis (projection metadata, band/pixel decisions)
from openeogeotrellis.stac.asset_table import (
    AssetTable,
    get_pixel_value_scaling_mode,
    build_asset_table,
)

# Target grid (EPSG/cellsize) selection
from openeogeotrellis.stac.target_grid import TargetGrid, select_target_grid

# GeoPySpark/JVM-specific pyramid factory / opensearch feature construction
from openeogeotrellis.stac.geopyspark_features import build_opensearch_features
from openeogeotrellis.stac.pyramid_factory import build_pyramid_factory

if TYPE_CHECKING:

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

logger = logging.getLogger(__name__)


class NoDataAvailableException(OpenEOApiException):
    status_code = 400
    code = "NoDataAvailable"
    message = "There is no data available for the given extents."


@dataclass
class _LoadStacContext:
    """Context object containing all inputs needed to build a datacube."""
    pyramid_factory: Any
    projected_polygons: Any
    from_date: dt.datetime
    to_date: dt.datetime
    metadata_properties: Dict[str, Any]
    correlation_id: str
    data_cube_parameters: Any
    opensearch_client: Any
    single_level: bool
    items_found: bool
    allow_empty_cubes: bool
    extent: Any
    extent_crs: Any
    netcdf_with_time_dimension: bool
    requested_bbox: Optional[BoundingBox]
    metadata: GeopysparkCubeMetadata
    spatiotemporal_extent: SpatioTemporalExtent
    target_grid: TargetGrid
    url: str
    jvm: Any


def get_stac_item_collection_filename(*, pg_node_id: str) -> str:
    return f"stac-item-collection-{pg_node_id}.json"


def _prepare_context(
    url: str,
    *,
    load_params: LoadParameters,
    env: EvalEnv,
    layer_properties: Optional[PropertyFilterPGMap] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    normalized_band_selection: Optional[List[str]] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    feature_flags: Optional[Dict[str, Any]] = None,
    data_cube_parameters: Optional[Any] = None,
    serialize_item_collection: bool = True,
    pg_node_id: Optional[str] = None,
) -> _LoadStacContext:
    """
    Prepare all metadata and inputs needed to build/load a datacube from raster files.

    :param normalized_band_selection: (Optional) list of normalized band names
        of the bands selected by the user or, as fallback, defined in the openEO collection metadata.
        Intended for openEO `load_collection` use cases, where:
        - openEO collection metadata only exposes a subset of all bands in the STAC collection
        - user is allowed to use band name aliases/variants defined in openEO collection metadata,
          like "eo:common_name" (from STAC EO extension)
          and "aliases" (non-standardized openeo-geopyspark-driver feature).
          `normalized_band_selection` must contain the standard band names after resolving these aliases.
    """
    from openeogeotrellis import datacube_parameters

    # Feature flags: merge global (e.g. from layer catalog info) and user-provided (higher precedence)
    feature_flags = {**(feature_flags or {}), **load_params.get("featureflags", {})}

    logger.info(f"load_stac with {url=} {load_params=} {feature_flags=}")

    # Collect some  feature flags
    allow_empty_cubes = feature_flags.get("allow_empty_cube", env.get(EVAL_ENV_KEY.ALLOW_EMPTY_CUBES, False))
    pixel_value_scaling_mode = get_pixel_value_scaling_mode(feature_flags=feature_flags, url=url)

    if use_raw_asset_href := feature_flags.get("use_raw_asset_href", False):
        logger.warning(f"Usage of feature flag {use_raw_asset_href=}, which is not recommended")

    # Merge property filters from layer catalog and user-provided load_params (with precedence to load_params)
    property_filter_pg_map: PropertyFilterPGMap = {
        **(layer_properties or {}),
        **(load_params.properties or {}),
    }

    user: Optional[User] = env.get("user")

    requested_bbox = BoundingBox.from_dict_or_none(load_params.spatial_extent, default_crs="EPSG:4326")
    requested_temporal_extent = TemporalExtent.from_load_param_extent(load_params.temporal_extent)

    # TODO normalize_temporal_extent replaces 'None' with "2000-01-01", which is not a good fallback date.
    from_date, until_date = map(dt.datetime.fromisoformat, normalize_temporal_extent(load_params.temporal_extent))
    to_date = (
        dt.datetime.combine(until_date, dt.time.max, until_date.tzinfo)
        if from_date == until_date
        else until_date - dt.timedelta(milliseconds=1)
    )
    spatiotemporal_extent = spatiotemporal_extent_from_load_params(
        spatial_extent=load_params.spatial_extent,
        temporal_extent=load_params.temporal_extent,
    )
    spatial_filtering_geometries = SpatialFilteringGeometries(geometries=load_params.aggregate_spatial_geometries)

    # Band selection: subset of bands to load from STAC assets.
    # Prefer `normalized_band_selection` (if available) over raw `load_params.bands`
    # as the former is result of resolving aliases/common_name to standard/expected band names.
    band_selection: Union[List[str], None] = normalized_band_selection or load_params.bands
    logger.debug(f"{band_selection=} (from {normalized_band_selection=} and {load_params.bands=})")

    collected_link_band_names = set()

    try:
        # `available_band_names`: all bands that were detected in STAC metadata,
        #       mainly to be used as fallback band listing when no user-specified band selection was made,
        #       and bit of validation too where appropriate.
        with TimingLogger(title=f"construct_item_collection({url=})", logger=logger.info):
            stac_source = construct_item_collection(
                url=url,
                spatiotemporal_extent=spatiotemporal_extent,
                property_filter_pg_map=property_filter_pg_map,
                batch_jobs=batch_jobs,
                env=env,
                feature_flags=feature_flags,
                stac_io=stac_io,
                user=user,
                spatial_filtering_geometries=spatial_filtering_geometries,
            )
            item_collection = stac_source.item_collection
            collection_summary = stac_source.collection_summary
            available_band_names = stac_source.band_names
            netcdf_with_time_dimension = stac_source.netcdf_with_time_dimension

        items_found = len(item_collection.items) > 0
        if not allow_empty_cubes and not items_found:
            raise NoDataAvailableException(message=f"No data available with load_stac of {url=} ({pg_node_id})")

        if serialize_item_collection and pg_node_id and (job_dir := env.get(EVAL_ENV_KEY.JOB_DIR)):
            item_collection_path = Path(job_dir) / get_stac_item_collection_filename(pg_node_id=pg_node_id)
            item_collection.to_file(path=item_collection_path)

        asset_table: AssetTable = build_asset_table(
            item_collection=item_collection,
            band_selection=band_selection,
            available_band_names=available_band_names,
            spatiotemporal_extent=spatiotemporal_extent,
            pixel_value_scaling_mode=pixel_value_scaling_mode,
            use_raw_asset_href=use_raw_asset_href,
            feature_flags=feature_flags,
        )
        resolution_tracker = asset_table.resolution_tracker
        observed_epsgs = asset_table.observed_epsgs
        stac_bbox = asset_table.stac_bbox
        asset_band_names = asset_table.asset_band_names
        collected_link_band_names = asset_table.collected_link_band_names

        jvm = get_jvm()
        opensearch_client = build_opensearch_features(asset_table=asset_table, jvm=jvm)
    except OpenEOApiException:
        raise
    except Exception as e:
        raise LoadStacException(url=url, info=repr(e)) from e

    target_bbox = requested_bbox or stac_bbox

    if not target_bbox:
        raise ProcessParameterInvalidException(
            process="load_stac",
            parameter="spatial_extent",
            reason=f"Unable to derive a spatial extent from provided STAC metadata: {url}, "
            f"please provide a spatial extent.",
        )

    # Adapt the plain `collection_summary` dict returned by `construct_item_collection`
    # into a `GeopysparkCubeMetadata`.
    metadata = GeopysparkCubeMetadata(metadata=collection_summary)

    if "x" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="x", extent=[])
    if "y" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="y", extent=[])

    item_collection_temporal_extent = item_collection.get_temporal_extent()
    metadata_temporal_extent = (
        map_optional(dt.datetime.isoformat, requested_temporal_extent.from_date or item_collection_temporal_extent[0]),
        map_optional(dt.datetime.isoformat, requested_temporal_extent.to_date or item_collection_temporal_extent[1]),
    )
    logger.info(f"_prepare_context {url=} {metadata_temporal_extent=}")
    metadata = metadata.with_temporal_extent(
        temporal_extent=metadata_temporal_extent,
        allow_adding_dimension=True,
    )

    fallback_band_names: List[str] = available_band_names.copy()
    if extra_fallback := sorted(b for b in collected_link_band_names if b not in available_band_names):
        # TODO: possible to eliminate need for this?
        logger.debug(f"Adding {extra_fallback=} to {available_band_names=}")
        fallback_band_names = available_band_names + extra_fallback
    # Source band names: normalized/standardized band names to be included in the cube,
    #   with naming as defined in openEO collection metadata or extracted from STAC metadata
    source_band_names: List[str] = normalized_band_selection or load_params.bands or fallback_band_names
    # Target band names: possibly contains user-picked aliases, expected as band names in resulting cube
    target_band_names: List[str] = load_params.bands or normalized_band_selection or fallback_band_names
    logger.debug(
        f"{source_band_names=} {target_band_names=} from {load_params.bands=} {normalized_band_selection=} {fallback_band_names=}"
    )
    if not target_band_names:
        raise OpenEOApiException(
            status_code=400,
            code="UndefinedBandSelection",
            message="Unable to determine bands in load_stac. Consider specifying bands explicitly.",
        )
    metadata = metadata.with_new_band_names(target_band_names)

    # TODO: calling this "requested" is misleading, as requested "bands" might be empty, while this variable is non-empty.
    #       Just reuse "target_band_names" here directly?
    requested_band_names = metadata.band_names

    target_grid = select_target_grid(
        resolution_tracker=resolution_tracker,
        observed_epsgs=observed_epsgs,
        source_band_names=source_band_names,
        target_bbox=target_bbox,
        feature_flags=feature_flags,
        load_params=load_params,
    )

    pyramid_factory = build_pyramid_factory(
        netcdf_with_time_dimension=netcdf_with_time_dimension,
        opensearch_client=opensearch_client,
        opensearch_link_titles_map=asset_table.opensearch_link_titles_map,
        source_band_names=source_band_names,
        requested_band_names=requested_band_names,
        asset_band_names=asset_band_names,
        cell_width=target_grid.cell_width,
        cell_height=target_grid.cell_height,
        url=url,
        env=env,
        jvm=jvm,
    )

    extent = jvm.geotrellis.vector.Extent(*map(float, target_bbox.as_wsen_tuple()))
    extent_crs = target_bbox.crs

    geometries = load_params.aggregate_spatial_geometries
    if isinstance(geometries, DriverVectorCube) and geometries.geometry_count() == 0:
        geometries = None

    if not geometries:
        projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, extent_crs)
    else:
        projected_polygons = to_projected_polygons(jvm, geometries, crs=extent_crs, buffer_points=True)

    projected_polygons = getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$").reproject(
        projected_polygons, target_grid.epsg
    )

    metadata_properties = {}
    correlation_id = env.get(EVAL_ENV_KEY.CORRELATION_ID, "")

    if data_cube_parameters is not None:
        single_level = env.get(EVAL_ENV_KEY.PYRAMID_LEVELS, "all") != "all"
    else:
        data_cube_parameters, single_level = datacube_parameters.create(load_params=load_params, env=env, jvm=jvm)
    getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

    tilesize = feature_flags.get("tilesize", None)
    if tilesize:
        getattr(data_cube_parameters, "tileSize_$eq")(tilesize)

    return _LoadStacContext(
        pyramid_factory=pyramid_factory,
        projected_polygons=projected_polygons,
        from_date=from_date,
        to_date=to_date,
        metadata_properties=metadata_properties,
        correlation_id=correlation_id,
        data_cube_parameters=data_cube_parameters,
        opensearch_client=opensearch_client,
        single_level=single_level,
        items_found=items_found,
        allow_empty_cubes=allow_empty_cubes,
        extent=extent,
        extent_crs=extent_crs,
        netcdf_with_time_dimension=netcdf_with_time_dimension,
        requested_bbox=requested_bbox,
        metadata=metadata,
        spatiotemporal_extent=spatiotemporal_extent,
        target_grid=target_grid,
        url=url,
        jvm=jvm,
    )


def _build_datacube(context: _LoadStacContext) -> "GeopysparkDataCube":
    """
    Build the raster pyramid using (heavy) raster loading operations.
    This function performs the actual calls to the PyramidFactory to load raster files.
    """
    from geopyspark import Pyramid, TiledRasterLayer

    from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

    # Unpack context
    pyramid_factory = context.pyramid_factory
    projected_polygons = context.projected_polygons
    from_date = context.from_date
    to_date = context.to_date
    metadata_properties = context.metadata_properties
    correlation_id = context.correlation_id
    data_cube_parameters = context.data_cube_parameters
    opensearch_client = context.opensearch_client
    single_level = context.single_level
    items_found = context.items_found
    allow_empty_cubes = context.allow_empty_cubes
    extent = context.extent
    extent_crs = context.extent_crs
    netcdf_with_time_dimension = context.netcdf_with_time_dimension
    requested_bbox = context.requested_bbox
    metadata = context.metadata
    spatiotemporal_extent = context.spatiotemporal_extent
    url = context.url
    jvm = context.jvm

    try:
        if netcdf_with_time_dimension:
            logger.info("_build_datacube: calling NetCDFCollection.datacube_seq")
            pyramid = pyramid_factory.datacube_seq(
                projected_polygons,
                from_date.isoformat(),
                to_date.isoformat(),
                metadata_properties,
                correlation_id,
                data_cube_parameters,
                opensearch_client,
            )
        elif single_level:
            if not items_found and allow_empty_cubes:
                logger.info("_build_datacube: calling PyramidFactory.empty_datacube_seq")
                pyramid = pyramid_factory.empty_datacube_seq(
                    projected_polygons,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    data_cube_parameters,
                )
            else:
                logger.info("_build_datacube: calling PyramidFactory.datacube_seq")
                pyramid = pyramid_factory.datacube_seq(
                    projected_polygons,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    metadata_properties,
                    correlation_id,
                    data_cube_parameters,
                )
        else:
            if requested_bbox:
                extent = jvm.geotrellis.vector.Extent(*map(float, requested_bbox.as_wsen_tuple()))
                extent_crs = requested_bbox.crs
            else:
                extent = jvm.geotrellis.vector.Extent(-180.0, -90.0, 180.0, 90.0)
                extent_crs = "EPSG:4326"

            if not items_found and allow_empty_cubes:
                logger.info("_build_datacube: calling PyramidFactory.empty_pyramid_seq")
                pyramid = pyramid_factory.empty_pyramid_seq(
                    extent, extent_crs, from_date.isoformat(), to_date.isoformat()
                )
            else:
                logger.info("_build_datacube: calling PyramidFactory.pyramid_seq")
                pyramid = pyramid_factory.pyramid_seq(
                    extent, extent_crs, from_date.isoformat(), to_date.isoformat(), metadata_properties, correlation_id
                )
        logger.info(f"_build_datacube: done building pyramid")
    except Exception as e:
        raise OpenEOApiException(
            message=f"load_stac: Error when constructing datacube from {url}: {e}",
            status_code=500,
        ) from e

    if not spatiotemporal_extent.temporal_extent.is_unbounded():
        metadata = metadata.filter_temporal(*spatiotemporal_extent.temporal_extent.isoformat())

    metadata = metadata.filter_bbox(
        west=extent.xmin(),
        south=extent.ymin(),
        east=extent.xmax(),
        north=extent.ymax(),
        crs=extent_crs,
    )

    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    option = jvm.scala.Option

    # noinspection PyProtectedMember
    levels = {
        pyramid.apply(index)._1(): TiledRasterLayer(
            LayerType.SPACETIME,
            temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2()),
        )
        for index in range(0, pyramid.size())
    }

    return GeopysparkDataCube(pyramid=Pyramid(levels), metadata=metadata)


def load_stac(
    url: str,
    *,
    load_params: LoadParameters,
    env: EvalEnv,
    layer_properties: Optional[PropertyFilterPGMap] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    normalized_band_selection: Optional[List[str]] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    feature_flags: Optional[Dict[str, Any]] = None,
    data_cube_parameters: Optional[Any] = None,
    pg_node_id: Optional[str] = None,
) -> "GeopysparkDataCube":
    """

    :param normalized_band_selection: (Optional) list of normalized band names
        of the bands selected by the user or, as fallback, defined in the openEO collection metadata.
        Intended for openEO `load_collection` use cases, where:
        - openEO collection metadata only exposes a subset of all bands in the STAC collection
        - user is allowed to use band name aliases/variants defined in openEO collection metadata,
          like "eo:common_name" (from STAC EO extension)
          and "aliases" (non-standardized openeo-geopyspark-driver feature).
          `normalized_band_selection` must contain the standard band names after resolving these aliases.
    """
    context = _prepare_context(
        url=url,
        load_params=load_params,
        env=env,
        layer_properties=layer_properties,
        batch_jobs=batch_jobs,
        normalized_band_selection=normalized_band_selection,
        stac_io=stac_io,
        feature_flags=feature_flags,
        data_cube_parameters=data_cube_parameters,
        pg_node_id=pg_node_id,
    )
    return _build_datacube(context)
