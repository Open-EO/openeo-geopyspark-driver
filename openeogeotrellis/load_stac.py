from __future__ import annotations

import datetime
import datetime as dt
import functools
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Sequence
from urllib.parse import urlparse

from deprecated import deprecated
import geopyspark as gps
import pyproj
import pystac
import pystac.stac_io
import pystac.utils
import shapely.geometry
from geopyspark import LayerType, TiledRasterLayer

import openeo_driver.backend
from openeo.metadata import _StacMetadataParser
from openeo_driver.backend import LoadParameters
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.errors import (
    OpenEOApiException,
    ProcessParameterInvalidException,
    ProcessParameterRequiredException,
)
from openeo_driver.users import User
from openeo_driver.util.geometry import BoundingBox, GeometryBufferer
from openeo_driver.util.utm import utm_zone_from_epsg
from openeo_driver.utils import EvalEnv
from openeogeotrellis import datacube_parameters
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.stac.exceptions import NoDataAvailableException, LoadStacException
from openeogeotrellis.stac.fixed_features_open_search_client import (
    FixedFeaturesOpenSearchClient,
    JvmFixedFeaturesOpenSearchClient,
)
from openeogeotrellis.stac.item_collection import PropertyFilterPGMap, construct_item_collection, ItemCollection
from openeogeotrellis.util.datetime import DateTimeLikeOrNone, to_datetime_utc_unless_none
from openeogeotrellis.util.geometry import GridSnapper
from openeogeotrellis.utils import get_jvm, to_projected_polygons, unzip

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StacLoadParameters:
    spatial_extent: Optional[Dict[str, Any]]
    temporal_extent: Tuple[Union[str, None], Union[str, None]]
    bands: Optional[Union[List[str], str]]
    properties: Optional[Dict[str, Any]]
    target_crs: Optional[Union[int, Dict[str, Any]]]
    target_resolution: Optional[Tuple[float, float]]
    global_extent: Optional[Dict[str, Any]]
    featureflags: StacFeatureFlags

    @staticmethod
    def from_load_parameters(
        load_params: LoadParameters,
    ) -> StacLoadParameters:
        return StacLoadParameters(
            spatial_extent=load_params.spatial_extent,
            temporal_extent=load_params.temporal_extent,
            bands=load_params.bands,
            properties=load_params.properties,
            target_crs=load_params.target_crs,
            target_resolution=load_params.target_resolution,
            global_extent=load_params.global_extent,
            featureflags=load_params.get("featureflags", StacFeatureFlags()),
        )


@dataclass
class StacFeatureFlags:
    allow_empty_cube: bool = False
    cellsize: Optional[Tuple[float, float]] = None
    tilesize: Optional[int] = None
    use_filter_extension: bool = True
    deduplicate_items: bool = get_backend_config().load_stac_deduplicate_items_default

    @staticmethod
    def from_dict(flags: Dict[str, Any]) -> StacFeatureFlags:
        return StacFeatureFlags(
            allow_empty_cube=flags.get("allow_empty_cube", False),
            cellsize=flags.get("cellsize"),
            tilesize=flags.get("tilesize"),
            use_filter_extension=flags.get("use-filter-extension", True),
            deduplicate_items=flags.get("deduplicate_items", get_backend_config().load_stac_deduplicate_items_default),
        )

    def update(self, other: Dict[str, Any]) -> None:
        # Update fields from a dict, ignoring unknown keys
        for key, value in other.items():
            if hasattr(self, key):
                setattr(self, key, value)


@dataclass
class StacEvalEnv:
    correlation_id: Optional[str] = None
    user: Optional[User] = None
    collected_parameters: Dict[str, Any] = None
    allow_empty_cubes: bool = False
    max_soft_errors_ratio: float = 0.0

    @staticmethod
    def from_eval_env(env: EvalEnv) -> StacEvalEnv:
        return StacEvalEnv(
            correlation_id=env.get(EVAL_ENV_KEY.CORRELATION_ID),
            allow_empty_cubes=env.get(EVAL_ENV_KEY.ALLOW_EMPTY_CUBES, False),
            max_soft_errors_ratio=env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0),
            user=env.get(EVAL_ENV_KEY.USER),
            collected_parameters=env.collect_parameters(),
        )

    def collect_parameters(self):
        return self.collected_parameters or {}


def load_stac_from_layercatalog(
        url: str,
        load_params: StacLoadParameters,
        env: StacEvalEnv,
        batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
        override_band_names: Optional[List[str]] = None,
):
    # User-provided (load_params) featureflags have precedence and will override layer catalog flags.
    # TODO: layercatalog has to fill in the correct featureflags in load_params beforehand.
    # TODO: layercatalog should also fill in load_params.properties based on its own property filters.
    # Merge property filters from layer catalog and user-provided load_params (with precedence to load_params)
    # property_filter_pg_map: PropertyFilterPGMap = {
    #     **(layer_properties or {}),
    #     **(load_params.properties or {}),
    # }
    # load_params.properties = property_filter_pg_map
    return load_stac(
        url=url,
        load_params=load_params,
        env=env,
        batch_jobs=batch_jobs,
        override_band_names=override_band_names,
    )


def load_stac_from_process(
        url: str,
        *,
        load_params: StacLoadParameters,
        env: StacEvalEnv,
        batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    ):
    return load_stac(
        url=url,
        load_params=load_params,
        env=env,
        batch_jobs=batch_jobs,
    )


@dataclass
class FeatureBuildResult:
    """Result of building features from STAC item collection (JVM-free, testable)."""
    collection_band_names: List[str]
    band_cell_size: Dict[str, Tuple[float, float]]
    band_epsgs: Dict[str, Set[int]]
    stac_bbox: Optional[BoundingBox]
    # Last seen values from iteration (ill-defined, TODO: improve)
    asset_band_names: Optional[List[str]]
    proj_epsg: Optional[int]
    proj_bbox: Optional[Tuple[float, float, float, float]]
    proj_shape: Optional[Tuple[int, int]]


@dataclass
class TargetProjection:
    """Target projection parameters."""
    target_epsg: int
    cell_width: float
    cell_height: float
    target_bbox: BoundingBox


def build_opensearch_features(
    item_collection: ItemCollection,
    opensearch_client: FixedFeaturesOpenSearchClient,
    *,
    load_params: StacLoadParameters,
    collection_band_names: List[str],
) -> FeatureBuildResult:
    """
    Populate opensearch client with features from STAC items.

    Args:
        item_collection: Collection of STAC items to process
        opensearch_client: Client to populate (can be in-memory or JVM-backed)
        load_params: Load parameters for filtering
        collection_band_names: List of collection band names (modified in-place)

    Returns:
        FeatureBuildResult with metadata collected during feature building
    """
    feature_flags = load_params.featureflags


    # TODO: code smell: (most of) these vars should not be initialized with None here
    # asset_band_names = the full list of band names contained by the asset
    # in the same order as defined in the asset (e.g. NetCDF file) itself.
    # Note that this list is not yet filtered by the requested bands, as the asset loader needs to know
    # which band index in the file to read.
    asset_band_names = None
    stac_bbox = None
    proj_epsg = None
    proj_bbox = None
    proj_shape = None

    stac_metadata_parser = _StacMetadataParser(logger=logger)

    # The minimum cell size per band name across all assets
    band_cell_size: Dict[str, Tuple[float, float]] = {}
    band_epsgs: Dict[str, Set[int]] = {}

    for itm, band_assets in item_collection.iter_items_with_band_assets():
        builder = (
            opensearch_client.feature_builder()
            .withId(itm.id)
            .withNominalDate(itm.properties.get("datetime") or itm.properties["start_datetime"])
        )

        band_names_tracker = NoveltyTracker()
        for asset_id, asset in sorted(
                # Go through assets ordered by asset GSD (from finer to coarser) if possible,
                # falling back on deterministic alphabetical asset_id order.
                # see https://github.com/Open-EO/openeo-geopyspark-driver/pull/1213#discussion_r2107353442
                # TODO: move this sorting feature inside iter_items_with_band_assets
            band_assets.items(),
            key=lambda kv: (
                float(kv[1].extra_fields.get("gsd") or itm.properties.get("gsd") or 40e6),
                kv[0],
            ),
        ):
            proj_epsg, proj_bbox, proj_shape = _get_proj_metadata(asset=asset, item=itm)

            asset_band_names_from_metadata: List[str] = stac_metadata_parser.bands_from_stac_asset(asset=asset).band_names()
            if not asset_band_names_from_metadata:
                asset_band_names_from_metadata = feature_flags.get("asset_id_to_bands_map", {}).get(asset_id, [])
                logger.debug(f"using `asset_id_to_bands_map`: mapping {asset_id} to {asset_band_names_from_metadata}")
            logger.debug(f"from intersecting_items: {itm.id=} {asset_id=} {asset_band_names_from_metadata=}")

            if not load_params.bands:
                asset_band_names = asset_band_names_from_metadata or [asset_id]
            elif isinstance(load_params.bands, list) and asset_id in load_params.bands:
                # No user-specified band filtering: follow band names from metadata (if possible)
                if asset_id not in collection_band_names:
                    logger.warning(f"Using asset key {asset_id!r} as band name.")
                asset_band_names = [asset_id]
            elif set(asset_band_names_from_metadata).intersection(load_params.bands or []):
                # User-specified bands match with band names in metadata
                asset_band_names = asset_band_names_from_metadata
            else:
                # No match with load_params.bands in some way -> skip this asset
                continue

            if band_names_tracker.already_seen(sorted(asset_band_names)):
                # We've already seen this set of bands (e.g. at finer GSD), so skip this asset.
                continue

            for asset_band_name in asset_band_names:
                if asset_band_name not in collection_band_names:
                    collection_band_names.append(asset_band_name)

                if proj_bbox and proj_shape:
                    asset_cell_size = _compute_cellsize(proj_bbox, proj_shape)
                    band_cell_width, band_cell_height = band_cell_size.get(asset_band_name, (float("inf"), float("inf")))
                    band_cell_size[asset_band_name] = (
                        min(band_cell_width, asset_cell_size[0]),
                        min(band_cell_height, asset_cell_size[1]),
                    )
                if proj_epsg:
                    # TODO: risk on overwriting/conflict
                    band_epsgs.setdefault(asset_band_name, set()).add(proj_epsg)

            pixel_value_offset = _get_pixel_value_offset(item=itm, asset=asset)
            logger.debug(
                f"FeatureBuilder.addlink {itm.id=} {asset_id=} {asset_band_names_from_metadata=} {asset_band_names=}"
            )
            builder = builder.addLink(get_best_url(asset), asset_id, pixel_value_offset, asset_band_names)

        # TODO: the proj_* values are assigned in inner per-asset loop,
        #       so the values here are ill-defined (the values might even come from another item)
        if proj_epsg:
            builder = builder.withCRS(f"EPSG:{proj_epsg}")
        if proj_bbox:
            builder = builder.withRasterExtent(*(float(b) for b in proj_bbox))

        if proj_bbox and proj_shape:
            cell_width, cell_height = _compute_cellsize(proj_bbox, proj_shape)
            builder = builder.withResolution(cell_width)

        latlon_bbox = BoundingBox.from_wsen_tuple(itm.bbox, 4326) if itm.bbox else None
        item_bbox = latlon_bbox
        if proj_bbox is not None and proj_epsg is not None:
            item_bbox = BoundingBox.from_wsen_tuple(proj_bbox, crs=proj_epsg)
            latlon_bbox = item_bbox.reproject(4326)

        if latlon_bbox is not None:
            builder = builder.withBBox(*map(float, latlon_bbox.as_wsen_tuple()))

        if itm.geometry is not None:
            builder = builder.withGeometryFromWkt(str(shapely.geometry.shape(itm.geometry)))

        self_links = itm.get_links(rel="self")
        self_url = self_links[0].href if self_links else None

        if self_url:
            builder = builder.withSelfUrl(self_url)

        opensearch_client.add_feature(builder.build())

        stac_bbox = (
            item_bbox
            if stac_bbox is None
            else BoundingBox.from_wsen_tuple(
                item_bbox.as_polygon().union(stac_bbox.as_polygon()).bounds, stac_bbox.crs
            )
        )

    return FeatureBuildResult(
        collection_band_names=collection_band_names,
        band_cell_size=band_cell_size,
        band_epsgs=band_epsgs,
        stac_bbox=stac_bbox,
        asset_band_names=asset_band_names,
        proj_epsg=proj_epsg,
        proj_bbox=proj_bbox,
        proj_shape=proj_shape,
    )


def calculate_target_projection(
    *,
    requested_bbox: Optional[BoundingBox],
    stac_bbox: Optional[BoundingBox],
    band_epsgs: Dict[str, Set[int]],
    band_cell_size: Dict[str, Tuple[float, float]],
    requested_band_names: List[str],
    load_params: StacLoadParameters,
    feature_flags: StacFeatureFlags,
    proj_epsg: Optional[int],
    url: str,
) -> TargetProjection:
    """
    Calculate target projection parameters (pure Python, no JVM).

    Args:
        requested_bbox: User-requested bounding box (if any)
        stac_bbox: Bounding box derived from STAC items
        band_epsgs: EPSG codes per band
        band_cell_size: Cell sizes per band
        requested_band_names: List of requested band names
        load_params: Load parameters
        feature_flags: Feature flags
        proj_epsg: Last seen projection EPSG (ill-defined)
        url: STAC URL (for error messages)

    Returns:
        TargetProjection with calculated parameters
    """
    target_bbox = requested_bbox or stac_bbox

    if not target_bbox:
        raise ProcessParameterInvalidException(
            process="load_stac",
            parameter="spatial_extent",
            reason=f"Unable to derive a spatial extent from provided STAC metadata: {url}, "
            f"please provide a spatial extent.",
        )

    requested_band_epsgs = [epsgs for band_name, epsgs in band_epsgs.items() if band_name in requested_band_names]
    unique_epsgs = {epsg for epsgs in requested_band_epsgs for epsg in epsgs}
    requested_band_cell_sizes = [size for band_name, size in band_cell_size.items() if band_name in requested_band_names]

    if len(unique_epsgs) == 1 and requested_band_cell_sizes:  # exact resolution
        target_epsg = unique_epsgs.pop()
        cell_widths, cell_heights = unzip(*requested_band_cell_sizes)
        cell_width = min(cell_widths)
        cell_height = min(cell_heights)
    elif len(unique_epsgs) == 1:  # about 10m in given CRS
        target_epsg = unique_epsgs.pop()
        # Fall back to the cell size from the layercatalog or provided by user.
        (cell_width, cell_height) = feature_flags.get("cellsize", (10.0, 10.0))
        try:
            utm_zone_from_epsg(proj_epsg)
        except ValueError:
            # Cannot convert EPSG to UTM zone. Use unit from CRS instead of meters.
            target_bbox_center = target_bbox.as_polygon().centroid
            cell_width = GeometryBufferer.transform_meter_to_crs(
                cell_width, f"EPSG:{proj_epsg}", loi=(target_bbox_center.x, target_bbox_center.y)
            )
            cell_height = GeometryBufferer.transform_meter_to_crs(
                cell_height, f"EPSG:{proj_epsg}", loi=(target_bbox_center.x, target_bbox_center.y)
            )
    else:
        # Fall back to the cell size from the layercatalog or provided by user.
        target_epsg = target_bbox.best_utm()
        (cell_width, cell_height) = feature_flags.get("cellsize", (10.0, 10.0))

    if load_params.target_resolution is not None:
        if load_params.target_resolution[0] != 0.0 and load_params.target_resolution[1] != 0.0:
            cell_width = float(load_params.target_resolution[0])
            cell_height = float(load_params.target_resolution[1])

    if load_params.target_crs is not None:
        if (
            load_params.target_resolution is not None
            and load_params.target_resolution[0] != 0.0
            and load_params.target_resolution[1] != 0.0
        ):
            if isinstance(load_params.target_crs, int):
                target_epsg = load_params.target_crs
            elif (
                isinstance(load_params.target_crs, dict)
                and load_params.target_crs.get("id", {}).get("code") == "Auto42001"
            ):
                target_epsg = target_bbox.best_utm()
            else:
                target_epsg = pyproj.CRS.from_user_input(load_params.target_crs).to_epsg()

    return TargetProjection(
        target_epsg=target_epsg,
        cell_width=cell_width,
        cell_height=cell_height,
        target_bbox=target_bbox,
    )


def create_pyramid_factory(
    *,
    jvm,
    opensearch_client: JvmFixedFeaturesOpenSearchClient,
    url: str,
    requested_band_names: List[str],
    cell_width: float,
    cell_height: float,
    max_soft_errors_ratio: float,
    netcdf_with_time_dimension: bool,
):
    """
    Create PyramidFactory (requires JVM).

    Args:
        jvm: JVM instance
        opensearch_client: JVM-backed opensearch client
        url: STAC URL
        requested_band_names: List of requested band names
        cell_width: Cell width in target CRS
        cell_height: Cell height in target CRS
        max_soft_errors_ratio: Maximum allowed soft errors ratio
        netcdf_with_time_dimension: Whether using NetCDF with time dimension

    Returns:
        PyramidFactory JVM object
    """
    if netcdf_with_time_dimension:
        return jvm.org.openeo.geotrellis.layers.NetCDFCollection
    else:
        jvm_opensearch_client = opensearch_client.get_jvm_client()
        return jvm.org.openeo.geotrellis.file.PyramidFactory(
            jvm_opensearch_client,
            url,  # openSearchCollectionId, not important
            requested_band_names,  # openSearchLinkTitles
            None,  # rootPath, not important
            jvm.geotrellis.raster.CellSize(float(cell_width), float(cell_height)),
            False,  # experimental
            max_soft_errors_ratio,
        )


def create_datacube_from_pyramid(
    *,
    jvm,
    pyramid_factory,
    opensearch_client: JvmFixedFeaturesOpenSearchClient,
    target_projection: TargetProjection,
    load_params: StacLoadParameters,
    env: StacEvalEnv,
    items_found: bool,
    allow_empty_cubes: bool,
    netcdf_with_time_dimension: bool,
    requested_bbox: Optional[BoundingBox],
    feature_flags: StacFeatureFlags,
    metadata: GeopysparkCubeMetadata,
) -> GeopysparkDataCube:
    """
    Create GeopysparkDataCube from pyramid factory (requires JVM).

    Args:
        jvm: JVM instance
        pyramid_factory: JVM pyramid factory
        opensearch_client: JVM-backed opensearch client
        target_projection: Target projection parameters
        load_params: Load parameters
        env: Evaluation environment
        items_found: Whether items were found
        allow_empty_cubes: Whether to allow empty cubes
        netcdf_with_time_dimension: Whether using NetCDF with time dimension
        requested_bbox: User-requested bounding box
        feature_flags: Feature flags
        metadata: Cube metadata

    Returns:
        GeopysparkDataCube
    """
    target_bbox = target_projection.target_bbox
    target_epsg = target_projection.target_epsg
    # From_date, to_date are only used to query the opensearch_client and possibly set layer metadata.
    # The fixed opensearch client `getProducts` ignores date range anyway. So we just sync python and jvm metadata.
    from_date = dt.datetime.fromisoformat(metadata.temporal_extent[0])
    to_date = dt.datetime.fromisoformat(metadata.temporal_extent[1])

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
        projected_polygons, target_epsg
    )

    metadata_properties = {}
    correlation_id = env.get(EVAL_ENV_KEY.CORRELATION_ID, "")

    data_cube_parameters, single_level = datacube_parameters.create(load_params=load_params, env=env, jvm=jvm)
    getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

    tilesize = feature_flags.get("tilesize", None)
    if tilesize:
        getattr(data_cube_parameters, "tileSize_$eq")(tilesize)

    try:
        if netcdf_with_time_dimension:
            jvm_opensearch_client = opensearch_client.get_jvm_client()
            pyramid = pyramid_factory.datacube_seq(
                projected_polygons,
                from_date.isoformat(),
                to_date.isoformat(),
                metadata_properties,
                correlation_id,
                data_cube_parameters,
                jvm_opensearch_client,
            )
        elif single_level:
            if not items_found and allow_empty_cubes:
                pyramid = pyramid_factory.empty_datacube_seq(
                    projected_polygons,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    data_cube_parameters,
                )
            else:
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
                pyramid = pyramid_factory.empty_pyramid_seq(
                    extent, extent_crs, from_date.isoformat(), to_date.isoformat()
                )
            else:
                pyramid = pyramid_factory.pyramid_seq(
                    extent, extent_crs, from_date.isoformat(), to_date.isoformat(), metadata_properties, correlation_id
                )
    except Exception as e:
        raise OpenEOApiException(
            message=f"load_stac: Error when constructing datacube from pyramid: {e}",
            status_code=500,
        ) from e

    metadata = metadata.filter_bbox(
        west=extent.xmin(),
        south=extent.ymin(),
        east=extent.xmax(),
        north=extent.ymax(),
        crs=extent_crs,
    )

    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    option = jvm.scala.Option

    levels = {
        pyramid.apply(index)._1(): TiledRasterLayer(
            LayerType.SPACETIME,
            temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2()),
        )
        for index in range(0, pyramid.size())
    }

    return GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)


@deprecated
def load_stac(
    url: str,
    *,
    load_params: LoadParameters,
    env: EvalEnv,
    layer_properties: Optional[PropertyFilterPGMap] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    override_band_names: Optional[List[str]] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    feature_flags: Optional[Dict[str, Any]] = None,
) -> GeopysparkDataCube:
    """
    Load a STAC collection/catalog/item as a GeopysparkDataCube.

    Legacy.
    """
    stac_load_params = StacLoadParameters.from_load_parameters(load_params)
    if feature_flags:
        stac_load_params.featureflags.update(feature_flags)

    stac_env = StacEvalEnv.from_eval_env(env)

    return load_stac_v2(
        url=url,
        load_params=stac_load_params,
        env=stac_env,
        batch_jobs=batch_jobs,
        override_band_names=override_band_names,
        stac_io=stac_io,
    )


def load_stac_v2(
    url: str,
    *,
    load_params: StacLoadParameters,
    env: StacEvalEnv,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    override_band_names: Optional[List[str]] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    opensearch_client: Optional[FixedFeaturesOpenSearchClient] = None,
) -> GeopysparkDataCube:
    """
    Load a STAC collection/catalog/item as a GeopysparkDataCube.

    This function orchestrates the loading process by:
    1. Constructing item collection from STAC metadata
    2. Building features in opensearch client
    3. Calculating target projection
    4. Creating pyramid factory (requires JVM)
    5. Creating datacube from pyramid (requires JVM)
    """
    feature_flags = load_params.featureflags
    logger.info(f"load_stac with {url=} {load_params=} {feature_flags=}")
    if override_band_names is None:
        override_band_names = []
    allow_empty_cubes: bool = feature_flags.allow_empty_cube or env.allow_empty_cubes
    requested_spatiotemporal_extent = _spatiotemporal_extent_from_load_params(load_params)

    try:
        # Step 1: Construct item collection from STAC metadata.
        item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
            url=url,
            spatiotemporal_extent=requested_spatiotemporal_extent,
            property_filter_pg_map=load_params.properties or {},
            batch_jobs=batch_jobs,
            env=env,
            feature_flags=feature_flags,
            stac_io=stac_io,
            user=env.user,
        )

        items_found = len(item_collection.items) > 0
        if not allow_empty_cubes and not items_found:
            raise NoDataAvailableException()

        # Step 2: Convert items to features and add them to the opensearch_client.
        if opensearch_client is None:
            jvm = get_jvm()
            opensearch_client = JvmFixedFeaturesOpenSearchClient(jvm)
        feature_build_result: FeatureBuildResult = build_opensearch_features(
            item_collection=item_collection,
            opensearch_client=opensearch_client,
            load_params=load_params,
            collection_band_names=collection_band_names,
        )

    except OpenEOApiException:
        raise
    except Exception as e:
        raise LoadStacException(url=url, info=repr(e)) from e

    # Step 3: Prepare metadata
    if "x" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="x", extent=[])
    if "y" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="y", extent=[])

    item_collection_temporal_extent = item_collection.get_temporal_extent()
    requested_temporal_extent_from = requested_spatiotemporal_extent.temporal_extent[0] or dt.datetime.min
    requested_temporal_extent_to = requested_spatiotemporal_extent.temporal_extent[1] or dt.datetime.max
    metadata = metadata.with_temporal_extent(
        temporal_extent=(
            dt.datetime.isoformat(item_collection_temporal_extent[0] or requested_temporal_extent_from),
            dt.datetime.isoformat(item_collection_temporal_extent[1] or requested_temporal_extent_to),
        ),
        allow_adding_dimension=True,
    )

    # Overwrite band_names because new bands could be detected in stac items:
    metadata = metadata.with_new_band_names(override_band_names or feature_build_result.collection_band_names)

    if allow_empty_cubes and not metadata.band_names:
        # no knowledge of bands except for what the user requested
        if load_params.bands:
            metadata = metadata.with_new_band_names(load_params.bands)
        else:
            raise ProcessParameterRequiredException(process="load_stac", parameter="bands")

    if load_params.bands:
        metadata = metadata.filter_bands(load_params.bands)

    requested_band_names = metadata.band_names

    # Step 4: Calculate target projection
    target_projection: TargetProjection = calculate_target_projection(
        requested_bbox=requested_spatiotemporal_extent.spatial_extent.as_bbox(),
        stac_bbox=feature_build_result.stac_bbox,
        band_epsgs=feature_build_result.band_epsgs,
        band_cell_size=feature_build_result.band_cell_size,
        requested_band_names=requested_band_names,
        load_params=load_params,
        feature_flags=feature_flags,
        proj_epsg=feature_build_result.proj_epsg,
        url=url,
    )

    # NetCDF band order validation
    if netcdf_with_time_dimension and feature_build_result.asset_band_names:
        # When no products are found, asset_band_names is None
        # TODO: avoid `asset_band_names` as it is an ill-defined here (outside its original for-loop scoped life cycle)
        sorted_bands_from_catalog = sorted(feature_build_result.asset_band_names)
        if requested_band_names != sorted_bands_from_catalog:
            # TODO: Pass band_names to NetCDFCollection, just like PyramidFactory.
            logger.warning(
                f"load_stac: Band order should be alphabetical for NetCDF STAC-catalog with a time dimension. "
                f"Was {requested_band_names}, but should be {sorted_bands_from_catalog} instead.",
            )

    # Step 5: Create pyramid factory (requires JVM)
    if not isinstance(opensearch_client, JvmFixedFeaturesOpenSearchClient):
        raise TypeError(
            f"PyramidFactory creation requires JvmFixedFeaturesOpenSearchClient, got {type(opensearch_client)}"
        )

    jvm = get_jvm()
    max_soft_errors_ratio = env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0)
    pyramid_factory = create_pyramid_factory(
        jvm=jvm,
        opensearch_client=opensearch_client,
        url=url,
        requested_band_names=requested_band_names,
        cell_width=target_projection.cell_width,
        cell_height=target_projection.cell_height,
        max_soft_errors_ratio=max_soft_errors_ratio,
        netcdf_with_time_dimension=netcdf_with_time_dimension,
    )

    # Step 6: Create datacube from pyramid (requires JVM)
    return create_datacube_from_pyramid(
        jvm=jvm,
        pyramid_factory=pyramid_factory,
        opensearch_client=opensearch_client,
        target_projection=target_projection,
        load_params=load_params,
        env=env,
        items_found=items_found,
        allow_empty_cubes=allow_empty_cubes,
        netcdf_with_time_dimension=netcdf_with_time_dimension,
        requested_bbox=requested_spatiotemporal_extent.spatial_extent.as_bbox(),
        feature_flags=feature_flags,
        metadata=metadata,
    )


class _TemporalExtent:
    """
    Helper to represent a load_collection/load_stac-style temporal extent
    with a from_date (inclusive) and to_date (exclusive)
    and calculate intersection with STAC entities
    based on nominal datetime or start_datetime+end_datetime

    refs:
    - https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#datetime
    - https://github.com/radiantearth/stac-spec/blob/master/commons/common-metadata.md#date-and-time-range
    """

    # TODO: move this to a more generic location for better reuse

    __slots__ = ("from_date", "to_date")

    def __init__(self, from_date: DateTimeLikeOrNone, to_date: DateTimeLikeOrNone):
        self.from_date: Union[datetime.datetime, None] = to_datetime_utc_unless_none(from_date)
        self.to_date: Union[datetime.datetime, None] = to_datetime_utc_unless_none(to_date)

    def as_tuple(self) -> Tuple[Union[datetime.datetime, None], Union[datetime.datetime, None]]:
        return self.from_date, self.to_date

    def is_unbounded(self) -> bool:
        return self.from_date is None and self.to_date is None

    def intersects(
        self,
        nominal: DateTimeLikeOrNone = None,
        start_datetime: DateTimeLikeOrNone = None,
        end_datetime: DateTimeLikeOrNone = None,
    ) -> bool:
        """
        Check if the given datetime/interval intersects with the spatiotemporal extent.

        :param nominal: nominal datetime (e.g. typically the "datetime" property of a STAC Item)
        :param start_datetime: start of the interval (e.g. "start_datetime" property of a STAC Item)
        :param end_datetime: end of the interval (e.g. "end_datetime" property of a STAC Item)
        """
        start_datetime = to_datetime_utc_unless_none(start_datetime)
        end_datetime = to_datetime_utc_unless_none(end_datetime)
        nominal = to_datetime_utc_unless_none(nominal)

        # If available, start+end are preferred (cleanly defined interval)
        # fall back on nominal otherwise
        if start_datetime is None and end_datetime is None and nominal:
            start_datetime = end_datetime = nominal

        return (self.from_date is None or end_datetime is None or self.from_date <= end_datetime) and (
            self.to_date is None or start_datetime is None or start_datetime < self.to_date
        )

    def intersects_interval(
        self,
        interval: Union[
            Tuple[DateTimeLikeOrNone, DateTimeLikeOrNone],
            List[DateTimeLikeOrNone],
        ],
    ) -> bool:
        start, end = interval
        return self.intersects(start_datetime=start, end_datetime=end)


class _SpatialExtent:
    """
    Helper to represent a spatial extent with a bounding box
    and calculate intersection with STAC entities (e.g. bbox of a STAC Item).
    """

    # TODO: move this to a more generic location for better reuse

    __slots__ = ("_bbox", "_bbox_lonlat_shape")

    def __init__(self, *, bbox: Union[BoundingBox, None]):
        # TODO: support more bbox representations as input
        self._bbox = bbox
        # cache for shapely polygon in lon/lat
        self._bbox_lonlat_shape = self._bbox.reproject("EPSG:4326").as_polygon() if self._bbox else None

    def as_bbox(self, crs: Optional[str] = None) -> Union[BoundingBox, None]:
        bbox = self._bbox
        if bbox and crs:
            bbox = bbox.reproject(crs)
        return bbox

    def intersects(self, bbox: Union[List[float], Tuple[float, float, float, float], None]):
        # TODO: this assumes bbox is in lon/lat coordinates, also support other CRSes?
        if not self._bbox or bbox is None:
            return True
        return self._bbox_lonlat_shape.intersects(shapely.geometry.Polygon.from_bounds(*bbox))


class _SpatioTemporalExtent:
    # TODO: move this to a more generic location for better reuse
    def __init__(
        self,
        *,
        bbox: Union[BoundingBox, None] = None,
        from_date: DateTimeLikeOrNone = None,
        to_date: DateTimeLikeOrNone = None,
    ):
        self._spatial_extent = _SpatialExtent(bbox=bbox)
        self._temporal_extent = _TemporalExtent(from_date=from_date, to_date=to_date)

    @property
    def spatial_extent(self) -> _SpatialExtent:
        return self._spatial_extent

    @property
    def temporal_extent(self) -> _TemporalExtent:
        return self._temporal_extent

    def item_intersects(self, item: pystac.Item) -> bool:
        return self._temporal_extent.intersects(
            nominal=item.datetime,
            start_datetime=item.properties.get("start_datetime"),
            end_datetime=item.properties.get("end_datetime"),
        ) and self._spatial_extent.intersects(item.bbox)

    def collection_intersects(self, collection: pystac.Collection) -> bool:
        bboxes = collection.extent.spatial.bboxes
        intervals = collection.extent.temporal.intervals
        # If multiple bboxes/intervals, skip the first "overall" one (per STAC spec),
        # for more granular checking (if available)
        if len(bboxes) > 1:
            bboxes = bboxes[1:]
        if len(intervals) > 1:
            intervals = intervals[1:]

        return any(self._spatial_extent.intersects(bbox) for bbox in bboxes) and any(
            self._temporal_extent.intersects_interval(interval) for interval in intervals
        )


def _spatiotemporal_extent_from_load_params(load_params: StacLoadParameters) -> _SpatioTemporalExtent:
    bbox = BoundingBox.from_dict_or_none(load_params.spatial_extent, default_crs="EPSG:4326")
    (from_date, until_date) = (to_datetime_utc_unless_none(d) for d in load_params.temporal_extent)
    if until_date is None:
        to_date = None
    elif from_date == until_date:
        # Fallback mechanism for legacy usage patterns
        to_date = datetime.datetime.combine(until_date, datetime.time.max, until_date.tzinfo)
        logger.warning(
            f"Invalid temporal extent (identical start and end: {from_date!r}). Normalized end to {to_date!r}."
        )
    else:
        # Convert openEO temporal extent convention (end-exclusive) to internal(?) convention (end-inclusive)
        to_date = until_date - datetime.timedelta(milliseconds=1)
    return _SpatioTemporalExtent(bbox=bbox, from_date=from_date, to_date=to_date)


_REGEX_EPSG_CODE = re.compile(r"^EPSG:(\d+)$", re.IGNORECASE)


@functools.lru_cache
def _proj_code_to_epsg(proj_code: str) -> Union[int, None]:
    if isinstance(proj_code, str) and (match := _REGEX_EPSG_CODE.match(proj_code)):
        return int(match.group(1))
    # TODO pass-through integers as-is?
    return None


def _get_asset_property(asset: pystac.Asset, field: str) -> Union[Any, None]:
    """
    Helper to get a property directly from asset,
    or from bands metadata embedded in asset metadata (if consistent across all bands).
    """
    if field in asset.extra_fields:
        return asset.extra_fields.get(field)
    if "bands" in asset.extra_fields:
        # TODO: Is it actually ok to look for projection properties at bands level?
        #       See https://github.com/stac-extensions/projection/issues/25
        values = []
        for band in asset.extra_fields["bands"]:
            if field in band and band[field] and band[field] not in values:
                values.append(band.get(field))
        if len(values) == 1:
            return values[0]
        if len(values) > 1:
            # For now, using debug level here instead of warning,
            # as this can be done for each asset, which might be too much
            logger.debug(f"Multiple differing values for {field=} found in asset bands: {values=}")

    return None


class _ProjectionMetadata:
    """
    Container of and conversion interface for projection metadata from STAC Projection Extension.
    https://github.com/stac-extensions/projection

    Covering these fields:
    - "proj:code" (preferably, with (less ideal) alternative sources:
        "proj:epsg" (deprecated), "proj:wkt2" or "proj:projjson")
    - "proj:bbox"
    - "proj:shape"
    - "proj:transform"
    """

    # TODO: move to more generic geometry/projection utility module for better reuse and cleaner separation?
    # TODO: any added value to leverage projection extension support from pystac in some way?

    __slots__ = ("_code", "_bbox", "_shape", "_transform")

    def __init__(
        self,
        *,
        code: Optional[str] = None,
        epsg: Optional[int] = None,
        bbox: Optional[Sequence[float]] = None,
        shape: Optional[Sequence[int]] = None,
        transform: Optional[Sequence[float]] = None,
    ):
        # TODO: support wkt2 and projjson as well in some way?
        self._code = code or (f"EPSG:{epsg}" if epsg is not None else None)
        self._bbox = tuple(bbox) if bbox else None
        self._shape = tuple(shape) if shape else None
        self._transform = tuple(transform) if transform else None

    def __repr__(self) -> str:
        return f"_ProjectionMetadata(code={self._code!r}, bbox={self._bbox!r}, shape={self._shape!r})"

    @property
    def code(self) -> Union[str, None]:
        return self._code

    @property
    def epsg(self) -> Union[int, None]:
        # Note: The field `proj:epsg` has been deprecated in v1.2.0 of projection extension
        # in favor of `proj:code` and has been removed in v2.0.0.
        return _proj_code_to_epsg(self._code) if self._code else None

    @property
    def bbox(self) -> Union[Tuple[float, float, float, float], None]:
        """
        Bounding box of the assets represented by this Item in the asset data CRS.
        Specified as 4 or 6 coordinates ... e.g., [west, south, east, north], ...
        """
        if self._bbox and len(self._bbox) in {4, 6}:
            # TODO: need for support of 6 values?
            return self._bbox[:4]
        elif self._shape and self._transform:
            # per https://github.com/soxofaan/projection/blob/reformat-best-practices/README.md#projtransform
            a0, a1, a2, a3, a4, a5 = self._transform[:6]

            def project(x: float, y: float) -> Tuple[float, float]:
                return a0 * x + a1 * y + a2, a3 * x + a4 * y + a5

            sy, sx = self._shape
            p00 = project(0, 0)
            px0 = project(sx, 0)
            p0y = project(0, sy)
            pxy = project(sx, sy)
            xs, ys = zip(p00, px0, p0y, pxy)
            return (min(xs), min(ys), max(xs), max(ys))

    def to_bounding_box(self) -> Union[BoundingBox, None]:
        """Get bbox (if any) as BoundingBox object."""
        if bbox := self.bbox:
            return BoundingBox.from_wsen_tuple(bbox, crs=self.code)

    @property
    def shape(self) -> Union[Tuple[int, int], None]:
        """Number of pixels in the most common pixel grid used by the assets (in Y, X order)."""
        if self._shape and len(self._shape) == 2:
            return self._shape
        # TODO: calculate from bbox and transform?

    def resolution(self, *, fail_on_miss: bool = True) -> Union[Tuple[float, float], None]:
        """
        Calculate resolution (xres, yres) expressed as distance in the projection CRS
        based on bbox/shape/transform.
        """
        # TODO: rename to resolution(), which is more self-descriptive than "cell size"?
        if self._bbox and self._shape:
            xmin, ymin, xmax, ymax = self._bbox[:4]
            yn, xn = self.shape
            return float(xmax - xmin) / xn, float(ymax - ymin) / yn
        elif self._transform:
            a0, _, _, _, a4, _ = self._transform[:6]
            return abs(a0), abs(a4)

        if fail_on_miss:
            raise ValueError(f"Unable to calculate cell size with {self._shape=}, {self._bbox}, {self._transform}")
        else:
            return None

    @classmethod
    def from_item(cls, item: pystac.Item) -> "_ProjectionMetadata":
        return cls(
            code=item.properties.get("proj:code"),
            epsg=item.properties.get("proj:epsg"),
            bbox=item.properties.get("proj:bbox"),
            shape=item.properties.get("proj:shape"),
            transform=item.properties.get("proj:transform"),
        )

    @classmethod
    def from_asset(cls, asset: pystac.Asset, *, item: Optional[pystac.Item] = None) -> "_ProjectionMetadata":
        """
        Extract projection metadata from asset, with fallback to asset bands or (owning) item.
        """
        if item is None:
            item = asset.owner

        def get(field):
            return _get_asset_property(asset, field=field) or (item and item.properties.get(field))

        return cls(
            code=get("proj:code"),
            epsg=get("proj:epsg"),
            bbox=get("proj:bbox"),
            shape=get("proj:shape"),
            transform=get("proj:transform"),
        )

    @functools.lru_cache
    def _snappers(self) -> Tuple[GridSnapper, GridSnapper]:
        """Lazy init of x and y coordinate snappers based on bbox and shape"""
        xres, yres = self.resolution(fail_on_miss=True)
        xmin, ymin, xmax, ymax = self.bbox
        x_snapper = GridSnapper(origin=xmin, resolution=xres)
        y_snapper = GridSnapper(origin=ymin, resolution=yres)
        return x_snapper, y_snapper

    def coverage_for(self, extent: BoundingBox, snap: bool = True) -> Union[BoundingBox, None]:
        """
        Find the coverage (as bounding box) of the given extent
        within the pixel grid defined by this `_ProjectionMetadata`,
        including reprojection (if necessary), aligning/snapping to the pixel grid
        and clamping to the bounds.

        Returns None if no intersection or bbox.
        """
        bbox = self.to_bounding_box()
        if not bbox:
            logger.warning(f"coverage_for: missing bbox.")
            return None
        intersection = bbox.intersection(extent)
        if not intersection:
            return None

        if snap:
            x_snapper, y_snapper = self._snappers()
            return BoundingBox(
                west=x_snapper.down(intersection.west),
                south=y_snapper.down(intersection.south),
                east=x_snapper.up(intersection.east),
                north=y_snapper.up(intersection.north),
                crs=self.code,
            )
        else:
            return intersection


def _get_proj_metadata(
    asset: pystac.Asset, *, item: pystac.Item
) -> Tuple[Optional[int], Optional[Tuple[float, float, float, float]], Optional[Tuple[int, int]]]:
    """
    Get projection metadata from asset:
    EPSG code (int), bbox (in that EPSG) and number of pixels (rows, cols), if available.
    """
    # TODO: phase out usage and switch to using _ProjectionMetadata directly?
    metadata = _ProjectionMetadata.from_asset(asset, item=item)
    return metadata.epsg, metadata.bbox, metadata.shape


def _get_pixel_value_offset(*, item: pystac.Item, asset: pystac.Asset) -> float:
    raster_scale = asset.extra_fields.get("raster:scale", item.properties.get("raster:scale", 1.0))
    raster_offset = asset.extra_fields.get("raster:offset", item.properties.get("raster:offset", 0.0))
    return raster_offset / raster_scale


def get_best_url(asset: pystac.Asset):
    """
    Relevant doc: https://github.com/stac-extensions/alternate-assets
    """
    for key, alternate_asset in asset.extra_fields.get("alternate", {}).items():
        if key in {"local", "s3"}:
            href = alternate_asset["href"]
            # Checking if file exists takes around 10ms on /data/MTDA mounted on laptop
            # Checking if URL exists takes around 100ms on https://services.terrascope.be
            # Checking if URL exists depends also on what Datasource is used in the scala code.
            # That would be hacky to predict here.
            url = urlparse(href)
            # Support paths like "file:///data/MTDA", but also "//data/MTDA" just in case.

            file_path = None
            if url.scheme in ["", "file"]:
                file_path = url.path
            elif url.scheme == "s3":
                file_path = f"/{url.netloc}{url.path}"

            if file_path and Path(file_path).exists():
                logger.debug(f"Using local alternate file path {file_path}")
                return file_path
            else:
                logger.warning(f"Only support file paths as local alternate urls, but found {href}")

    href = asset.get_absolute_href() or asset.href

    return (
        href.replace("s3://eodata/", "/vsis3/EODATA/")
        if os.environ.get("AWS_DIRECT") == "TRUE"
        else href.replace("s3://eodata/", "/eodata/")
    )


def _compute_cellsize(
    proj_bbox: Tuple[float, float, float, float],
    proj_shape: Tuple[float, float],
) -> Tuple[float, float]:
    # TODO: replace usage with _ProjectionMetadata.cell_size()?
    xmin, ymin, xmax, ymax = proj_bbox
    rows, cols = proj_shape
    cell_width = (xmax - xmin) / cols
    cell_height = (ymax - ymin) / rows
    return cell_width, cell_height


class NoveltyTracker:
    """Utility to detect new things."""

    # TODO: move to more general utility module

    def __init__(self):
        self._seen: set = set()

    def is_new(self, x) -> bool:
        """Check if the item is new (not seen before)."""
        if isinstance(x, list):
            key = tuple(x)
        else:
            # TODO: wider coverage to make the thing hashable
            key = x
        if key in self._seen:
            return False
        else:
            self._seen.add(key)
            return True

    def already_seen(self, x) -> bool:
        """Check if the item was seen before."""
        return not self.is_new(x)
