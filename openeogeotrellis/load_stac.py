from __future__ import annotations

import datetime
import datetime as dt
import functools
import logging
import os
import re
import time
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union, Iterable
from urllib.parse import urlparse

import geopyspark as gps
import openeo_driver.backend
import planetary_computer
import pyproj
import pystac
import pystac.stac_io
import pystac.utils
import pystac_client
import pystac_client.stac_api_io
import requests.adapters
from geopyspark import LayerType, TiledRasterLayer
from openeo.metadata import _StacMetadataParser
from openeo.util import Rfc3339, dict_no_none
from openeo_driver import filter_properties
from openeo_driver.backend import BatchJobMetadata, LoadParameters
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.errors import (
    JobNotFoundException,
    OpenEOApiException,
    ProcessParameterInvalidException,
    ProcessParameterRequiredException,
    ProcessParameterUnsupportedException,
)
from openeo_driver.jobregistry import PARTIAL_JOB_STATUS
from openeo_driver.ProcessGraphDeserializer import DEFAULT_TEMPORAL_EXTENT
from openeo_driver.users import User
from openeo_driver.util.geometry import BoundingBox, GeometryBufferer
from openeo_driver.util.http import requests_with_retry
from openeo_driver.util.utm import utm_zone_from_epsg
from openeo_driver.utils import EvalEnv
from pystac import STACObject
import shapely.geometry
from urllib3 import Retry

from openeogeotrellis import datacube_parameters
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.integrations.stac import ResilientStacIO
from openeogeotrellis.util.datetime import DateTimeLikeOrNone, to_datetime_utc_unless_none
from openeogeotrellis.utils import get_jvm, map_optional, normalize_temporal_extent, to_projected_polygons, unzip

logger = logging.getLogger(__name__)
REQUESTS_TIMEOUT_SECONDS = 60


class NoDataAvailableException(OpenEOApiException):
    status_code = 400
    code = "NoDataAvailable"
    message = "There is no data available for the given extents."


class LoadStacException(OpenEOApiException):
    """Generic/base exception for load_stac failures"""

    status_code = 500
    code = "LoadStacFailure"

    def __init__(
        self,
        *,
        url: str = "n/a",
        info: str = "n/a",
        message: Optional[str] = None,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
    ):
        if not message:
            message = f"Error when constructing data cube from load_stac({url!r}): {info}"
        super().__init__(message=message, code=code, status_code=status_code)
        self.url = url


# Some type aliases related to property filters expressed as process graphs
# (e.g. like the `properties` argument of `load_collection`/`load_stac` processes).
FlatProcessGraph = Dict[str, dict]
PropertyFilterPGMap = Dict[str, FlatProcessGraph]


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
    if override_band_names is None:
        override_band_names = []

    # Feature flags: merge global (e.g. from layer catalog info) and user-provided (higher precedence)
    feature_flags = {**(feature_flags or {}), **load_params.get("featureflags", {})}

    logger.info(f"load_stac with {url=} {load_params=} {feature_flags=}")

    allow_empty_cubes = feature_flags.get("allow_empty_cube", env.get(EVAL_ENV_KEY.ALLOW_EMPTY_CUBES, False))

    # Merge property filters from layer catalog and user-provided load_params (with precedence to load_params)
    property_filter_pg_map: PropertyFilterPGMap = {
        **(layer_properties or {}),
        **(load_params.properties or {}),
    }

    user: Optional[User] = env.get("user")

    requested_bbox = BoundingBox.from_dict_or_none(load_params.spatial_extent, default_crs="EPSG:4326")
    temporal_extent = load_params.temporal_extent
    from_date, until_date = map(dt.datetime.fromisoformat, normalize_temporal_extent(temporal_extent))
    to_date = (
        dt.datetime.combine(until_date, dt.time.max, until_date.tzinfo)
        if from_date == until_date
        else until_date - dt.timedelta(milliseconds=1)
    )

    try:
        item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
            url=url,
            load_params=load_params,
            property_filter_pg_map=property_filter_pg_map,
            batch_jobs=batch_jobs,
            env=env,
            feature_flags=feature_flags,
            stac_io=stac_io,
            user=user,
        )

        # Deduplicate items
        # TODO: smarter and more fine-grained deduplication behavior?
        #       - enable by default or only do it on STAC API usage?
        #       - allow custom deduplicators (e.g. based on layer catalog info about openeo collections)
        if feature_flags.get("deduplicate_items", get_backend_config().load_stac_deduplicate_items_default):
            item_collection = item_collection.deduplicated(deduplicator=ItemDeduplicator())

        items_found = len(item_collection.items) > 0
        if not allow_empty_cubes and not items_found:
            raise NoDataAvailableException()

        jvm = get_jvm()

        opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

        asset_band_names = None
        stac_bbox = None
        proj_epsg = None
        proj_bbox = None
        proj_shape = None

        stac_metadata_parser = _StacMetadataParser(logger=logger)

        # The minimum cell size per band name across all assets
        band_cell_size: Dict[str, Tuple[float, float]] = {}
        band_epsgs: Dict[str, Set[int]] = {}

        for itm in item_collection.items:

            band_assets = {
                asset_id: asset for asset_id, asset in dict(sorted(itm.assets.items())).items() if _is_band_asset(asset)
            }

            builder = (
                jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
                .withId(itm.id)
                .withNominalDate(itm.properties.get("datetime") or itm.properties["start_datetime"])
            )

            band_names_tracker = NoveltyTracker()
            for asset_id, asset in sorted(
                # Go through assets ordered by asset GSD (from finer to coarser) if possible,
                # falling back on deterministic alphabetical asset_id order.
                # see https://github.com/Open-EO/openeo-geopyspark-driver/pull/1213#discussion_r2107353442
                band_assets.items(),
                key=lambda kv: (
                    float(kv[1].extra_fields.get("gsd") or itm.properties.get("gsd") or 40e6),
                    kv[0],
                ),
            ):
                proj_epsg, proj_bbox, proj_shape = _get_proj_metadata(asset=asset, item=itm)

                asset_band_names_from_metadata = stac_metadata_parser.bands_from_stac_asset(asset=asset).band_names()
                logger.debug(f"from intersecting_items: {itm.id=} {asset_id=} {asset_band_names_from_metadata=}")

                if not load_params.bands:
                    # No user-specified band filtering: follow band names from metadata (if possible)
                    asset_band_names = asset_band_names_from_metadata or [asset_id]
                elif isinstance(load_params.bands, list) and asset_id in load_params.bands:
                    # User-specified asset_id as band name: use that directly
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

            if proj_epsg:
                builder = builder.withCRS(f"EPSG:{proj_epsg}")
            if proj_bbox:
                builder = builder.withRasterExtent(*proj_bbox)

            # TODO: does not seem right conceptually; an Item's assets can have different resolutions (and CRS)
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

            opensearch_client.addFeature(builder.build())

            stac_bbox = (
                item_bbox
                if stac_bbox is None
                else BoundingBox.from_wsen_tuple(
                    item_bbox.as_polygon().union(stac_bbox.as_polygon()).bounds, stac_bbox.crs
                )
            )

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

    if "x" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="x", extent=[])
    if "y" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="y", extent=[])

    item_collection_temporal_extent = item_collection.get_temporal_extent()
    metadata = metadata.with_temporal_extent(
        temporal_extent=(
            map_optional(dt.datetime.isoformat, item_collection_temporal_extent[0]) or temporal_extent[0],
            map_optional(dt.datetime.isoformat, item_collection_temporal_extent[1]) or temporal_extent[1],
        ),
        allow_adding_dimension=True,
    )
    # Overwrite band_names because new bands could be detected in stac items:
    metadata = metadata.with_new_band_names(override_band_names or collection_band_names)

    if allow_empty_cubes and not metadata.band_names:
        # no knowledge of bands except for what the user requested
        if load_params.bands:
            metadata = metadata.with_new_band_names(load_params.bands)
        else:
            raise ProcessParameterRequiredException(process="load_stac", parameter="bands")

    if load_params.global_extent is None or len(load_params.global_extent) == 0:
        layer_native_extent = metadata.get_layer_native_extent()
        if layer_native_extent:
            load_params = load_params.copy()
            logger.info(f"global_extent fallback: {layer_native_extent=}")
            load_params.global_extent = layer_native_extent.as_dict()

    if load_params.bands:
        metadata = metadata.filter_bands(load_params.bands)

    requested_band_names = metadata.band_names

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

    if netcdf_with_time_dimension:
        # TODO: avoid `asset_band_names` as it is an ill-defined here (outside its original for-loop scoped life cycle)
        if asset_band_names:  # When no products are found, asset_band_names is None
            sorted_bands_from_catalog = sorted(asset_band_names)
            if requested_band_names != sorted_bands_from_catalog:
                # TODO: Pass band_names to NetCDFCollection, just like PyramidFactory.
                logger.warning(
                    f"load_stac: Band order should be alphabetical for NetCDF STAC-catalog with a time dimension. "
                    f"Was {requested_band_names}, but should be {sorted_bands_from_catalog} instead.",
                )
        pyramid_factory = jvm.org.openeo.geotrellis.layers.NetCDFCollection
    else:
        max_soft_errors_ratio = env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0)

        pyramid_factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
            opensearch_client,
            url,  # openSearchCollectionId, not important
            requested_band_names,  # openSearchLinkTitles
            None,  # rootPath, not important
            jvm.geotrellis.raster.CellSize(cell_width, cell_height),
            False,  # experimental
            max_soft_errors_ratio,
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
        projected_polygons, target_epsg
    )

    metadata_properties = {}
    correlation_id = env.get(EVAL_ENV_KEY.CORRELATION_ID, "")

    data_cube_parameters, single_level = datacube_parameters.create(load_params, env, jvm)
    getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

    tilesize = feature_flags.get("tilesize", None)
    if tilesize:
        getattr(data_cube_parameters, "tileSize_$eq")(tilesize)

    try:
        if netcdf_with_time_dimension:
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
            message=f"load_stac: Error when constructing datacube from {url}: {e}",
            status_code=500,
        ) from e

    metadata = metadata.filter_temporal(from_date.isoformat(), to_date.isoformat())

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

    return GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)


def construct_item_collection(
    url: str,
    *,
    load_params: Optional[LoadParameters] = None,
    property_filter_pg_map: Optional[PropertyFilterPGMap] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    env: Optional[EvalEnv] = None,
    feature_flags: Optional[Dict[str, Any]] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    user: Optional[User] = None,
) -> Tuple[ItemCollection, GeopysparkCubeMetadata, List[str], bool]:
    """
    Construct Stac ItemCollection from given load_stac URL
    """
    load_params = load_params or LoadParameters()
    property_filter_pg_map = property_filter_pg_map or {}
    env = env or EvalEnv()
    feature_flags = feature_flags or {}

    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(load_params)

    netcdf_with_time_dimension = False

    backend_config = get_backend_config()
    poll_interval_seconds = backend_config.job_dependencies_poll_interval_seconds
    max_poll_delay_seconds = backend_config.job_dependencies_max_poll_delay_seconds
    max_poll_time = time.time() + max_poll_delay_seconds

    dependency_job_info = (
        _await_dependency_job(
            url=url,
            user=user,
            batch_jobs=batch_jobs,
            poll_interval_seconds=poll_interval_seconds,
            max_poll_delay_seconds=max_poll_delay_seconds,
            max_poll_time=max_poll_time,
        )
        if user and batch_jobs
        else None
    )

    stac_metadata_parser = _StacMetadataParser(logger=logger)

    if dependency_job_info and batch_jobs:
        # TODO: improve metadata for this case
        metadata = GeopysparkCubeMetadata(metadata={})
        item_collection = ItemCollection.from_own_job(
            job=dependency_job_info, spatiotemporal_extent=spatiotemporal_extent, batch_jobs=batch_jobs, user=user
        )
        # TODO: improve band name detection for this case
        band_names = []
    else:
        logger.info(f"load_stac of arbitrary URL {url}")

        stac_object = _await_stac_object(
            url=url,
            poll_interval_seconds=poll_interval_seconds,
            max_poll_delay_seconds=max_poll_delay_seconds,
            max_poll_time=max_poll_time,
            stac_io=stac_io,
        )

        if isinstance(stac_object, pystac.Item):
            if property_filter_pg_map:
                # as dictated by the load_stac spec
                # TODO: it's not that simple see https://github.com/Open-EO/openeo-processes/issues/536 and https://github.com/Open-EO/openeo-processes/pull/547
                raise ProcessParameterUnsupportedException(process="load_stac", parameter="properties")

            item = stac_object
            # TODO: improve metadata for this case
            metadata = GeopysparkCubeMetadata(metadata={})
            band_names = stac_metadata_parser.bands_from_stac_item(item=item).band_names()
            item_collection = ItemCollection.from_stac_item(item=item, spatiotemporal_extent=spatiotemporal_extent)
        elif isinstance(stac_object, pystac.Collection) and _supports_item_search(stac_object):
            collection = stac_object
            netcdf_with_time_dimension = contains_netcdf_with_time_dimension(collection)
            metadata = GeopysparkCubeMetadata(
                metadata=collection.to_dict(include_self_link=False, transform_hrefs=False)
            )

            band_names = stac_metadata_parser.bands_from_stac_collection(collection=collection).band_names()
            property_filter = PropertyFilter(properties=property_filter_pg_map, env=env)

            item_collection = ItemCollection.from_stac_api(
                collection=stac_object,
                original_url=url,
                property_filter=property_filter,
                spatiotemporal_extent=spatiotemporal_extent,
                use_filter_extension=feature_flags.get("use-filter-extension", True),
                skip_datetime_filter=(
                    (load_params.temporal_extent is DEFAULT_TEMPORAL_EXTENT) or netcdf_with_time_dimension
                ),
            )
        else:
            assert isinstance(stac_object, pystac.Catalog)  # static Catalog + Collection
            catalog = stac_object
            metadata = GeopysparkCubeMetadata(metadata=catalog.to_dict(include_self_link=False, transform_hrefs=False))

            if property_filter_pg_map:
                # as dictated by the load_stac spec
                # TODO: it's not that simple see https://github.com/Open-EO/openeo-processes/issues/536 and https://github.com/Open-EO/openeo-processes/pull/547
                raise ProcessParameterUnsupportedException(process="load_stac", parameter="properties")

            if isinstance(catalog, pystac.Collection):
                netcdf_with_time_dimension = contains_netcdf_with_time_dimension(collection=catalog)

            band_names = stac_metadata_parser.bands_from_stac_object(obj=stac_object).band_names()

            item_collection = ItemCollection.from_stac_catalog(catalog, spatiotemporal_extent=spatiotemporal_extent)

    # TODO: possible to embed band names in metadata directly?
    return item_collection, metadata, band_names, netcdf_with_time_dimension


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
    def __init__(self, *, bbox: Union[BoundingBox, None], from_date: DateTimeLikeOrNone, to_date: DateTimeLikeOrNone):
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


def _spatiotemporal_extent_from_load_params(load_params: LoadParameters) -> _SpatioTemporalExtent:
    bbox = BoundingBox.from_dict_or_none(load_params.spatial_extent, default_crs="EPSG:4326")
    from_date, until_date = map(dt.datetime.fromisoformat, normalize_temporal_extent(load_params.temporal_extent))
    to_date = (
        dt.datetime.combine(until_date, dt.time.max, until_date.tzinfo)
        if from_date == until_date
        else until_date - dt.timedelta(milliseconds=1)
    )
    return _SpatioTemporalExtent(bbox=bbox, from_date=from_date, to_date=to_date)


def _get_item_temporal_extent(item: pystac.Item) -> Tuple[datetime.datetime, datetime.datetime]:
    if start := item.properties.get("start_datetime"):
        start = pystac.utils.str_to_datetime(start)
    else:
        start = item.datetime
    if end := item.properties.get("end_datetime"):
        end = pystac.utils.str_to_datetime(end)
    else:
        end = item.datetime
    return start, end


class ItemCollection:
    """
    Collection of STAC Items.
    Typically a subset from a larger Collection/Catalog/API based on spatiotemporal filtering.

    Experimental/WIP API
    """

    # TODO: leverage pystac.ItemCollection in some way ?

    def __init__(self, items: List[pystac.Item]):
        self.items = items

    @staticmethod
    def from_stac_item(item: pystac.Item, *, spatiotemporal_extent: _SpatioTemporalExtent) -> ItemCollection:
        items = [item] if spatiotemporal_extent.item_intersects(item) else []
        return ItemCollection(items)

    @staticmethod
    def from_own_job(
        job: BatchJobMetadata,
        *,
        spatiotemporal_extent: _SpatioTemporalExtent,
        batch_jobs: openeo_driver.backend.BatchJobs,
        user: Optional[User],
    ) -> ItemCollection:
        items = []
        rfc3339 = Rfc3339(propagate_none=True)

        for asset_id, asset in batch_jobs.get_result_assets(job_id=job.id, user_id=user.user_id).items():
            parse_datetime = partial(rfc3339.parse_datetime, with_timezone=True)

            item_geometry = asset.get("geometry", job.geometry)
            item_bbox = asset.get("bbox", job.bbox)
            item_datetime = parse_datetime(asset.get("datetime"))
            item_start_datetime = None
            item_end_datetime = None

            if not item_datetime:
                item_start_datetime = parse_datetime(asset.get("start_datetime")) or job.start_datetime
                item_end_datetime = parse_datetime(asset.get("end_datetime")) or job.end_datetime

                if item_start_datetime == item_end_datetime:
                    item_datetime = item_start_datetime

            pystac_item = pystac.Item(
                id=asset_id,
                geometry=item_geometry,
                bbox=item_bbox,
                datetime=item_datetime,
                properties=dict_no_none(
                    {
                        "datetime": rfc3339.datetime(item_datetime),
                        "start_datetime": rfc3339.datetime(item_start_datetime),
                        "end_datetime": rfc3339.datetime(item_end_datetime),
                        "proj:epsg": asset.get("proj:epsg"),
                        "proj:bbox": asset.get("proj:bbox"),
                        "proj:shape": asset.get("proj:shape"),
                    }
                ),
            )

            if spatiotemporal_extent.item_intersects(pystac_item) and "data" in asset.get("roles", []):
                pystac_asset = pystac.Asset(
                    href=asset["href"],
                    extra_fields={
                        "eo:bands": [{"name": b.name} for b in asset["bands"]]
                        # TODO #1109 #1015 also add common "bands"?
                    },
                )
                pystac_item.add_asset(asset_id, pystac_asset)
                items.append(pystac_item)

        return ItemCollection(items)

    @staticmethod
    def from_stac_catalog(catalog: pystac.Catalog, *, spatiotemporal_extent: _SpatioTemporalExtent) -> ItemCollection:
        def intersecting_catalogs(root: pystac.Catalog) -> Iterator[pystac.Catalog]:
            if isinstance(root, pystac.Collection) and not spatiotemporal_extent.collection_intersects(root):
                return
            yield root
            for child in root.get_children():
                yield from intersecting_catalogs(child)

        items = [
            item
            for intersecting_catalog in intersecting_catalogs(root=catalog)
            for item in intersecting_catalog.get_items(recursive=False)
            if spatiotemporal_extent.item_intersects(item)
        ]
        return ItemCollection(items)

    @staticmethod
    def from_stac_api(
        collection: pystac.Collection,
        *,
        property_filter: PropertyFilter,
        spatiotemporal_extent: _SpatioTemporalExtent,
        use_filter_extension: Union[bool, str] = True,
        skip_datetime_filter: bool = False,
        original_url: str = "n/a",
    ) -> ItemCollection:
        root_catalog = collection.get_root()

        # TODO: avoid hardcoded domain sniffing. Possible to discover capabilities in some way?
        # TODO: still necessary to handle `fields` here? It's apparently always the same.
        if root_catalog.get_self_href().startswith("https://planetarycomputer.microsoft.com/api/stac/v1"):
            modifier = planetary_computer.sign_inplace
            # by default, returns all properties and an invalid STAC Item if fields are specified
            fields = None
        elif (
            root_catalog.get_self_href().startswith("https://tamn.snapplanet.io")
            or root_catalog.get_self_href().startswith("https://stac.eurac.edu")
            or root_catalog.get_self_href().startswith("https://catalogue.dataspace.copernicus.eu/stac")
            or root_catalog.get_self_href().startswith("https://pgstac.demo.cloudferro.com")
        ):
            modifier = None
            # by default, returns all properties and "none" if fields are specified
            fields = None
        else:
            modifier = None
            # Those now also return all fields by default as well:
            # https://stac.openeo.vito.be/ and https://stac.terrascope.be
            fields = None

        retry = requests.adapters.Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=frozenset([429, 500, 502, 503, 504]),
            allowed_methods=Retry.DEFAULT_ALLOWED_METHODS.union({"POST"}),
            raise_on_status=False,  # otherwise StacApiIO will catch this and lose the response body
        )
        query_info = ""
        try:
            stac_io = pystac_client.stac_api_io.StacApiIO(timeout=REQUESTS_TIMEOUT_SECONDS, max_retries=retry)
            client = pystac_client.Client.open(root_catalog.get_self_href(), modifier=modifier, stac_io=stac_io)

            cql2_filter = property_filter.to_cql2_filter(
                client=client,
                use_filter_extension=use_filter_extension,
            )
            method = "POST" if isinstance(cql2_filter, dict) else "GET"
            query_info += f" {use_filter_extension=} {cql2_filter=} {method=}"

            bbox = spatiotemporal_extent.spatial_extent.as_bbox(crs="EPSG:4326")
            bbox = bbox.as_wsen_tuple() if bbox else None
            search_request = client.search(
                method=method,
                collections=collection.id,
                bbox=bbox,
                limit=20,
                datetime=None if skip_datetime_filter else spatiotemporal_extent.temporal_extent.as_tuple(),
                filter=cql2_filter,
                fields=fields,
            )
            query_info += f" {search_request.url=} {search_request.get_parameters()=}"
            logger.info(f"ItemCollection.from_stac_api: STAC API request: {query_info}")

            # STAC API might not support Filter Extension so always use client-side filtering as well
            # TODO: check "filter" conformance class for this instead of blindly trying to do double work
            #       see https://github.com/stac-api-extensions/filter
            property_matcher = property_filter.build_matcher()
            items = [item for item in search_request.items() if property_matcher(item.properties)]
        except Exception as e:
            raise LoadStacException(
                url=original_url, info=f"failed to construct ItemCollection from STAC API. {query_info=} {e=}"
            ) from e

        return ItemCollection(items)

    def get_temporal_extent(self) -> Tuple[Union[datetime.datetime, None], Union[datetime.datetime, None]]:
        """Get overall tempoarl extent of all items in the collection."""
        start = None
        end = None
        for item in self.items:
            item_start, item_end = _get_item_temporal_extent(item=item)
            if not start or item_start < start:
                start = item_start
            if not end or item_end > end:
                end = item_end
        return start, end

    def deduplicated(self, deduplicator: "ItemDeduplicator") -> ItemCollection:
        """Create new ItemCollection by deduplicating items using the given deduplicator."""
        orig_count = len(self.items)
        items = deduplicator.deduplicate(items=self.items)
        logger.debug(f"ItemCollection.deduplicated: from {orig_count} to {len(items)}")
        return ItemCollection(items=items)


class ItemDeduplicator:
    """
    Deduplicate STAC Items based on nominal datetime and selected properties.
    """

    DEFAULT_DUPLICATION_PROPERTIES = [
        "platform",
        "constellation",
        "gsd",
        "processing:level",
        "product:timeliness",
        "product:type",
        "proj:code",
        "sar:frequency_band",
        "sar:instrument_mode",
        "sar:observation_direction",
        "sar:polarizations",
        "sat:absolute_orbit",
        "sat:orbit_state",
    ]

    def __init__(self, *, time_shift_max: float = 30, duplication_properties: Optional[List[str]] = None):
        self._time_shift_max = time_shift_max

        # Duplication properties: properties that will be compared
        # with simple equality to determine duplication (among other criteria).
        if duplication_properties is None:
            self._duplication_properties = self.DEFAULT_DUPLICATION_PROPERTIES
        else:
            self._duplication_properties = duplication_properties

    @staticmethod
    def _item_nominal_date(item: pystac.Item) -> datetime.datetime:
        # TODO: cache result (e.g. by item id)?
        dt = item.datetime or pystac.utils.str_to_datetime(item.properties["start_datetime"])
        # ensure UTC timezone for proper comparison
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

    def _is_duplicate_item(self, item1: pystac.Item, item2: pystac.Item) -> bool:
        try:
            return (
                (
                    abs((self._item_nominal_date(item1) - self._item_nominal_date(item2)).total_seconds())
                    < self._time_shift_max
                )
                and all(item1.properties.get(p) == item2.properties.get(p) for p in self._duplication_properties)
                and self._is_same_bbox(item1.bbox, item2.bbox)
                and self._is_same_geometry(item1.geometry, item2.geometry)
            )
        except Exception as e:
            logger.warning(f"Failed to compare {item1.id=} and {item2.id=} for duplication: {e=}", exc_info=True)
            return False

    def _is_same_bbox(self, bbox1: Optional[List[float]], bbox2: Optional[List[float]], epsilon=1e-6) -> bool:
        if isinstance(bbox1, list) and isinstance(bbox2, list):
            return len(bbox1) == 4 and len(bbox2) == 4 and all(abs(a - b) <= epsilon for a, b in zip(bbox1, bbox2))
        elif bbox1 is None and bbox2 is None:
            return True
        else:
            return False

    def _is_same_geometry(self, geom1: Optional[Dict], geom2: Optional[Dict]) -> bool:
        if isinstance(geom1, dict) and isinstance(geom2, dict):
            # TODO: need for smarter geometry comparison (e.g. within some epsilon)?
            return shapely.equals(shapely.geometry.shape(geom1), shapely.geometry.shape(geom2))
        elif geom1 is None and geom2 is None:
            return True
        else:
            return False

    def _score(self, item: pystac.Item) -> tuple:
        """Score an item for deduplication preference (higher is better)."""
        # Prefer more recently updated items
        # use item id as tie breaker
        return (item.properties.get("updated", ""), item.id)

    def _group_duplicates(self, items: Iterable[pystac.Item]) -> Iterator[List[pystac.Item]]:
        """Produce groups of duplicate items."""
        # Pre-sort items, to allow quick breaking out of inner loop
        items = sorted(items, key=self._item_nominal_date)
        handled = set()
        time_shift_max = datetime.timedelta(seconds=self._time_shift_max)
        stats = {"items": 0, "groups": 0}
        for i, item_i in enumerate(items):
            stats["items"] += 1
            if i in handled:
                continue
            group = [item_i]
            horizon = self._item_nominal_date(item_i) + time_shift_max
            for j in range(i + 1, len(items)):
                item_j = items[j]
                if self._item_nominal_date(item_j) > horizon:
                    break
                if self._is_duplicate_item(item_i, item_j):
                    group.append(item_j)
                    handled.add(j)
            yield group
            stats["groups"] += 1
        logger.debug(f"ItemDeduplicator._group_duplicates {stats=}")

    def deduplicate(self, items: Iterable[pystac.Item]) -> List[pystac.Item]:
        result = []
        for group in self._group_duplicates(items):
            if len(group) > 1:
                best = max(group, key=self._score)
                logger.debug(f"Deduplicate: keeping {best.id=} from {len(group)=}")
            else:
                best = group[0]
            result.append(best)
        return result


def _is_supported_raster_mime_type(mime_type: str) -> bool:
    mime_type = mime_type.lower()
    # https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
    return (
        mime_type.startswith("image/tiff")  # No 'image/tif', only double 'f' in spec
        or mime_type.startswith("image/vnd.stac.geotiff")
        or mime_type.startswith("image/jp2")
        or mime_type.startswith("image/png")
        or mime_type.startswith("image/jpeg")
        or mime_type.startswith("application/x-hdf")  # matches hdf5 and hdf
        or mime_type.startswith("application/x-netcdf")
        or mime_type.startswith("application/netcdf")
    )


def _is_band_asset(asset: pystac.Asset) -> bool:
    # TODO: what does this function actually detect?
    #       Name seems to suggest that it's about having necessary band metadata (e.g. a band name)
    #       but implementation also seems to be happy with just being loadable as raster data in some sense.

    # Skip unsupported media types (if known)
    if asset.media_type and not _is_supported_raster_mime_type(asset.media_type):
        return False

    # Decide based on role (if known)
    if asset.roles is None:
        pass
    elif len(asset.roles) > 0:
        roles_with_bands = {
            "data",
            "data-mask",
            "snow-ice",
            "land-water",
            "water-mask",
        }
        return bool(roles_with_bands.intersection(asset.roles))
    else:
        logger.warning(f"_is_band_asset with {asset.href=}: ignoring empty {asset.roles=}")

    # Fallback based on presence of any band metadata
    return (
        "eo:bands" in asset.extra_fields
        or "bands" in asset.extra_fields  # TODO: built-in "bands" support seems to be scheduled for pystac V2
    )


_REGEX_EPSG_CODE = re.compile(r"^EPSG:(\d+)$", re.IGNORECASE)


@functools.lru_cache
def _proj_code_to_epsg(proj_code: str) -> Optional[int]:
    if isinstance(proj_code, str) and (match := _REGEX_EPSG_CODE.match(proj_code)):
        return int(match.group(1))
    # TODO pass-through integers as-is?
    return None


def _get_proj_metadata(
    asset: pystac.Asset, *, item: pystac.Item
) -> Tuple[Optional[int], Optional[Tuple[float, float, float, float]], Optional[Tuple[int, int]]]:
    """
    Get projection metadata from asset:
    EPSG code (int), bbox (in that EPSG) and number of pixels (rows, cols), if available.
    """
    # TODO: possible to avoid item argument and just use asset.owner?

    def _get_asset_property(asset: pystac.Asset, field: str) -> Optional:
        """Helper to get a property directly from asset, or from bands (if consistent across all bands)"""
        # TODO: Is band peeking feature general enough to make this a more reusable helper?
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

    # Note: The field `proj:epsg` has been deprecated in v1.2.0 of projection extension
    # in favor of `proj:code` and has been removed in v2.0.0.
    proj_code = _get_asset_property(asset, field="proj:code") or item.properties.get("proj:code")
    epsg = (
        _proj_code_to_epsg(proj_code)
        or _get_asset_property(asset, field="proj:epsg")
        or item.properties.get("proj:epsg")
    )

    proj_bbox = _get_asset_property(asset, field="proj:bbox") or item.properties.get("proj:bbox")

    if not proj_bbox and epsg == 4326:
        proj_bbox = item.bbox

    proj_shape = _get_asset_property(asset, field="proj:shape") or item.properties.get("proj:shape")

    return (
        epsg,
        tuple(map(float, proj_bbox)) if proj_bbox else None,
        tuple(proj_shape) if proj_shape else None,
    )


def _get_pixel_value_offset(*, item: pystac.Item, asset: pystac.Asset) -> float:
    raster_scale = asset.extra_fields.get("raster:scale", item.properties.get("raster:scale", 1.0))
    raster_offset = asset.extra_fields.get("raster:offset", item.properties.get("raster:offset", 0.0))
    return raster_offset / raster_scale


def _supports_item_search(collection: pystac.Collection) -> bool:
    # TODO: use pystac_client instead?
    catalog = collection.get_root()
    if catalog:
        conforms_to = catalog.extra_fields.get("conformsTo", [])
        return any(re.match(r"^https://api\.stacspec\.org/v1\..*/item-search$", c) for c in conforms_to)
    return False


def contains_netcdf_with_time_dimension(collection: pystac.Collection) -> bool:
    """
    Checks if the STAC collection contains netcdf files with multiple time stamps.
    This collection organization is used for storing small patches of EO data, and requires special loading because the
    default readers will not handle this case properly.

    """
    if collection is not None:
        # we found some collection level metadata
        item_assets = collection.extra_fields.get("item_assets", {})
        dimensions = set(
            [
                tuple(v.get("dimensions"))
                for i in item_assets.values()
                if "cube:variables" in i
                for v in i.get("cube:variables", {}).values()
            ]
        )
        # this is one way to determine if a time dimension is used, but it does depend on the use of item_assets and datacube extension.
        return len(dimensions) == 1 and "time" in dimensions.pop()
    return False


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
    xmin, ymin, xmax, ymax = proj_bbox
    rows, cols = proj_shape
    cell_width = (xmax - xmin) / cols
    cell_height = (ymax - ymin) / rows
    return cell_width, cell_height


def extract_own_job_info(
    url: str, user_id: str, batch_jobs: openeo_driver.backend.BatchJobs
) -> Optional[BatchJobMetadata]:
    path_segments = urlparse(url).path.split("/")

    if len(path_segments) < 3:
        return None

    jobs_position_segment, job_id, results_position_segment = path_segments[-3:]
    if jobs_position_segment != "jobs" or results_position_segment != "results":
        return None

    try:
        return batch_jobs.get_job_info(job_id=job_id, user_id=user_id)
    except JobNotFoundException:
        logger.debug(f"job {job_id} does not belong to current user {user_id}", exc_info=True)
        return None


def _await_dependency_job(
    url: str,
    *,
    user: Optional[User] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    poll_interval_seconds: float,
    max_poll_delay_seconds: float,
    max_poll_time: float,
) -> Optional[BatchJobMetadata]:
    def get_dependency_job_info() -> Optional[BatchJobMetadata]:
        return extract_own_job_info(url, user.user_id, batch_jobs) if user and batch_jobs else None

    dependency_job_info = get_dependency_job_info()
    if not dependency_job_info:
        return None

    logger.info(f"load_stac of results of own job {dependency_job_info.id}")

    while True:
        partial_job_status = PARTIAL_JOB_STATUS.for_job_status(dependency_job_info.status)

        logger.debug(f"OpenEO batch job results status of own job {dependency_job_info.id}: {partial_job_status}")

        if partial_job_status in [PARTIAL_JOB_STATUS.ERROR, PARTIAL_JOB_STATUS.CANCELED]:
            logger.error(f"Failing because own OpenEO batch job {dependency_job_info.id} failed")
        elif partial_job_status in [None, PARTIAL_JOB_STATUS.FINISHED]:
            break  # not a partial job result or success: proceed

        # still running: continue polling
        if time.time() >= max_poll_time:
            max_poll_delay_reached_error = (
                f"OpenEO batch job results dependency of"
                f"own job {dependency_job_info.id} was not satisfied after"
                f" {max_poll_delay_seconds} s, aborting"
            )

            raise Exception(max_poll_delay_reached_error)

        time.sleep(poll_interval_seconds)

        dependency_job_info = get_dependency_job_info()

    return dependency_job_info


def _await_stac_object(
    url: str,
    *,
    poll_interval_seconds: float,
    max_poll_delay_seconds: float,
    max_poll_time: float,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
) -> STACObject:
    if stac_io is None:
        session = requests_with_retry(total=5, backoff_factor=0.1, status_forcelist={500, 502, 503, 504})
        stac_io = ResilientStacIO(session)

    while True:
        stac_object = pystac.read_file(href=url, stac_io=stac_io)

        if isinstance(stac_object, pystac.Catalog):
            stac_object._stac_io = stac_io  # TODO: avoid accessing internals (fix pystac)

        partial_job_status = stac_object.to_dict(include_self_link=False, transform_hrefs=False).get("openeo:status")

        logger.debug(f"OpenEO batch job results status of {url}: {partial_job_status}")

        if partial_job_status in [PARTIAL_JOB_STATUS.ERROR, PARTIAL_JOB_STATUS.CANCELED]:
            logger.error(f"Failing because OpenEO batch job with results at {url} failed")
        elif partial_job_status in [None, PARTIAL_JOB_STATUS.FINISHED]:
            break  # not a partial job result or success: proceed

        # still running: continue polling
        if time.time() >= max_poll_time:
            max_poll_delay_reached_error = (
                f"OpenEO batch job results dependency at {url} was not satisfied after"
                f" {max_poll_delay_seconds} s, aborting"
            )

            raise Exception(max_poll_delay_reached_error)

        time.sleep(poll_interval_seconds)

    return stac_object


class PropertyFilter:
    """
    Container for STAC object property filters declared as process graphs
    (e.g. like the `properties` argument of `load_collection`/`load_stac` processes).

    :param properties: mapping of property names to the desired conditions
        expressed as openEO-style process graphs (flat graph)
    :param env: optional evaluation environment,
        e.g. with extra parameters to consider when evaluating the process graphs
    """

    # TODO: move this utility to a more generic location for better reuse

    def __init__(self, properties: PropertyFilterPGMap, *, env: Optional[EvalEnv] = None):
        self._properties = properties
        self._env = env or EvalEnv()

    @staticmethod
    def _build_callable(operator: str, value: Any) -> Callable[[Any], bool]:
        if operator == "eq":
            return lambda actual: actual == value
        elif operator == "lte":
            return lambda actual: actual is not None and actual <= value
        elif operator == "gte":
            return lambda actual: actual is not None and value <= actual
        elif operator == "in":
            return lambda actual: actual is not None and actual in value
        else:
            # TODO: support more operators?
            raise ValueError(f"Unsupported operator: {operator}")

    def build_matcher(self) -> Callable[[Dict[str, Any]], bool]:
        """
        Build an evaluating function (a closure)
        that can be used to check if properties match the filter conditions.
        """
        conditions = [
            (name, self._build_callable(operator, value))
            for name, pg in self._properties.items()
            for operator, value in filter_properties.extract_literal_match(pg, env=self._env).items()
        ]

        def match(properties: Dict[str, Any]) -> bool:
            return all(name in properties and condition(properties[name]) for name, condition in conditions)

        return match

    def to_cql2_filter(
        self,
        *,
        use_filter_extension: Union[bool, str],
        client: pystac_client.Client,
    ) -> Union[str, dict, None]:
        # TODO: the strong coupling between GET+CQL2-text and POST+CQL2-JSON is a bit off here:
        #       per [STAC API filter spec](https://github.com/stac-api-extensions/filter?tab=readme-ov-file#get-query-parameters-and-post-json-fields)
        #       GET can use both CQL2 text and JSON, but POST should only use JSON.
        #       Method and CQL2 format should ideally be decoupled.
        if use_filter_extension == "cql2-json":  # force POST JSON
            return self.to_cql2_json()
        elif use_filter_extension == "cql2-text":  # force GET text
            return self.to_cql2_text()
        elif use_filter_extension == True:  # auto-detect, favor POST
            # TODO: CQL2 format detection should be done through conformance classes instead of link rels
            #      also see https://github.com/stac-api-extensions/filter?tab=readme-ov-file#get-query-parameters-and-post-json-fields
            search_links = client.get_links(rel="search")
            supports_post_search = any(link.extra_fields.get("method") == "POST" for link in search_links)
            if supports_post_search:
                return self.to_cql2_json()
            else:
                # assume serves ignores filter if no "search" method advertised
                return self.to_cql2_text()
        elif use_filter_extension == False:
            return None  # explicitly disabled
        else:
            raise ValueError(f"Invalid use-filter-extension value: {use_filter_extension!r}")

    def to_cql2_text(self) -> str:
        """Convert the property filter to a CQL2 text representation."""
        literal_matches = {
            property_name: filter_properties.extract_literal_match(condition, self._env)
            for property_name, condition in self._properties.items()
        }
        cql2_text_formatter = get_jvm().org.openeo.geotrellissentinelhub.Cql2TextFormatter()

        return cql2_text_formatter.format(
            # Cql2TextFormatter won't add necessary quotes so provide them up front
            # TODO: are these quotes actually necessary?
            {f'"properties.{name}"': criteria for name, criteria in literal_matches.items()}
        )

    def to_cql2_json(self) -> Union[Dict, None]:
        literal_matches = {
            property_name: filter_properties.extract_literal_match(condition, self._env)
            for property_name, condition in self._properties.items()
        }
        if len(literal_matches) == 0:
            return None

        operator_mapping = {
            "eq": "=",
            "neq": "<>",
            "lt": "<",
            "lte": "<=",
            "gt": ">",
            "gte": ">=",
            "in": "in",
        }

        def single_filter(property, operator, value) -> dict:
            cql2_json_operator = operator_mapping.get(operator)

            if cql2_json_operator is None:
                raise ValueError(f"unsupported operator {operator}")

            return {"op": cql2_json_operator, "args": [{"property": f"properties.{property}"}, value]}

        filters = [
            single_filter(property, operator, value)
            for property, criteria in literal_matches.items()
            for operator, value in criteria.items()
        ]

        if len(filters) == 1:
            return filters[0]

        return {
            "op": "and",
            "args": filters,
        }


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
