"""
Item collection construction for load_stac.

Given a resolved STAC object (an Item, Collection, or Catalog) or an own-job
dependency, collects and filters STAC Items into an `ItemCollection` for
per-item/per-asset analysis.

Covers:
- ItemCollection: the container of collected Items and its factory methods
  (from a single Item, a static Catalog, a STAC API, or a dependency batch job)
- StacSourceResolver / LiveStacSourceResolver / OwnJobStacSourceResolver:
  resolving a load_stac `url` to either a live STAC object or a pre-built
  own-job `ItemCollection`
- construct_item_collection: the top-level function orchestrating STAC-object
  fetching → routing → item collection for all STAC object types

Property filtering (`PropertyFilter`/`AdaptingPropertyFilter`) and
deduplication (`ItemDeduplicator`) live in their own modules
(`openeogeotrellis.stac.property_filter`, `openeogeotrellis.stac.item_deduplicator`).
"""
from __future__ import annotations

import datetime
import logging
import re
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Protocol, Sequence, Tuple, Union

import pystac
import pystac_client
import pystac_client.stac_api_io
from openeo.metadata import _StacMetadataParser
from openeo.util import Rfc3339, dict_no_none, TimingLogger
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import (
    ProcessParameterUnsupportedException,
)
from openeo_driver.users import User
from openeo_driver.utils import EvalEnv
from urllib3 import Retry

import openeo_driver.backend
from openeogeotrellis.constants import EVAL_ENV_KEY, STAC_API_FILTER_BY_GEOMETRY_DEFAULT
from openeogeotrellis.integrations.stac import CompactJsonStacIO, LoggingStacApiIO
from openeogeotrellis.stac.assets import is_band_asset
from openeogeotrellis.stac.exceptions import LoadStacException
from openeogeotrellis.stac.extents import (
    SpatialFilteringGeometries,
    SpatioTemporalExtent,
    get_item_temporal_extent,
)
from openeogeotrellis.stac.item_deduplicator import ItemDeduplicator, deduplicator_from_feature_flags
from openeogeotrellis.stac.own_job import await_dependency_job
from openeogeotrellis.stac.property_filter import AdaptingPropertyFilter, PropertyFilter, PropertyFilterPGMap
from openeogeotrellis.stac.stac_object_fetching import (
    STAC_API_BACKOFF_FACTOR,
    STAC_API_RETRY_TOTAL,
    REQUESTS_TIMEOUT_SECONDS,
    PollingConfig,
    JitteredRetry,
    await_stac_object,
)
from openeogeotrellis.util.logging import TrackingIter

logger = logging.getLogger(__name__)

STAC_API_PER_PAGE_LIMIT_DEFAULT = 100
STAC_API_MAX_ITEMS_DEFAULT = 5000

# TODO: change default to False (make post-query property filtering an opt-in feature instead of opt-out)?
POST_QUERY_PROPERTY_FILTERING_DEFAULT = True


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


def _pystac_item_from_dict_lenient(item: dict) -> pystac.Item:
    """
    Lenient variant of pystac.Item.from_dict
    that skips bad assets (without href) instead of raising exception
    """
    assets = item.get("assets") or {}
    bad_assets = [k for k, v in assets.items() if not isinstance(v,dict) or not v.get("href")]
    if bad_assets:
        logger.warning(
            f"ItemCollection: dropping {len(bad_assets)} asset(s) "
            f"without 'href' from item {item.get('id')!r}: {bad_assets}"
        )
        # Shallow copy with rewritten assets
        item = dict(item, assets={k: v for k, v in assets.items() if k not in bad_assets})
    return pystac.Item.from_dict(item, migrate=False, preserve_dict=False)


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
    def from_stac_item(item: pystac.Item, *, spatiotemporal_extent: SpatioTemporalExtent) -> "ItemCollection":
        items = [item] if spatiotemporal_extent.item_intersects(item) else []
        return ItemCollection(items)

    @staticmethod
    def from_own_job(
        job: BatchJobMetadata,
        *,
        spatiotemporal_extent: SpatioTemporalExtent,
        batch_jobs: openeo_driver.backend.BatchJobs,
        user: Optional[User],
    ) -> "ItemCollection":
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
    def from_stac_catalog(catalog: pystac.Catalog, *, spatiotemporal_extent: SpatioTemporalExtent) -> "ItemCollection":
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
        spatiotemporal_extent: SpatioTemporalExtent,
        use_filter_extension: Union[bool, str] = True,
        # TODO: is it possible to eliminate the need for this parameter?
        skip_datetime_filter: bool = False,
        original_url: str = "n/a",
        per_page_limit: int = STAC_API_PER_PAGE_LIMIT_DEFAULT,
        max_items: Union[int, None] = STAC_API_MAX_ITEMS_DEFAULT,
        filter_by_geometry: bool = False,
        spatial_filtering_geometries: Union[SpatialFilteringGeometries, None] = None,
        post_query_property_filtering: bool = POST_QUERY_PROPERTY_FILTERING_DEFAULT,
    ) -> "ItemCollection":
        root_catalog = collection.get_root()

        # TODO: avoid hardcoded domain sniffing. Possible to discover capabilities in some way?
        # TODO: still necessary to handle `fields` here? It's apparently always the same.
        if root_catalog.get_self_href().startswith("https://planetarycomputer.microsoft.com/api/stac/v1"):
            import planetary_computer
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

        retry = JitteredRetry(
            total=STAC_API_RETRY_TOTAL,
            backoff_factor=STAC_API_BACKOFF_FACTOR,
            status_forcelist=frozenset([429, 500, 502, 503, 504]),
            allowed_methods=Retry.DEFAULT_ALLOWED_METHODS.union({"POST"}),
            raise_on_status=False,  # otherwise StacApiIO will catch this and lose the response body
        )
        query_info = ""
        try:
            stac_io = LoggingStacApiIO(timeout=REQUESTS_TIMEOUT_SECONDS, max_retries=retry)
            client = pystac_client.Client.open(root_catalog.get_self_href(), modifier=modifier, stac_io=stac_io)

            cql2_filter = property_filter.to_cql2_filter(
                client=client,
                use_filter_extension=use_filter_extension,
            )
            method = "POST" if isinstance(cql2_filter, dict) else "GET"
            query_info += f" {use_filter_extension=} {cql2_filter=}"

            bbox = spatiotemporal_extent.spatial_extent.as_bbox(crs="EPSG:4326")
            if bbox is None:
                query_bboxes = [None]
            elif bbox.cyclic_antimeridian_crossing():
                # TODO: proper antimeridian handling should be supported directly by a STAC API
                #       (https://github.com/radiantearth/stac-api-spec/issues/473)
                #       but unfortunately that isn't the case in the CDSE pgSTAC deployment (CDSE-2834) and maybe others.
                #       Can we at some point eliminate this hack to split the query bounding box
                #       and all the additional housekeeping overhead that comes with it?
                query_bboxes = [b.as_wsen_tuple() for b in bbox.cyclic_antimeridian_split()]
                logger.warning(
                    f"Query across the antimeridian, which should be supported transparently by a STAC API, but some implementations don't, so we split up the query for now: {query_bboxes=}"
                )
            else:
                query_bboxes = [bbox.as_wsen_tuple()]

            intersects_geometry = None
            if filter_by_geometry and spatial_filtering_geometries:
                # Include geometry filtering already in STAC API query
                intersects_geometry = spatial_filtering_geometries.get_simplified_geojson()

            # Note that per STAC API spec, "Only one of either `intersects` or `bbox` may be specified"
            if intersects_geometry:
                query_bboxes = [None]

            query_datetime = (
                None
                if spatiotemporal_extent.temporal_extent.is_unbounded() or skip_datetime_filter
                else spatiotemporal_extent.temporal_extent.as_tuple()
            )

            # STAC API might not support Filter Extension so always do post-process filtering as well
            # TODO: check "filter" conformance class for this instead of blindly trying to do double work
            #       see https://github.com/stac-api-extensions/filter
            if post_query_property_filtering:
                # Support various forms of finetuning the post-query filter
                if isinstance(post_query_property_filtering, dict):
                    # Take property subset with allow and deny list
                    post_query_property_match = property_filter.subsetted(
                        allow=post_query_property_filtering.get("allow"),
                        deny=post_query_property_filtering.get("deny"),
                    ).build_matcher()
                elif isinstance(post_query_property_filtering, list):
                    # Use as provided property allow list
                    post_query_property_match = property_filter.subsetted(
                        allow=post_query_property_filtering
                    ).build_matcher()
                else:
                    # Use full property filter
                    post_query_property_match = property_filter.build_matcher()
            else:
                post_query_property_match = lambda properties: True

            # Set of item ids for on the fly deduplication (when we have to do two queries around the antimeridian)
            seen_item_ids = set()

            items = []
            for query_bbox in query_bboxes:
                search_request = client.search(
                    method=method,
                    collections=collection.id,
                    bbox=query_bbox,
                    intersects=intersects_geometry,
                    max_items=max_items,
                    limit=per_page_limit,
                    datetime=query_datetime,
                    filter=cql2_filter,
                    fields=fields,
                )
                if search_request.method == "GET":
                    query_info += f" {search_request.method} {search_request.url_with_parameters()}"
                else:
                    query_info += f" {search_request.method} {search_request.url} {search_request.get_parameters()=}"
                logger.info(f"ItemCollection.from_stac_api: STAC API request: {query_info}")

                items_from_query: Iterator[pystac.Item] = (
                    _pystac_item_from_dict_lenient(item) for item in search_request.items_as_dicts()
                )

                tracking_iter_raw = TrackingIter()
                tracking_iter_filtered = TrackingIter()
                items.extend(
                    tracking_iter_filtered(
                        item
                        for item in tracking_iter_raw(items_from_query)
                        if post_query_property_match(item.properties) and item.id not in seen_item_ids
                        # TODO also do filtering with spatial_filtering_geometries here?
                    )
                )
                logger.info(f"ItemCollection.from_stac_api: {tracking_iter_raw=!s} {tracking_iter_filtered=!s}")
                if max_items and tracking_iter_raw.count >= max_items:
                    logger.warning(
                        f"ItemCollection.from_stac_api: reached {max_items=}: {tracking_iter_raw!s}, item collection is probably incomplete"
                    )

                if len(query_bboxes) > 1:
                    # Only track seen items when we're going to check for duplicates (multiple query bboxes)
                    seen_item_ids.update(item.id for item in items)

            logger.info(f"ItemCollection.from_stac_api: Collected {len(items)} items (from {query_bboxes=})")
        except Exception as e:
            raise LoadStacException(
                url=original_url, info=f"failed to construct ItemCollection from STAC API. {query_info=} {e=}"
            ) from e

        return ItemCollection(items)

    def get_temporal_extent(self) -> Tuple[Union[datetime.datetime, None], Union[datetime.datetime, None]]:
        """Get overall temporal extent of all items in the collection."""
        start = None
        end = None
        for item in self.items:
            item_start, item_end = get_item_temporal_extent(item=item)
            if not start or item_start < start:
                start = item_start
            if not end or item_end > end:
                end = item_end
        return start, end

    def deduplicated(self, deduplicator: ItemDeduplicator) -> "ItemCollection":
        """Create new ItemCollection by deduplicating items using the given deduplicator."""
        orig_count = len(self.items)
        logger.info(f"ItemCollection.deduplicated: deduplicating {orig_count} items using {deduplicator=}")
        items = deduplicator.deduplicate(items=self.items)
        logger.info(f"ItemCollection.deduplicated: from {orig_count} to {len(items)} items")
        return ItemCollection(items=items)

    def iter_items_with_band_assets(self) -> Iterator[Tuple[pystac.Item, Dict[str, pystac.Asset]]]:
        """Iterate over items along with their band assets only."""
        for item in self.items:
            band_assets = {asset_id: asset for asset_id, asset in sorted(item.assets.items()) if is_band_asset(asset)}
            if band_assets:
                yield item, band_assets

    def to_file(self, path: Union[str, Path], stac_io: Optional[pystac.StacIO] = None) -> None:
        """Serialize item collection to a JSON file."""
        pystac_item_collection = pystac.item_collection.ItemCollection(items=self.items)
        # TODO: performance aspects and file size of JSON serialization of large item collections?
        # Use compact JSON by default
        pystac_item_collection.save_object(dest_href=str(path), stac_io=stac_io or CompactJsonStacIO())

    @classmethod
    def from_file(cls, path: Union[str, Path], stac_io: Optional[pystac.StacIO] = None) -> "ItemCollection":
        """Deserialize an item collection from a JSON file."""
        pystac_item_collection = pystac.item_collection.ItemCollection.from_file(href=str(path), stac_io=stac_io)
        return cls(items=pystac_item_collection.items)


@dataclass(frozen=True)
class StacResolution:
    """
    What a source resolver produced: either a STAC object that still needs
    routing/collecting/filtering, or an already-collected set of Items
    (own-job dependency case, bypassing STAC fetching and property filtering
    entirely). Exactly one of the two fields is set.
    """

    stac_object: Optional[Union[pystac.Item, pystac.Collection, pystac.Catalog]] = None
    item_collection: Optional["ItemCollection"] = None

    def __post_init__(self):
        if (self.stac_object is None) == (self.item_collection is None):
            raise ValueError("StacResolution needs exactly one of stac_object / item_collection")


class StacSourceResolver(Protocol):
    """
    Resolves a load_stac `url` to either a live STAC object or a pre-built
    `ItemCollection`. Has two concrete implementations: `LiveStacSourceResolver`
    (fetches/polls the URL as a live STAC object) and `OwnJobStacSourceResolver`
    (resolves an own-job dependency directly into an `ItemCollection`,
    bypassing STAC fetching and property filtering).
    """

    def resolve(self, url: str, *, spatiotemporal_extent: SpatioTemporalExtent) -> Optional[StacResolution]: ...


class LiveStacSourceResolver:
    """Resolves `url` by fetching/polling it as a live STAC object (Item/Collection/Catalog)."""

    def __init__(self, *, stac_io: Optional[pystac.stac_io.StacIO] = None, polling: Optional[PollingConfig] = None):
        self._stac_io = stac_io
        self._polling = polling or PollingConfig.from_backend_config()

    def resolve(self, url: str, *, spatiotemporal_extent: SpatioTemporalExtent) -> StacResolution:
        logger.info(f"LiveStacSourceResolver: fetching STAC object from {url=} {spatiotemporal_extent=}")
        stac_object = await_stac_object(
            url=url,
            poll_interval_seconds=self._polling.poll_interval_seconds,
            max_poll_delay_seconds=self._polling.max_poll_delay_seconds,
            max_poll_time=self._polling.deadline(),
            stac_io=self._stac_io,
        )
        return StacResolution(stac_object=stac_object)


class OwnJobStacSourceResolver:
    """
    Resolves `url` by checking whether it points at a sibling batch job of
    `user` (own-job dependency), polling that job to completion, and building
    an `ItemCollection` directly from its result assets — bypassing STAC
    fetching and property filtering entirely.

    Returns `None` from `resolve()` when `url` does not point at an own job
    (e.g. it's a plain STAC API/catalog URL).
    """

    def __init__(
        self,
        *,
        user: User,
        batch_jobs: openeo_driver.backend.BatchJobs,
        polling: Optional[PollingConfig] = None,
    ):
        self._user = user
        self._batch_jobs = batch_jobs
        self._polling = polling or PollingConfig.from_backend_config()

    def resolve(self, url: str, *, spatiotemporal_extent: SpatioTemporalExtent) -> Optional[StacResolution]:
        dependency_job_info = await_dependency_job(
            url=url,
            user=self._user,
            batch_jobs=self._batch_jobs,
            poll_interval_seconds=self._polling.poll_interval_seconds,
            max_poll_delay_seconds=self._polling.max_poll_delay_seconds,
            max_poll_time=self._polling.deadline(),
        )
        if not dependency_job_info:
            return None

        logger.info(f"OwnJobStacSourceResolver: loading from dependency job {dependency_job_info.id!r}")
        item_collection = ItemCollection.from_own_job(
            job=dependency_job_info,
            spatiotemporal_extent=spatiotemporal_extent,
            batch_jobs=self._batch_jobs,
            user=self._user,
        )
        return StacResolution(item_collection=item_collection)


def _default_source_resolvers(
    *,
    user: Optional[User],
    batch_jobs: Optional[openeo_driver.backend.BatchJobs],
    stac_io: Optional[pystac.stac_io.StacIO],
) -> List[StacSourceResolver]:
    """
    Own-job resolution is tried first: a URL pointing at one of the user's own
    batch jobs is served from that job's results, without fetching it over HTTP.
    """
    resolvers: List[StacSourceResolver] = []
    if user and batch_jobs:
        resolvers.append(OwnJobStacSourceResolver(user=user, batch_jobs=batch_jobs))
    resolvers.append(LiveStacSourceResolver(stac_io=stac_io))
    return resolvers


@dataclass(frozen=True)
class StacSource:
    """
    A STAC source resolved down to the Items it contains, plus the collection-level
    metadata needed to describe the resulting cube.
    """

    item_collection: "ItemCollection"
    #: Raw STAC Collection/Catalog JSON; empty for a single Item or an own-job source.
    collection_summary: dict
    #: Band names detected in STAC metadata; fallback when the user selects no bands.
    band_names: List[str]
    #: Whether the source stores netCDF assets carrying their own time dimension.
    netcdf_with_time_dimension: bool


def construct_item_collection(
    url: str,
    *,
    spatiotemporal_extent: Optional[SpatioTemporalExtent] = None,
    property_filter_pg_map: Optional[PropertyFilterPGMap] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    env: Optional[EvalEnv] = None,
    feature_flags: Optional[Dict[str, Any]] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    user: Optional[User] = None,
    spatial_filtering_geometries: Union[SpatialFilteringGeometries, None] = None,
    source_resolvers: Optional[Sequence[StacSourceResolver]] = None,
) -> StacSource:
    """
    Construct a `StacSource` (an `ItemCollection` plus its collection-level metadata)
    from a given load_stac URL.

    It is up to the caller to turn `collection_summary` into a `GeopysparkCubeMetadata`
    (or equivalent) — this module intentionally has no engine-specific metadata coupling.
    """
    spatiotemporal_extent = spatiotemporal_extent or SpatioTemporalExtent()
    property_filter_pg_map = property_filter_pg_map or {}
    env = env or EvalEnv()
    feature_flags = feature_flags or {}

    netcdf_with_time_dimension = False

    if source_resolvers is None:
        source_resolvers = _default_source_resolvers(user=user, batch_jobs=batch_jobs, stac_io=stac_io)

    # Own-job URLs (when tried) are served from the sibling job's results directly,
    # bypassing STAC fetching and property filtering; anything else falls through
    # to the live STAC source resolver.
    for resolver in source_resolvers:
        resolution = resolver.resolve(url, spatiotemporal_extent=spatiotemporal_extent)
        if resolution is not None:
            break
    else:
        raise LoadStacException(url=url, info="no STAC source resolver could resolve this URL")

    stac_metadata_parser = _StacMetadataParser(logger=logger)

    if resolution.item_collection is not None:
        # TODO: improve metadata for this case
        collection_summary: dict = {}
        item_collection = resolution.item_collection
        # TODO: improve band name detection for this case
        band_names = []
    else:
        stac_object = resolution.stac_object
        logger.info(f"construct_item_collection: got {type(stac_object).__name__} {stac_object.id!r}")

        if isinstance(stac_object, pystac.Item):
            if property_filter_pg_map:
                # as dictated by the load_stac spec
                # TODO: it's not that simple see https://github.com/Open-EO/openeo-processes/issues/536 and https://github.com/Open-EO/openeo-processes/pull/547
                raise ProcessParameterUnsupportedException(process="load_stac", parameter="properties")

            item = stac_object
            # TODO: improve metadata for this case
            collection_summary = {}
            band_names = stac_metadata_parser.bands_from_stac_item(item=item).band_names()
            item_collection = ItemCollection.from_stac_item(item=item, spatiotemporal_extent=spatiotemporal_extent)
            logger.info(f"construct_item_collection: single Item, {band_names=}, collected {len(item_collection.items)} item(s)")
        elif isinstance(stac_object, pystac.Collection) and _supports_item_search(stac_object):
            collection = stac_object
            netcdf_with_time_dimension = contains_netcdf_with_time_dimension(collection)

            # TODO: remove workaround for "alternate:name": "S3" in band summary
            #  (https://github.com/eu-cdse/openeo-cdse-infra/issues/644)
            collection_dict = collection.to_dict(include_self_link=False, transform_hrefs=False)
            for band in collection_dict.get("summaries", {}).get("bands", []):
                if not "name" in band and "alternate:name" in band:
                    band["name"] = band["alternate:name"]

            collection_summary = collection_dict

            band_names = stac_metadata_parser.bands_from_stac_collection(collection=collection).band_names()
            logger.info(f"construct_item_collection: STAC API Collection {collection.id!r}, {band_names=}, {netcdf_with_time_dimension=}")

            # TODO: _experimental_properties_prefix is just a temporary feature flag to allow easy fall back to old behavior.
            #       Ideally however, this prefix stuff should just be dropped #1584
            properties_prefix = feature_flags.get("_experimental_properties_prefix", "")
            property_filter = PropertyFilter(
                properties=property_filter_pg_map, env=env, properties_prefix=properties_prefix
            )
            if property_filter_adaptations := feature_flags.get("property_filter_adaptations"):
                logger.debug(f"AdaptingPropertyFilter with {property_filter_adaptations=}")
                property_filter = AdaptingPropertyFilter(
                    properties=property_filter_pg_map,
                    env=env,
                    adaptations=property_filter_adaptations,
                    properties_prefix=properties_prefix,
                )

            stac_api_filter_by_geometry_default: bool = env.get(
                EVAL_ENV_KEY.STAC_API_FILTER_BY_GEOMETRY, default=STAC_API_FILTER_BY_GEOMETRY_DEFAULT
            )

            with TimingLogger(title=f"ItemCollection.from_stac_api from {url=}", logger=logger.info):
                item_collection = ItemCollection.from_stac_api(
                    collection=stac_object,
                    original_url=url,
                    property_filter=property_filter,
                    spatiotemporal_extent=spatiotemporal_extent,
                    use_filter_extension=feature_flags.get("use-filter-extension", True),
                    # TODO #1312 why skipping datetime filter especially for netcdf with time dimension?
                    skip_datetime_filter=netcdf_with_time_dimension,
                    per_page_limit=feature_flags.get("stac_api_per_page_limit", STAC_API_PER_PAGE_LIMIT_DEFAULT),
                    max_items=feature_flags.get("stac_api_max_items", STAC_API_MAX_ITEMS_DEFAULT),
                    filter_by_geometry=feature_flags.get(
                        "stac_api_filter_by_geometry", stac_api_filter_by_geometry_default
                    ),
                    spatial_filtering_geometries=spatial_filtering_geometries,
                    post_query_property_filtering=feature_flags.get(
                        "post_query_property_filtering", POST_QUERY_PROPERTY_FILTERING_DEFAULT
                    ),
                )
        else:
            assert isinstance(stac_object, pystac.Catalog)  # static Catalog + Collection
            catalog = stac_object
            collection_summary = catalog.to_dict(include_self_link=False, transform_hrefs=False)

            if property_filter_pg_map:
                # as dictated by the load_stac spec
                # TODO: it's not that simple see https://github.com/Open-EO/openeo-processes/issues/536 and https://github.com/Open-EO/openeo-processes/pull/547
                raise ProcessParameterUnsupportedException(process="load_stac", parameter="properties")

            if isinstance(catalog, pystac.Collection):
                netcdf_with_time_dimension = contains_netcdf_with_time_dimension(collection=catalog)

            band_names = stac_metadata_parser.bands_from_stac_object(obj=stac_object).band_names()
            logger.info(f"construct_item_collection: static Catalog {catalog.id!r}, {band_names=}, {netcdf_with_time_dimension=}")

            with TimingLogger(title=f"ItemCollection.from_stac_catalog from {url=}", logger=logger.info):
                item_collection = ItemCollection.from_stac_catalog(catalog, spatiotemporal_extent=spatiotemporal_extent)

    logger.info(f"construct_item_collection: collected {len(item_collection.items)} items")

    # Deduplicate items
    # TODO: smarter and more fine-grained deduplication behavior?
    #       - enable by default or only do it on STAC API usage?
    if deduplicator := deduplicator_from_feature_flags(feature_flags=feature_flags, id=url):
        item_collection = item_collection.deduplicated(deduplicator=deduplicator)

    return StacSource(
        item_collection=item_collection,
        collection_summary=collection_summary,
        band_names=band_names,
        netcdf_with_time_dimension=netcdf_with_time_dimension,
    )
