from __future__ import annotations

import datetime
import logging
import time
import re
from functools import partial
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union, Iterable
from urllib.parse import urlparse

import planetary_computer
import pystac
import pystac.stac_io
import pystac.utils
import pystac_client
import pystac_client.stac_api_io
import requests.adapters
import shapely.geometry
from pystac import STACObject
from urllib3 import Retry

import openeo_driver.backend
from openeo.metadata import _StacMetadataParser
from openeo.util import Rfc3339, dict_no_none
from openeo_driver import filter_properties
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import (
    JobNotFoundException,
    ProcessParameterUnsupportedException,
)
from openeo_driver.jobregistry import PARTIAL_JOB_STATUS
from openeo_driver.users import User
from openeo_driver.util.http import requests_with_retry
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata
from openeogeotrellis.integrations.stac import ResilientStacIO
from openeogeotrellis.stac.exceptions import LoadStacException
from openeogeotrellis.utils import get_jvm

logger = logging.getLogger(__name__)
REQUESTS_TIMEOUT_SECONDS = 60


# Some type aliases related to property filters expressed as process graphs
# (e.g. like the `properties` argument of `load_collection`/`load_stac` processes).
FlatProcessGraph = Dict[str, dict]
PropertyFilterPGMap = Dict[str, FlatProcessGraph]


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

    def __init__(self, properties: PropertyFilterPGMap, *, env: Optional[StacEvalEnv] = None):
        self._properties = properties
        self._env = env or StacEvalEnv()

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
        # TODO: is it possible to eliminate the need for this parameter?
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
            query_datetime = (
                None
                if spatiotemporal_extent.temporal_extent.is_unbounded() or skip_datetime_filter
                else spatiotemporal_extent.temporal_extent.as_tuple()
            )
            search_request = client.search(
                method=method,
                collections=collection.id,
                bbox=bbox,
                limit=20,
                datetime=query_datetime,
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


    def iter_items_with_band_assets(self) -> Iterator[Tuple[pystac.Item, Dict[str, pystac.Asset]]]:
        """Iterate over items along with their band assets only."""
        for item in self.items:
            band_assets = {asset_id: asset for asset_id, asset in sorted(item.assets.items()) if _is_band_asset(asset)}
            if band_assets:
                yield item, band_assets


def construct_item_collection(
    url: str,
    *,
    spatiotemporal_extent: Optional[_SpatioTemporalExtent] = None,
    property_filter_pg_map: Optional[PropertyFilterPGMap] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    env: Optional[StacEvalEnv] = None,
    feature_flags: Optional[StacFeatureFlags] = None,
    stac_io: Optional[pystac.stac_io.StacIO] = None,
    user: Optional[User] = None,
) -> Tuple[ItemCollection, GeopysparkCubeMetadata, List[str], bool]:
    """
    Construct Stac ItemCollection from given load_stac URL
    """
    spatiotemporal_extent = spatiotemporal_extent or _SpatioTemporalExtent()
    property_filter_pg_map = property_filter_pg_map or {}
    env = env or StacEvalEnv()
    feature_flags = feature_flags or StacFeatureFlags()

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
            netcdf_with_time_dimension = _contains_netcdf_with_time_dimension(collection)
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
                use_filter_extension=feature_flags.use_filter_extension,
                # TODO #1312 why skipping datetime filter especially for netcdf with time dimension?
                skip_datetime_filter=netcdf_with_time_dimension,
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
                netcdf_with_time_dimension = _contains_netcdf_with_time_dimension(collection=catalog)

            band_names = stac_metadata_parser.bands_from_stac_object(obj=stac_object).band_names()

            item_collection = ItemCollection.from_stac_catalog(catalog, spatiotemporal_extent=spatiotemporal_extent)

    # Deduplicate items
    # TODO: smarter and more fine-grained deduplication behavior?
    #       - enable by default or only do it on STAC API usage?
    #       - allow custom deduplicators (e.g. based on layer catalog info about openeo collections)
    if feature_flags.deduplicate_items:
        item_collection = item_collection.deduplicated(deduplicator=ItemDeduplicator())

    # TODO: possible to embed band names in metadata directly?
    return item_collection, metadata, band_names, netcdf_with_time_dimension


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


def _supports_item_search(collection: pystac.Collection) -> bool:
    # TODO: use pystac_client instead?
    catalog = collection.get_root()
    if catalog:
        conforms_to = catalog.extra_fields.get("conformsTo", [])
        return any(re.match(r"^https://api\.stacspec\.org/v1\..*/item-search$", c) for c in conforms_to)
    return False


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


def _contains_netcdf_with_time_dimension(collection: pystac.Collection) -> bool:
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
