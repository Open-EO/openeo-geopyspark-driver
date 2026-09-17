"""
Spatio-temporal extent helpers for load_stac.

Parses and structures the spatial/temporal filtering extents from user-provided
load_params, to be used for filtering STAC entities (Items, Collections, Catalogs).
"""
from __future__ import annotations

import datetime
import logging
from typing import Dict, List, Optional, Tuple, Union

import geopandas
import pystac
import pystac.utils
import shapely.geometry

from openeo_driver.datacube import DriverVectorCube
from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.util.datetime import DateTimeLikeOrNone, to_datetime_utc_unless_none
from openeogeotrellis.util.geometry import GeometrySimplifier

logger = logging.getLogger(__name__)


class TemporalExtent:
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
    # TODO: re-implement in dataclasses/attrs to better enforce immutability and simplify equality/hash implementation

    __slots__ = ("_from_date", "_to_date")

    def __init__(self, from_date: DateTimeLikeOrNone, to_date: DateTimeLikeOrNone):
        self._from_date: Union[datetime.datetime, None] = to_datetime_utc_unless_none(from_date)
        self._to_date: Union[datetime.datetime, None] = to_datetime_utc_unless_none(to_date)

    @property
    def from_date(self) -> Union[datetime.datetime, None]:
        return self._from_date

    @property
    def to_date(self) -> Union[datetime.datetime, None]:
        return self._to_date

    def _key(self) -> tuple:
        return (self.from_date, self.to_date)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        if isinstance(other, TemporalExtent):
            return self._key() == other._key()
        return NotImplemented

    @classmethod
    def from_load_param_extent(cls, extent: Tuple[DateTimeLikeOrNone, DateTimeLikeOrNone]) -> TemporalExtent:
        """
        Create from openEO load_collection/load_stac-style temporal extent, considering:
        - given as end-exclusive, per openEO convention
        - possibly given in legacy (but invalid) way, where from and end date/day are identical,
          which should be interpreted as a single-day extent
        """
        (from_date, until_date) = (to_datetime_utc_unless_none(d) for d in extent)
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
            # TODO: isn't it just more transparant/consistent to keep working with end-exclusive definition instead of subtracting 1 millisecond here?
            to_date = until_date - datetime.timedelta(milliseconds=1)
        return cls(from_date=from_date, to_date=to_date)

    def as_tuple(self) -> Tuple[Union[datetime.datetime, None], Union[datetime.datetime, None]]:
        return self.from_date, self.to_date

    def isoformat(self) -> Tuple[Union[str, None], Union[str, None]]:
        return (
            self.from_date.isoformat() if self.from_date else None,
            self.to_date.isoformat() if self.to_date else None,
        )

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
    # TODO: enforce/ensure immutability
    # TODO: re-implement in dataclasses/attrs to better enforce immutability and simplify equality/hash implementation

    __slots__ = ("_bbox", "_bbox_lonlat_shape")

    def __init__(self, *, bbox: Union[BoundingBox, None]):
        # TODO: support more bbox representations as input
        self._bbox = bbox
        # Cache for shapely polygon in lon/lat
        # Note that for cross-antimeridian cases this will be a multipolygon (two polygons on either side of the antimeridian)
        self._bbox_lonlat_shape = self._bbox.reproject("EPSG:4326").as_geometry() if self._bbox else None

    def _key(self) -> tuple:
        return (self._bbox,)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        if isinstance(other, _SpatialExtent):
            return self._key() == other._key()
        return NotImplemented

    def as_bbox(self, crs: Optional[str] = None) -> Union[BoundingBox, None]:
        bbox = self._bbox
        if bbox and crs:
            bbox = bbox.reproject(crs)
        return bbox

    def intersects(
        self,
        geometry: Union[
            List[float],
            Tuple[float, float, float, float],
            BoundingBox,
            shapely.geometry.base.BaseGeometry,
            None,
        ],
    ):
        """
        Check if given bbox/geometry is within the spatial extent.

        :param geometry: One of:

            - list/tuple of floats: assumed to be bounding box following GeoJSON conventions:
                - lon-lat (EPSG:4326) coordinates
                - antimeridian crossing is represented by `west` > `east`
            - BoundingBox object with valid CRS
            - shapely geometry (assumed to be in EPSG:4326, with proper antimeridian split if crossing)
        """
        # TODO: this assumes bbox is in lon/lat coordinates, also support other CRSes?
        if not self._bbox or geometry is None:
            return True
        if isinstance(geometry, (list, tuple)):
            shape = BoundingBox(*geometry, crs=4326).as_geometry()
        elif isinstance(geometry, BoundingBox):
            # TODO: this is technically not correct
            #       (better is first to convert to geometry and reproject that)
            #       but this is good enough for most intents and purposes
            shape = geometry.reproject("EPSG:4326").as_geometry()
        elif isinstance(geometry, shapely.geometry.base.BaseGeometry):
            shape = geometry
        else:
            raise ValueError(geometry)
        return self._bbox_lonlat_shape.intersects(shape)


class SpatialFilteringGeometries:
    """Like `_SpatialExtent` but geometry based (instead of bounding box based)"""

    __slots__ = ("_geometries",)

    def __init__(
        self, geometries: Union[geopandas.GeoSeries, DriverVectorCube, shapely.geometry.base.BaseGeometry, None]
    ):
        # TODO: do this geometry normalization lazily and only when it will be used
        self._geometries: Union[geopandas.GeoSeries, None]
        if isinstance(geometries, geopandas.GeoSeries):
            self._geometries = geometries
        elif isinstance(geometries, DriverVectorCube):
            self._geometries = geometries.get_geometries()
        elif isinstance(geometries, shapely.geometry.base.BaseGeometry):
            self._geometries = geopandas.GeoSeries([geometries])
        elif geometries is None:
            self._geometries = None
        else:
            self._geometries = None
            logger.warning(f"Unsupported geometries for SpatialFilteringGeometries: {type(geometries)=}")

    def get_simplified_geojson(self, *, vertex_threshold: int = 100) -> Union[str, None]:
        """
        Get simplification (if necessary) of the geometries as GeoJSON string
        to be used as spatial filter (`intersects` parameter) in STAC API queries
        """
        if self._geometries is None:
            return None
        try:
            simplified = GeometrySimplifier().to_simplified_geojson(
                geometry=self._geometries, vertex_threshold=vertex_threshold
            )
            return simplified
        except Exception as e:
            logger.warning(f"Failed to simplify spatial filtering geometries: {e}")
        return None


class SpatioTemporalExtent:
    """Container of spatio-temporal constraints for filtering STAC entities"""
    # TODO: move this to a more generic location for better reuse
    # TODO: enforce/ensure immutability
    # TODO: re-implement in dataclasses/attrs to better enforce immutability and simplify equality/hash implementation

    __slots__ = ("_spatial_extent", "_temporal_extent")

    def __init__(
        self,
        *,
        bbox: Union[BoundingBox, None] = None,
        temporal_extent: Optional[TemporalExtent] = None,
        from_date: DateTimeLikeOrNone = None,
        to_date: DateTimeLikeOrNone = None,
    ):
        self._spatial_extent = _SpatialExtent(bbox=bbox)
        self._temporal_extent = temporal_extent or TemporalExtent(from_date=from_date, to_date=to_date)

    def _key(self) -> tuple:
        return (self._spatial_extent, self._temporal_extent)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        if isinstance(other, SpatioTemporalExtent):
            return self._key() == other._key()
        return NotImplemented

    @property
    def spatial_extent(self) -> _SpatialExtent:
        return self._spatial_extent

    @property
    def temporal_extent(self) -> TemporalExtent:
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


def _spatiotemporal_extent_from_load_params(
    spatial_extent: Union[Dict, BoundingBox, None],
    temporal_extent: Tuple[Optional[str], Optional[str]],
) -> SpatioTemporalExtent:
    bbox = BoundingBox.from_dict_or_none(spatial_extent, default_crs="EPSG:4326")
    temporal_extent = TemporalExtent.from_load_param_extent(temporal_extent)
    return SpatioTemporalExtent(bbox=bbox, temporal_extent=temporal_extent)


def get_item_temporal_extent(item: pystac.Item) -> Tuple[datetime.datetime, datetime.datetime]:
    if start := item.properties.get("start_datetime"):
        start = pystac.utils.str_to_datetime(start)
    else:
        start = item.datetime
    if end := item.properties.get("end_datetime"):
        end = pystac.utils.str_to_datetime(end)
    else:
        end = item.datetime
    return start, end

