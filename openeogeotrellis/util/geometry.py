import functools
import json
import urllib.parse
import math
from typing import Union, Dict

import shapely.geometry
from openeo_driver.util.geometry import BoundingBox


class BoundingBoxMerger:
    """Helper to easily build union of multiple BoundingBox objects."""

    def __init__(self, *, crs: Union[None, str, int] = None):
        """
        :param crs: (optional) desired target CRS for the merged bounding box
        """
        self._crs = crs
        # Collect the bounding boxes per crs
        # so that `add`-time union does not involce reprojection.
        # Only do reprojection (if any) when the final merged bbox is requested.
        self._bboxes_per_crs: Dict[str, BoundingBox] = {}

    def add(self, bbox: BoundingBox):
        """Add a bounding box to merge."""
        if bbox.crs not in self._bboxes_per_crs:
            self._bboxes_per_crs[bbox.crs] = bbox
        else:
            self._bboxes_per_crs[bbox.crs] = self._bboxes_per_crs[bbox.crs].union(bbox)

    def get(self) -> Union[None, BoundingBox]:
        """Get the merged bounding box (or None if no boxes were added)."""
        if not self._bboxes_per_crs:
            return None
        elif self._crs:
            return functools.reduce(
                lambda b1, b2: b1.union(b2),
                (b.align_to(self._crs) for b in self._bboxes_per_crs.values()),
            )
        elif len(self._bboxes_per_crs) == 1:
            # Special case: no explicit target CRS:
            # only works if all input bboxes used same CRS,
            # so we can return their union as is.
            return self._bboxes_per_crs.popitem()[1]
        else:
            raise ValueError(
                f"Undefined bounding box merging: no target CRS specified, but multiple CRSes across input: {sorted(self._bboxes_per_crs.keys())}."
            )


class GridSnapper:
    """
    Utility to snap coordinates to a grid defined by origin and resolution.
    Note: this utility works in 1 dimension only, so you need separate instances for x and y coordinates.
    """

    # TODO: eliminate overlap with BoundingBox.round_to_resolution?
    # TODO: add clamping too? pre- or post-snap?
    # TODO: also add a 2D variant that combines two of these, where "down" means "down" in both dimensions, etc?

    __slots__ = ("_orig", "_res")

    def __init__(self, origin: float, resolution: float):
        self._orig = origin
        self._res = resolution

    def down(self, v: float):
        """Snap downwards"""
        return self._orig + self._res * math.floor((v - self._orig) / self._res)

    def round(self, v: float):
        """Snap to nearest"""
        return self._orig + self._res * round((v - self._orig) / self._res)

    def up(self, v: float):
        """Snap upwards"""
        return self._orig + self._res * math.ceil((v - self._orig) / self._res)


def to_geojson_dict(obj) -> dict:
    """Convert various geometry-like objects to GeoJSON dictionary representation."""
    # TODO #1161 use `orient_polygons` to be sure about vertex order, once we require Shapely>=2.1.0
    if isinstance(obj, shapely.geometry.base.BaseGeometry):
        return shapely.geometry.mapping(obj)
    elif isinstance(obj, BoundingBox):
        return obj.as_geojson()
    elif isinstance(obj, list):
        # Assume this is a collection of geometries
        features = [
            {
                "type": "Feature",
                "properties": {},
                "geometry": to_geojson_dict(item),
            }
            for item in obj
        ]
        return {"type": "FeatureCollection", "features": features}
    elif isinstance(obj, dict) and obj.get("type") in {
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "Feature",
        "FeatureCollection",
    }:
        return obj

    # TODO: support other types?
    raise ValueError(obj)


def to_geojson_io_url(geometry):
    """Encode geometry as a geojson.io URL for quick visualization."""
    geojson_dict = to_geojson_dict(geometry)
    geojson_str = json.dumps(geojson_dict)
    encoded = urllib.parse.quote(geojson_str)
    return f"https://geojson.io/#data=data:application/json,{encoded}"


def bbox_to_geojson(*args) -> dict:
    """
    Convert bounding box (xmin, ymin, xmax, ymon),
    given as separate arguments or a single tuple/list,
    to a GeoJSON (Polygon) dict.

    Note that given coordinates must be in lon-lat CRS (because GeoJSON).
    """
    if len(args) == 1:
        xmin, ymin, xmax, ymax = args[0]
    elif len(args) == 4:
        xmin, ymin, xmax, ymax = args
    else:
        raise ValueError(args)

    geometry = BoundingBox(xmin, ymin, xmax, ymax, crs=4326).as_geometry()
    # TODO #1161 use `orient_polygons` to be sure, once we require Shapely>=2.1.0
    # polygon = shapely.orient_polygons(geometry)
    return shapely.geometry.mapping(geometry)
