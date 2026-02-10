import json
import urllib.parse

import math
from typing import Union

import shapely.geometry
from openeo_driver.util.geometry import BoundingBox


class BoundingBoxMerger:
    """Helper to easily build union of multiple BoundingBox objects."""

    def __init__(self, *, crs: Union[None, str, int] = None):
        """
        :param crs: (optional) desired target CRS for the merged bounding box
        """
        self._crs = crs
        self._bbox: Union[None, BoundingBox] = None

    def add(self, bbox: BoundingBox):
        """Add a bounding box to merge."""
        if self._bbox is None:
            if self._crs:
                # Just ensure first bbox is in desired CRS (if any)
                # (subsequent unions will follow automatically)
                bbox = bbox.align_to(target=self._crs)
            self._bbox = bbox
        else:
            self._bbox = self._bbox.union(bbox)

    def get(self) -> Union[None, BoundingBox]:
        """Get the merged bounding box (or None if no boxes were added)."""
        return self._bbox


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

    polygon = shapely.geometry.box(xmin, ymin, xmax, ymax, ccw=True)
    # TODO #1161 use `orient_polygons` to be sure, once we require Shapely>=2.1.0
    # polygon = shapely.orient_polygons(polygon)
    return shapely.geometry.mapping(polygon)
