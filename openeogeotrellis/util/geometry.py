import functools
import json
import math
import urllib.parse
from typing import Dict, Union

import geopandas
import shapely.geometry
import shapely.geometry.base
from openeo_driver.util.geometry import BoundingBox, reproject_geometry


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


class GeometrySimplifier:
    @staticmethod
    def _vertex_count(geometries: Union[geopandas.GeoSeries, shapely.geometry.base.BaseGeometry]) -> int:
        """
        Count number of vertices in given geometries as measure of complexity.
        Note that this is based on length of shapely coordinate array,
        where the first vertex of a polygon is duplicated as the final one,
        so the vertex count is off by one (e.g. a rectangle will give 5).
        But that's generally fine here because that is also the case in
        the GeoJSON representation.
        """
        if isinstance(geometries, geopandas.GeoSeries):
            return geometries.apply(lambda g: shapely.count_coordinates(g)).sum()
        elif isinstance(geometries, shapely.geometry.base.BaseGeometry):
            return shapely.count_coordinates(geometries)
        else:
            raise ValueError(geometries)

    def simplify(
        self,
        geometry: Union[
            geopandas.GeoSeries,
            shapely.geometry.Polygon,
            shapely.geometry.MultiPolygon,
        ],
        *,
        vertex_threshold: int = 32,
    ) -> shapely.geometry.base.BaseGeometry:
        """
        Simplify given geometry (Shapely shape or GeoPandas series)
        to a (Multi)Polygon
        :param geometry:
        :param vertex_threshold: limit for number of vertices to in simplified geometry
        :return:
        """
        if isinstance(geometry, geopandas.GeoSeries):
            geometry_types = set(geometry.geometry.geom_type.unique())
            if not geometry_types.issubset({"Polygon", "MultiPolygon"}):
                raise ValueError(f"Simplification only supported for (Multi)Polygons, but got {geometry_types=}")

            # Simplify each geometry of the series to its bounding box to reduce complexity
            if (
                # Quick lower bound estimation (assuming polygons are at least triangles)
                geometry.shape[0] * 4 > vertex_threshold
                or self._vertex_count(geometry) > vertex_threshold
            ):
                geometry = geometry.envelope
            # Flatten to single (multi)polygon
            # method "coverage": optimized for non-overlapping polygons
            # and can be significantly faster than the unary union algorithm.
            if hasattr(geopandas.GeoSeries, "union_all"):
                simplified: shapely.geometry.base.BaseGeometry = geometry.union_all(method="coverage")
            else:
                # TODO: remove this fallback for geopandas<1.0.0 once Python 3.8 support is dropped
                simplified: shapely.geometry.base.BaseGeometry = geometry.unary_union
        elif isinstance(geometry, (shapely.geometry.Polygon, shapely.geometry.MultiPolygon)):
            simplified = geometry
        else:
            # TODO: more geometry types/containers to support?
            raise ValueError(geometry)

        if self._vertex_count(simplified) > vertex_threshold:
            simplified = shapely.convex_hull(simplified)
            if self._vertex_count(simplified) > vertex_threshold:
                simplified = simplified.envelope
        return simplified

    def to_simplified_geojson(
        self,
        geometry: Union[
            geopandas.GeoSeries,
            shapely.geometry.Polygon,
            shapely.geometry.MultiPolygon,
        ],
        *,
        vertex_threshold: int = 100,
        round_decimals: Union[int, None] = 4,
    ) -> str:
        simplified_geometry = self.simplify(geometry=geometry, vertex_threshold=vertex_threshold)

        # Reproject to lon/lat for GeoJSON compliance
        if isinstance(geometry, geopandas.GeoSeries) and geometry.crs:
            simplified_geometry = reproject_geometry(simplified_geometry, from_crs=geometry.crs, to_crs="epsg:4326")

        if round_decimals is not None:
            # 4 decimal places (default) is enough in lon-lat degrees for meter-level precision
            simplified_geometry = shapely.transform(simplified_geometry, lambda x: x.round(round_decimals))

        return shapely.to_geojson(simplified_geometry)
