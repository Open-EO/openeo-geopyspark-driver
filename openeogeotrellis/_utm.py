import math
from functools import partial
from typing import Tuple

import pyproj
import shapely.ops
from shapely.geometry.base import BaseGeometry


def auto_utm_epsg(lon: float, lat: float) -> int:
    """
    Get EPSG code of UTM zone containing given long-lat coordinates
    """
    # Use longitude to determine zone 'band'
    zone = (math.floor((lon + 180.0) / 6.0) % 60) + 1

    # Use latitude to determine north/south
    if lat >= 0.0:
        epsg = 32600 + zone
    else:
        epsg = 32700 + zone

    return epsg


def utm_zone_from_epsg(epsg: int) -> Tuple[int, bool]:
    """Get `(utm_zone, is_northern_hemisphere)` from given EPSG code."""
    if not (32601 <= epsg <= 32660 or 32701 <= epsg <= 32760):
        raise ValueError("Can not convert EPSG {e} to UTM zone".format(e=epsg))
    return epsg % 100, epsg < 32700


def auto_utm_epsg_for_geometry(geometry: BaseGeometry, crs: str = "EPSG:4326") -> int:
    """
    Get EPSG code of best UTM zone for given geometry.
    """
    # Pick a geometry coordinate
    p = geometry.representative_point()
    x = p.x
    y = p.y

    # If needed, convert it to lon/lat (WGS84)
    crs_wgs = 'epsg:4326'
    if crs.lower() != crs_wgs:
        x, y = pyproj.transform(pyproj.Proj(crs),
                                pyproj.Proj(crs_wgs),
                                x, y, always_xy=True)

    # And derive the EPSG code
    return auto_utm_epsg(x, y)


def auto_utm_crs_for_geometry(geometry: BaseGeometry, crs: str) -> str:
    epsg = auto_utm_epsg_for_geometry(geometry, crs)
    return 'epsg:' + str(epsg)


def geometry_to_crs(geometry, crs_from, crs_to):
    # Skip if CRS definitions are exactly the same
    if crs_from == crs_to:
        return geometry

    # Construct a function for projecting coordinates
    proj_from = pyproj.Proj(crs_from)
    proj_to = pyproj.Proj(crs_to)

    def project(x, y, z=0):
        return pyproj.transform(proj_from, proj_to, x, y, always_xy=True)

    # And apply to all coordinates in the geometry
    return shapely.ops.transform(project, geometry)


def area_in_square_meters(geometry, crs):
    geometry_area = shapely.ops.transform(
        partial(
            pyproj.transform,
            pyproj.Proj(init=crs),
            pyproj.Proj(
                proj='aea',
                lat_1=geometry.bounds[1],
                lat_2=geometry.bounds[3])
        ),
        geometry)

    return geometry_area.area
