#!/usr/bin/env python3

import math

import pyproj
import shapely.ops


def auto_utm_epsg(lon, lat):

    # Use longitude to determine zone 'band'

    zone = (math.floor((lon + 180.0) / 6.0) % 60) + 1

    # Use latitude to determine north/south

    if lat >= 0.0:
        epsg = 32600 + zone
    else:
        epsg = 32700 + zone

    return epsg


def auto_utm_epsg_for_geometry(geometry, crs):

    # Pick a geometry coordinate

    p = geometry.representative_point()

    x = p.x
    y = p.y

    # If needed, convert it to lon/lat (WGS84)

    crs_wgs = 'epsg:4326'

    if crs != crs_wgs:
        x, y = pyproj.transform(pyproj.Proj(crs),
                                pyproj.Proj(crs_wgs),
                                x, y, always_xy=True)

    # And derive the EPSG code

    return auto_utm_epsg(x, y)


def auto_utm_crs_for_geometry(geometry, crs):

    epsg = auto_utm_epsg_for_geometry(geometry, crs)

    return 'epsg:' + str(epsg)


def geometry_to_crs(geometry, crs_from, crs_to):

    # Skip if CRS definitions are exactly the same

    if crs_from == crs_to:
        return geometry

    # Construct a function for projecting coordinates

    proj_from = pyproj.Proj(crs_from)
    proj_to   = pyproj.Proj(crs_to)

    def project(x, y, z=0):
        return pyproj.transform(proj_from, proj_to, x, y, always_xy=True)

    # And apply to all coordinates in the geometry

    return shapely.ops.transform(project, geometry)

