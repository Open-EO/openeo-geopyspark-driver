import pytest
import shapely.geometry

from openeogeotrellis._utm import auto_utm_epsg, auto_utm_epsg_for_geometry


@pytest.mark.parametrize(["lon", "lat", "epsg"], [
    (-179, 45, 32601),
    (-179, -45, 32701),
    (179, 45, 32660),
    (179, -45, 32760),
    (0, 0, 32631),
    (3.7, 51.03, 32631),
    (270, 40, 32616),
    (-270, -40, 32746),
])
def test_auto_utm_epsg(lon, lat, epsg):
    assert auto_utm_epsg(lon, lat) == epsg


@pytest.mark.parametrize(["geom", "crs", "epsg"], [
    (shapely.geometry.box(3, 51, 4, 52), "EPSG:4326", 32631),
    (shapely.geometry.box(-10, -20, 40, 50), "EPSG:4326", 32633),
    (shapely.geometry.box(500100, 100, 500500, 1000), "EPSG:32610", 32610),
])
def test_auto_utm_epsg_for_geometry(geom, crs, epsg):
    assert auto_utm_epsg_for_geometry(geom, crs) == epsg
