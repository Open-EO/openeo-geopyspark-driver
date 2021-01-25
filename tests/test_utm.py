import pytest
import shapely.geometry

from openeogeotrellis._utm import auto_utm_epsg, auto_utm_epsg_for_geometry, utm_zone_from_epsg


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


@pytest.mark.parametrize(["epsg", "expected"], [
    (32601, (1, True)), (32602, (2, True)), (32660, (60, True)),
    (32701, (1, False)), (32702, (2, False)), (32760, (60, False)),
    (32600, ValueError), (32661, ValueError),
    (32700, ValueError), (32761, ValueError),
    (31, ValueError), (-32740, ValueError),
])
def test_utm_zone_from_epsg(epsg, expected):
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            utm_zone_from_epsg(epsg)
    else:
        assert utm_zone_from_epsg(epsg) == expected
