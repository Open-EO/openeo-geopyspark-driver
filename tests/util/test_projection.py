from openeogeotrellis.util.projection import is_utm_epsg_code


def test_is_utm_epsg_code():
    assert is_utm_epsg_code(32601) is True
    assert is_utm_epsg_code(32631) is True
    assert is_utm_epsg_code(32660) is True
    assert is_utm_epsg_code(32701) is True
    assert is_utm_epsg_code(32729) is True
    assert is_utm_epsg_code(32760) is True

    assert is_utm_epsg_code(4326) is False
    assert is_utm_epsg_code(None) is False
    assert is_utm_epsg_code("32731") is False
    assert is_utm_epsg_code("epsg:32731") is False
    assert is_utm_epsg_code("EPSG:32731") is False
