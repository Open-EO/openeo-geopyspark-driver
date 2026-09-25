import pytest

from openeogeotrellis.util.projection import is_utm_epsg_code, reproject_cellsize

spatial_extent_tap = {
    "east": 5.08,
    "north": 51.22,
    "south": 51.215,
    "west": 5.07,
}


@pytest.mark.parametrize(
    ["spatial_extent", "input_resolution", "input_crs", "to_crs", "expected"],
    [
        (
                {'crs': 'EPSG:4326', 'east': 93.178583, 'north': 71.89922, 'south': -21.567515, 'west': -54.925613},
                (8.3333333333e-05, 8.3333333333e-05),
                'EPSG:4326',
                'Auto42001',
                (8.529099359293468, 9.347610141150653),
        ),
        (
                spatial_extent_tap,
                (8.3333333333e-05, 8.3333333333e-05),
                'EPSG:4326',
                'Auto42001',
                (6.080971189774573, 9.430383333005011),
        ),
        (
                spatial_extent_tap,
                (10, 10),
                'Auto42001',
                'EPSG:4326',
                (0.0001471299295632278, 9.240073598704157e-05),
        ),
        (
                # North Pole is outside EPSG:32632, but still interesting:
                {'east': 0.01, 'north': 89.999999, 'south': 89.999998, 'west': 0},
                (1000, 1000),
                'EPSG:32632',
                'EPSG:4326',
                (314.99451024025336, 0.012663855310563576),
        ),
        (
                # North of UTM zone:
                {'east': 0.01, 'north': 83.01, 'south': 83, 'west': 0},
                (10, 10),
                'EPSG:32632',
                'EPSG:4326',
                # note that here we have 9x more degrees in the x-dimension for 10m compared to at the equator
                (0.0008405907359465923, 0.00010237891864051107),
        ),
        (
                # At equator:
                {'east': 0.01, 'north': 0.01, 'south': 0, 'west': 0},
                (10, 10),
                'EPSG:32632',
                'EPSG:4326',
                (0.0000887560370977725, 0.00008935420776900408)
        ),
    ],
)
def test_reproject_cellsize(spatial_extent: dict, input_resolution: tuple, input_crs: str,
                            to_crs: str, expected: tuple):
    projected_resolution = reproject_cellsize(spatial_extent, input_resolution, input_crs, to_crs)
    print(projected_resolution)
    assert projected_resolution == tuple(pytest.approx(x, abs=1e-7) for x in expected)


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
