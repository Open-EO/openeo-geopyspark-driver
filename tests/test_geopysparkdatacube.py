import pytest

from openeogeotrellis.testing import DummyCubeBuilder
from openeogeotrellis.util.datetime import to_datetime_naive


class TestGeopysparkDataCube:
    @pytest.mark.parametrize(
        ["start", "end", "expected"],
        [
            (None, None, ("2020-09-24", "2020-10-04")),
            ("2020-09-01", "2020-10-30", ("2020-09-24", "2020-10-04")),
            ("2020-09-01", "2020-10-01", ("2020-09-24", "2020-09-30")),
            ("2020-09-26", "2020-10-04", ("2020-09-26", "2020-10-01")),
            ("2020-09-26", "2020-11-01", ("2020-09-26", "2020-10-04")),
        ],
    )
    def test_filter_temporal_half_open(self, start, end, expected):
        cube_dates = [
            "2020-09-24",
            "2020-09-26",
            "2020-09-30",
            "2020-10-01",
            "2020-10-04",
        ]
        cube = DummyCubeBuilder().build_cube(dates=cube_dates)
        result = cube.filter_temporal(start=start, end=end)

        layer = result.get_max_level()
        instants = set(layer.to_numpy_rdd().map(lambda kv: kv[0].instant).collect())
        expected = tuple(to_datetime_naive(e) for e in expected)
        assert (min(instants), max(instants)) == expected
