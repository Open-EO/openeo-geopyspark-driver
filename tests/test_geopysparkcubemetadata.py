import pytest

from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata


class TestGeopysparkCubeMetadata:

    @pytest.mark.parametrize(
        ["this_temporal_extent", "that_temporal_extent", "expected"],
        [
            (
                ("2024-09-02T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),  # this fully within that
                ("2024-09-01T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),
                ("2024-09-02T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),
            ),
            (
                ("2024-09-01T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),  # that fully within this
                ("2024-09-02T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),
                ("2024-09-02T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),
            ),
            (
                ("2024-09-02T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),  # that intersects this on the earlier side
                ("2024-09-01T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),
                ("2024-09-02T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),
            ),
            (
                ("2024-09-01T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),  # that intersects this on the later side
                ("2024-09-02T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),
                ("2024-09-02T00:00:00+00:00", "2024-09-03T00:00:00+00:00"),
            ),
            (
                ("2024-09-01T00:00:00+00:00", "2024-09-02T00:00:00+00:00"),  # this earlier than that
                ("2024-09-03T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),
                "Empty temporal extent after filtering",
            ),
            (
                ("2024-09-03T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),  # this later than that
                ("2024-09-01T00:00:00+00:00", "2024-09-02T00:00:00+00:00"),
                "Empty temporal extent after filtering",
            ),
            ((None, None), (None, None), (None, None)),
            ((None, "2024-09-09"), (None, None), (None, "2024-09-09")),
            (("2024-09-09", None), (None, None), ("2024-09-09", None)),
            ((None, None), (None, "2024-09-09"), (None, "2024-09-09")),
            ((None, None), ("2024-09-09", None), ("2024-09-09", None)),
            ((None, "2024-09-09"), (None, "2024-11-11"), (None, "2024-09-09")),
            ((None, "2024-11-11"), (None, "2024-09-09"), (None, "2024-09-09")),
            (("2024-09-09", None), ("2024-11-11", None), ("2024-11-11", None)),
            (("2024-11-11", None), ("2024-09-09", None), ("2024-11-11", None)),
            ((None, "2024-09-09"), ("2024-11-11", None), "Empty temporal extent after filtering"),
            ((None, "2024-11-11"), ("2024-09-09", None), ("2024-09-09", "2024-11-11")),
            (("2024-11-11", None), (None, "2024-09-09"), "Empty temporal extent after filtering"),
            (("2024-09-09", None), (None, "2024-11-11"), ("2024-09-09", "2024-11-11")),
            (("2024-09-09", "2024-11-11"), ("2024-10-10", None), ("2024-10-10", "2024-11-11")),
            (("2024-09-09", "2024-11-11"), (None, "2024-10-10"), ("2024-09-09", "2024-10-10")),
            (("2024-10-10", None), ("2024-09-09", "2024-11-11"), ("2024-10-10", "2024-11-11")),
            ((None, "2024-10-10"), ("2024-09-09", "2024-11-11"), ("2024-09-09", "2024-10-10")),
        ],
    )
    def test_filter_temporal_overlapping_extents(self, this_temporal_extent, that_temporal_extent, expected):
        metadata = GeopysparkCubeMetadata(metadata={}, temporal_extent=this_temporal_extent)
        that_start, that_end = that_temporal_extent
        if isinstance(expected, tuple):
            assert metadata.filter_temporal(start=that_start, end=that_end).temporal_extent == expected
        elif isinstance(expected, str):
            with pytest.raises(ValueError, match=expected):
                metadata.filter_temporal(start=that_start, end=that_end)
        else:
            raise ValueError(f"Invalid test case: {expected=}")
