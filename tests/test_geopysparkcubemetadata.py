import pytest

from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata


@pytest.mark.parametrize(
    ["this_temporal_extent", "that_temporal_extent", "expected_temporal_extent"],
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
    ],
)
def test_filter_temporal_overlapping_extents(this_temporal_extent, that_temporal_extent, expected_temporal_extent):
    this_start, this_end = this_temporal_extent
    that_start, that_end = that_temporal_extent
    expected_start, expected_end = expected_temporal_extent

    metadata = GeopysparkCubeMetadata(metadata={}, temporal_extent=(this_start, this_end))

    assert metadata.filter_temporal(start=that_start, end=that_end).temporal_extent == (expected_start, expected_end)


@pytest.mark.parametrize(
    ["this_temporal_extent", "that_temporal_extent"],
    [
        (
            ("2024-09-01T00:00:00+00:00", "2024-09-02T00:00:00+00:00"),  # this earlier than that
            ("2024-09-03T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),
        ),
        (
            ("2024-09-03T00:00:00+00:00", "2024-09-04T00:00:00+00:00"),  # this later than that
            ("2024-09-01T00:00:00+00:00", "2024-09-02T00:00:00+00:00"),
        ),
    ],
)
def test_filter_temporal_disjunct_extents(this_temporal_extent, that_temporal_extent):
    this_start, this_end = this_temporal_extent
    that_start, that_end = that_temporal_extent

    metadata = GeopysparkCubeMetadata(metadata={}, temporal_extent=(this_start, this_end))

    with pytest.raises(ValueError) as exc_info:
        metadata.filter_temporal(start=that_start, end=that_end)

    assert exc_info.value.args == (that_start, that_end)
