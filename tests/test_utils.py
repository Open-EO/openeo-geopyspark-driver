import collections
import datetime
import getpass
import logging
from pathlib import Path

import pytest

from openeo_driver.testing import TIFF_DUMMY_DATA
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.utils import (
    dict_merge_recursive,
    describe_path,
    lonlat_to_mercator_tile_indices,
    nullcontext,
    utcnow,
    UtcNowClock,
    single_value,
    StatsReporter,
    get_s3_binary_file_contents,
    to_s3_url, parse_approximate_isoduration, reproject_cellsize,
)


@pytest.mark.parametrize(["a", "b", "expected"], [
    ({}, {}, {}),
    ({1: 2}, {}, {1: 2}),
    ({}, {1: 2}, {1: 2}),
    ({1: 2}, {3: 4}, {1: 2, 3: 4}),
    ({1: {2: 3}}, {1: {4: 5}}, {1: {2: 3, 4: 5}}),
    ({1: {2: 3, 4: 5}, 6: 7}, {1: {8: 9}, 10: 11}, {1: {2: 3, 4: 5, 8: 9}, 6: 7, 10: 11}),
    ({1: {2: {3: {4: 5, 6: 7}}}}, {1: {2: {3: {8: 9}}}}, {1: {2: {3: {4: 5, 6: 7, 8: 9}}}}),
    ({1: {2: 3}}, {1: {2: 3}}, {1: {2: 3}})
])
def test_merge_recursive_default(a, b, expected):
    assert dict_merge_recursive(a, b) == expected


@pytest.mark.parametrize(["a", "b", "expected"], [
    ({1: 2}, {1: 3}, {1: 3}),
    ({1: 2, 3: 4}, {1: 5}, {1: 5, 3: 4}),
    ({1: {2: {3: {4: 5}}, 6: 7}}, {1: {2: "foo"}}, {1: {2: "foo", 6: 7}}),
    ({1: {2: {3: {4: 5}}, 6: 7}}, {1: {2: {8: 9}}}, {1: {2: {3: {4: 5}, 8: 9}, 6: 7}}),
])
def test_merge_recursive_overwrite(a, b, expected):
    result = dict_merge_recursive(a, b, overwrite=True)
    assert result == expected


@pytest.mark.parametrize(["a", "b", "expected"], [
    ({1: 2}, {1: 3}, {1: 3}),
    ({1: "foo"}, {1: {2: 3}}, {1: {2: 3}}),
    ({1: {2: 3}}, {1: "bar"}, {1: "bar"}),
    ({1: "foo"}, {1: "bar"}, {1: "bar"}),
])
def test_merge_recursive_overwrite_conflict(a, b, expected):
    with pytest.raises(ValueError) as e:
        dict_merge_recursive(a, b)
    assert "key 1" in str(e)

    result = dict_merge_recursive(a, b, overwrite=True)
    assert result == expected


def test_merge_recursive_preserve_input():
    a = {1: {2: 3}}
    b = {1: {4: 5}}
    result = dict_merge_recursive(a, b)
    assert result == {1: {2: 3, 4: 5}}
    assert a == {1: {2: 3}}
    assert b == {1: {4: 5}}


def test_dict_merge_recursive_accepts_arbitrary_mapping():
    class EmptyMapping(collections.Mapping):
        def __getitem__(self, key):
            raise KeyError(key)

        def __len__(self) -> int:
            return 0

        def __iter__(self):
            return iter(())

    a = EmptyMapping()
    b = {1: 2}
    assert dict_merge_recursive(a, b) == {1: 2}
    assert dict_merge_recursive(b, a) == {1: 2}
    assert dict_merge_recursive(a, a) == {}


def test_describe_path(tmp_path):
    tmp_path = Path(tmp_path)
    a_dir = tmp_path / "dir"
    a_dir.mkdir()
    a_file = tmp_path / "file.txt"
    a_file.touch()
    a_symlink = tmp_path / "symlink.txt"
    a_symlink.symlink_to(a_file)
    paths = [a_dir, a_file, a_symlink]
    paths.extend([str(p) for p in paths])
    for path in paths:
        d = describe_path(path)
        assert "rw" in d["mode"]
        assert d["user"] == getpass.getuser()

    assert describe_path(tmp_path / "invalid")["status"] == "does not exist"


@pytest.mark.parametrize(["lon", "lat", "zoom", "flip_y", "expected"], [
    (0, 0, 0, False, (0, 0)),
    (0, 0, 1, False, (0, 0)),
    (0, 0, 2, False, (1, 1)),
    (0, 0, 5, False, (15, 15)),
    (0, 0, 5, True, (15, 16)),
    (179, 85, 0, False, (0, 0)),
    (179, 85, 1, False, (1, 1)),
    (179, 85, 2, False, (3, 3)),
    (179, 85, 3, False, (7, 7)),
    (179, 85, 5, False, (31, 31)),
    (-179, 85, 5, False, (0, 31)),
    (179, -85, 5, False, (31, 0)),
    (-179, -85, 5, False, (0, 0)),
    (179, -85, 0, True, (0, 0)),
    (179, -85, 1, True, (1, 1)),
    (179, -85, 2, True, (3, 3)),
    (179, -85, 3, True, (7, 7)),
    (179, -85, 5, True, (31, 31)),
    (179, 85, 5, True, (31, 0)),
    (-179, -85, 5, True, (0, 31)),
    (-179, 85, 5, True, (0, 0)),
    (3.2, 51.3, 0, True, (0, 0)),
    (3.2, 51.3, 1, True, (1, 0)),
    (3.2, 51.3, 2, True, (2, 1)),
    (3.2, 51.3, 3, True, (4, 2)),
    (3.2, 51.3, 4, True, (8, 5)),
    (3.2, 51.3, 6, True, (32, 21)),
    (3.2, 51.3, 8, True, (130, 85)),
    (3.2, 51.3, 10, True, (521, 341)),
])
def test_lonlat_to_mercator_tile_indices(lon, lat, zoom, flip_y, expected):
    assert lonlat_to_mercator_tile_indices(longitude=lon, latitude=lat, zoom=zoom, flip_y=flip_y) == expected


def test_nullcontext():
    with nullcontext() as n:
        assert n is None


class TestUtcNowClock:

    def test_default(self):
        now = utcnow()
        real_now = datetime.datetime.utcnow()
        assert isinstance(now, datetime.datetime)
        assert (real_now - now).total_seconds() < 1

    def test_mock(self):
        with UtcNowClock.mock(now=datetime.datetime(2012, 3, 4, 5, 6)):
            assert utcnow() == datetime.datetime(2012, 3, 4, 5, 6)

    def test_mock_str_date(self):
        with UtcNowClock.mock(now="2021-10-22"):
            assert utcnow() == datetime.datetime(2021, 10, 22)

    def test_mock_str_datetime(self):
        with UtcNowClock.mock(now="2021-10-22 12:34:56"):
            assert utcnow() == datetime.datetime(2021, 10, 22, 12, 34, 56)


def test_single_value():
    try:
        single_value([])
        pytest.fail("an empty list doesn't have a single value")
    except ValueError:
        pass

    assert single_value([1]) == 1
    assert single_value([1, 1]) == 1

    try:
        xs = [1, 2]
        single_value(xs)
        pytest.fail(f"{xs} doesn't have a single value")
    except ValueError:
        pass

    assert single_value({'a': ['VH'], 'b': ['VH']}.values()) == ['VH']


class TestStatsReporter:
    def test_basic(self, caplog):
        caplog.set_level(logging.INFO)
        with StatsReporter() as stats:
            stats["apple"] += 1
            stats["banana"] += 2
            for i in range(3):
                stats["banana"] += 5
            stats["coconut"] = 8

        assert caplog.messages == ['stats: {"apple": 1, "banana": 17, "coconut": 8}']

    def test_exception(self, caplog):
        caplog.set_level(logging.INFO)
        with pytest.raises(ValueError):
            with StatsReporter() as stats:
                stats["apple"] += 1
                stats["banana"] += 2
                for i in range(3):
                    if i > 1:
                        raise ValueError
                    stats["banana"] += 5
                stats["coconut"] = 8

        assert caplog.messages == ['stats: {"apple": 1, "banana": 12}']


def test_get_s3_binary_file_contents(mock_s3_bucket):
    """Upload a file to the mock implementation of S3 and check that our wrapper
    function can download it correctly, meaning:
    - it processes the S3 URL correctly
    - it downloads the file as binary, so the result should be identical byte for byte.
    """
    output_file = "foo/bar.tif"
    # mock_s3_bucket sets the ConfigParams().s3_bucket_name to a fake test bucket.
    out_file_s3_url = f"s3://{ConfigParams().s3_bucket_name}/{output_file}"
    mock_s3_bucket.put_object(Key=output_file, Body=TIFF_DUMMY_DATA)

    bytes_retrieved = get_s3_binary_file_contents(out_file_s3_url)

    assert TIFF_DUMMY_DATA == bytes_retrieved


@pytest.mark.parametrize(
    ["file_or_folder_path", "bucket_name", "expected_url"],
    [
        # Slashes at the start and end of the path should be unified:
        # the S3 key has no slashes at the start or end.
        ("foo", "test-bucket", "s3://test-bucket/foo"),
        ("foo/", "test-bucket", "s3://test-bucket/foo"),
        ("/foo", "test-bucket", "s3://test-bucket/foo"),
        ("/foo/", "test-bucket", "s3://test-bucket/foo"),
        ("foo/bar", "test-bucket", "s3://test-bucket/foo/bar"),
        ("foo/bar/", "test-bucket", "s3://test-bucket/foo/bar"),
        ("/foo/bar", "test-bucket", "s3://test-bucket/foo/bar"),
        ("/foo/bar/", "test-bucket", "s3://test-bucket/foo/bar"),
        ("foo/bar/file.txt", "test-bucket", "s3://test-bucket/foo/bar/file.txt"),
        ("/foo/bar/file.txt", "test-bucket", "s3://test-bucket/foo/bar/file.txt"),
        # Less likely to occur: slashes at the start or end of the bucket name,
        # but just in case we have small mistakes in the bucket name.
        ("foo/bar/file.txt", "test-bucket/", "s3://test-bucket/foo/bar/file.txt"),
        ("foo/bar/file.txt", "/test-bucket", "s3://test-bucket/foo/bar/file.txt"),
        ("foo/bar/file.txt", "/test-bucket/", "s3://test-bucket/foo/bar/file.txt"),
        ("/foo/bar/file.txt", "test-bucket/", "s3://test-bucket/foo/bar/file.txt"),
        ("/foo/bar/file.txt", "/test-bucket", "s3://test-bucket/foo/bar/file.txt"),
        ("/foo/bar/file.txt", "/test-bucket/", "s3://test-bucket/foo/bar/file.txt"),
    ],
)
def test_to_s3_url(file_or_folder_path, bucket_name, expected_url, monkeypatch):
    monkeypatch.setenv("SWIFT_BUCKET", "this-should-not-show-up-as-the-bucket")
    actual1 = to_s3_url(file_or_folder_path, bucketname=bucket_name)
    assert actual1 == expected_url

    # Check result when bucket_name arg not set => using ConfigParams
    # ConfigParams gets its value from the env var SWIFT_BUCKET
    bucket_name = bucket_name or "openeo-fake-bucketname"
    monkeypatch.setenv("SWIFT_BUCKET", bucket_name)
    actual2 = to_s3_url(file_or_folder_path)
    assert actual2 == expected_url


spatial_extent_tap = {
    "east": 5.08,
    "north": 51.22,
    "south": 51.215,
    "west": 5.07,
}


@pytest.mark.parametrize(
    ["spatial_extent", "native_resolution", "to_crs", "expected"],
    [
        (
                {'crs': 'EPSG:4326', 'east': 93.178583, 'north': 71.89922, 'south': -21.567515, 'west': -54.925613},
                {'cell_height': 8.3333333333e-05, 'cell_width': 8.3333333333e-05, 'crs': 'EPSG:4326'},
                'Auto42001',
                (8.529099359293468, 9.347610141150653),
        ),
        (
                spatial_extent_tap,
                {'cell_height': 8.3333333333e-05, 'cell_width': 8.3333333333e-05, 'crs': 'EPSG:4326'},
                'Auto42001',
                (6.080971189774573, 9.430383333005011),
        ),
        (
                spatial_extent_tap,
                {'cell_height': 10, 'cell_width': 10, 'crs': 'Auto42001'},
                'EPSG:4326',
                (0.0001471299295632278, 9.240073598704157e-05),
        ),
        (
                # North of UTM zone:
                {'east': 0.01, 'north': 83.01, 'south': 83, 'west': 0},
                {'cell_height': 10, 'cell_width': 10, 'crs': 'EPSG:32632'},
                'EPSG:4326',
                # note that here we have 14.6% more degrees for 10m compared to at the equator
                (0.0008405907359465923, 0.00010237891864051107),
        ),
        (
                # At equator:
                {'east': 0.01, 'north': 0.01, 'south': 0, 'west': 0},
                {'cell_height': 10, 'cell_width': 10, 'crs': 'EPSG:32632'},
                'EPSG:4326',
                (0.0000887560370977725, 0.00008935420776900408)
        ),
    ],
)
def test_reproject_cellsize(spatial_extent, native_resolution, to_crs, expected):
    projected_resolution = reproject_cellsize(spatial_extent, native_resolution, to_crs)
    print(projected_resolution)
    assert abs(projected_resolution[0] - expected[0]) < 1e-7
    assert abs(projected_resolution[1] - expected[1]) < 1e-7


@pytest.mark.parametrize(
    ["duration_str", "expected"],
    [
        ("PT1H30M15.460S", "1:30:15.460000"),
        ("P5DT4M", "5 days, 0:04:00"),
        ("P2WT3H", "14 days, 3:00:00"),
        ("P16D", "16 days, 0:00:00"),
        ("P1D", "1 day, 0:00:00"),
        ("P1M", "30 days, 9:36:00"),
        ("P1Y", "365 days, 0:00:00"),
        ("P2D", "2 days, 0:00:00"),
        ("P5D", "5 days, 0:00:00"),
        ("P6Y", "2190 days, 0:00:00"),
        ("P999D", "999 days, 0:00:00"),
        ("P999M", "30369 days, 14:24:00"),
        ("P999Y", "364635 days, 0:00:00"),
    ],
)
def test_parse_approximate_isoduration(duration_str, expected):
    # This function needed some adjustments to work with durations found in layercatalog metadata:
    duration = parse_approximate_isoduration(duration_str)
    print(f"duration={duration}")
    assert str(duration) == expected
