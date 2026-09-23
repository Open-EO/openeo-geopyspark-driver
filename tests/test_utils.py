import getpass
import json
import logging
import pathlib
from pathlib import Path

import botocore.exceptions
import pytest
from openeo_driver.testing import TIFF_DUMMY_DATA

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.geopysparkdatacube import callsite
from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.utils import (
    StatsReporter,
    describe_path,
    json_default,
    lonlat_to_mercator_tile_indices,
    map_optional,
    md5_checksum,
    nullcontext,
    parse_json_from_output,
    single_value,
    stream_s3_binary_file_contents,
    to_s3_url,
    FileChangeWatcher,
    get_jvm,
    to_tuple,
    unzip,
    partition,
    get_s3_file_contents,
)


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


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        (3.1415, 3.1415),
        (pathlib.Path("tmp"), "tmp"),
        # PosixPath is not available on Windows, but is a subclass of Path, so the previous test is enough
        # (pathlib.PosixPath("tmp"), "tmp"),
    ],
)
def test_json_default(value, expected):
    out = json.loads(json.dumps(value, default=json_default))
    assert out == expected


def test_parse_json_from_output():
    json_dict = parse_json_from_output("{}")
    assert json_dict == {}


def test_parse_json_from_output_complex():
    json_dict = parse_json_from_output("""prefix\n{}\nmiddle\n{"num":\n 5}\n""")
    assert json_dict == {"num": 5}


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
    out_file_s3_url = f"s3://{get_backend_config().s3_bucket_name}/{output_file}"
    mock_s3_bucket.put_object(Key=output_file, Body=TIFF_DUMMY_DATA)

    buffer = bytearray()

    for chunk in stream_s3_binary_file_contents(out_file_s3_url):
        buffer += bytearray(chunk)

    assert bytes(buffer) == TIFF_DUMMY_DATA


@pytest.mark.parametrize(
    ["args", "expectation"],
    [
        ([Path("/batch_jobs/j-abc123/text.txt")], nullcontext()),
        ([Path("/batch_jobs/j-abc123/text.txt"), "openeo-fake-bucketname"], nullcontext()),
        (
            [Path("/batch_jobs/j-abc123/text.txt"), "unknown-bucket"],
            pytest.raises(botocore.exceptions.ClientError, match="NoSuchBucket"),
        ),
    ],
)
def test_get_s3_file_contents(mock_s3_bucket, args, expectation):
    text = "some text"
    mock_s3_bucket.put_object(Key="batch_jobs/j-abc123/text.txt", Body=text.encode("utf-8"))

    with expectation:
        assert get_s3_file_contents(*args) == text


@pytest.mark.parametrize(
    ["file_or_folder_path", "bucket_name", "expected_url"],
    [
        ("foo", "test-bucket", "s3://test-bucket/foo"),
        ("/foo/bar/file.txt", "test-bucket", "s3://test-bucket/foo/bar/file.txt"),
    ],
)
def test_to_s3_url_default_bucket_from_config(file_or_folder_path, bucket_name, expected_url):
    # explicit-bucket cases are covered by tests/job_results/test_util.py;
    # this only covers the fallback to the config's bucket name.
    with gps_config_overrides(s3_bucket_name=bucket_name):
        actual = to_s3_url(file_or_folder_path)
        assert actual == expected_url


def test_callsite():
    # object that throws error when converting to string:
    class BadObject:
        value = 5
        def __str__(self):
            raise ValueError("to string error")

    @callsite
    def f(o):
        return "hello " + str(o.value)

    f(BadObject())
    print("done")

def test_map_optional():
    to_upper = str.upper

    assert map_optional(to_upper, None) is None
    assert map_optional(to_upper, "hello") == "HELLO"


def test_get_file_reload_register_func_if_changed(tmp_path):
    # GIVEN a file path that does not exist yet
    cfg_file_path = tmp_path.joinpath("cfg.json")
    watcher = FileChangeWatcher()

    # WHEN you check if a file needs to be reloaded the first time
    reg_func = watcher.get_file_reload_register_func_if_changed(cfg_file_path)
    # THEN it needs to be loaded whether it exists or not
    assert reg_func is not None

    # GIVEN the reload was not successfull (reg_func not called)
    # WHEN you check if a file needs to be reloaded again
    reg_func = watcher.get_file_reload_register_func_if_changed(cfg_file_path)
    # THEN it still needs to be loaded whether it exists or not
    assert reg_func is not None

    # WHEN reload is registered as completed
    reg_func()
    # WHEN you check if a file needs to be reloaded after succesful registration but no change to the file
    reg_func = watcher.get_file_reload_register_func_if_changed(cfg_file_path)
    # THEN no reload is required
    assert reg_func is None

    # WHEN file gets changed
    with open(cfg_file_path, "w") as fh:
        fh.write("file changed")
    # THEN a subsequent check would require reload
    reg_func = watcher.get_file_reload_register_func_if_changed(cfg_file_path)
    assert reg_func is not None

    # When reload succeeded again
    reg_func()
    # THEN subsequent check would not require reload
    reg_func = watcher.get_file_reload_register_func_if_changed(cfg_file_path)
    assert reg_func is None


def test_to_tuple():
    scala_tuple = get_jvm().scala.Tuple3(1, 2, 3)

    assert to_tuple(scala_tuple) == (1, 2, 3)


def test_unzip():
    pairs = [
        (1, "one"),
        (2, "two"),
        (3, "three"),
    ]

    digits, words = list(unzip(*pairs))

    assert digits == (1, 2, 3)
    assert words == ("one", "two", "three")


def test_partition():
    xs = range(10)

    even, odd = partition(lambda i: i % 2 == 0, xs)

    assert list(even) == [0, 2, 4, 6, 8]
    assert list(odd) == [1, 3, 5, 7, 9]


def test_md5_checksum(tmp_path):
    file = tmp_path / "file"

    with open(file, "wb") as f:
        f.write(b"hello world")

    assert md5_checksum(file) == "5eb63bbbe01eeed093cb22bb8f5acdc3"


