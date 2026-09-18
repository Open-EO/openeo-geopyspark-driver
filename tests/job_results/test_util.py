import json
import pathlib

import pytest
from shapely.geometry import Point

from openeogeotrellis.job_results.util import (
    AnnotatedDict,
    BadlyHashable,
    GDALINFO_SUFFIX,
    json_default,
    make_set_for_key,
    md5_checksum,
    parse_json_from_output,
    reproject_geometry,
    to_jsonable,
    to_s3_url,
    unzip,
)


def test_gdalinfo_suffix():
    assert GDALINFO_SUFFIX == "_gdalinfo.json"


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        (3.1415, 3.1415),
        (pathlib.Path("tmp"), "tmp"),
    ],
)
def test_json_default(value, expected):
    out = json.loads(json.dumps(value, default=json_default))
    assert out == expected


def test_json_default_unsupported():
    with pytest.raises(TypeError):
        json.dumps(object(), default=json_default)


def test_parse_json_from_output():
    json_dict = parse_json_from_output("{}")
    assert json_dict == {}


def test_parse_json_from_output_complex():
    json_dict = parse_json_from_output("""prefix\n{}\nmiddle\n{"num":\n 5}\n""")
    assert json_dict == {"num": 5}


def test_make_set_for_key():
    data = {
        "a": {"proj:epsg": 4326},
        "b": {"proj:epsg": 4326},
        "c": {"proj:epsg": 32631},
        "d": {},
    }
    assert make_set_for_key(data, "proj:epsg") == {4326, 32631}


def test_make_set_for_key_with_func():
    data = {
        "a": {"proj:shape": [10, 20]},
        "b": {"proj:shape": [10, 20]},
    }
    assert make_set_for_key(data, "proj:shape", tuple) == {(10, 20)}


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        (1.5, 1.5),
        (float("nan"), "nan"),
        (float("inf"), "inf"),
        (float("-inf"), "-inf"),
        ({"a": 1.5, "b": [float("nan")]}, {"a": 1.5, "b": ["nan"]}),
        ([1, float("nan"), "x"], [1, "nan", "x"]),
    ],
)
def test_to_jsonable(value, expected):
    assert to_jsonable(value) == expected


def test_unzip():
    pairs = [
        (1, "one"),
        (2, "two"),
        (3, "three"),
    ]

    digits, words = list(unzip(*pairs))

    assert digits == (1, 2, 3)
    assert words == ("one", "two", "three")


def test_md5_checksum(tmp_path):
    file = tmp_path / "file"

    with open(file, "wb") as f:
        f.write(b"hello world")

    assert md5_checksum(file) == "5eb63bbbe01eeed093cb22bb8f5acdc3"


class TestAnnotatedDict:
    def test_empty(self):
        d = AnnotatedDict()
        assert d == {}

    def test_set_annotation(self):
        d = AnnotatedDict(name="john")
        d.annotations["color"] = "green"
        assert d == {"name": "john"}
        assert d.annotations["color"] == "green"

    def test_annotate_dict(self):
        d = AnnotatedDict(name="john").annotate({"color": "green", "flavor": "lime"})
        assert d == {"name": "john"}
        assert d.annotations == {"color": "green", "flavor": "lime"}

    def test_annotate_kwargs(self):
        d = AnnotatedDict(name="john").annotate(color="green", flavor="lime")
        assert d == {"name": "john"}
        assert d.annotations == {"color": "green", "flavor": "lime"}

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (AnnotatedDict(name="john"), [None, None, None]),
            (AnnotatedDict(name="john").annotate(color="green"), [None, "green", None]),
            (dict(name="john", color="green"), [None, None, None]),
            (["john", "green"], [None, None, None]),
            (123, [None, None, None]),
        ],
    )
    def test_get_annotation(self, data, expected):
        keys = ["name", "color", "flavor"]
        assert [AnnotatedDict.get_annotation(data, key) for key in keys] == expected

    def test_json(self):
        d = AnnotatedDict(name="john").annotate(color="green")
        assert json.dumps(d) == '{"name": "john"}'
        assert d.annotations == {"color": "green"}

    def test_comparison(self):
        d0 = {"name": "john"}
        d1 = AnnotatedDict(name="john").annotate(color="green")
        d2 = AnnotatedDict(name="john").annotate(color="blue")
        assert d0 == d1
        assert d0 == d2
        # Annotations do not contribute to equality at the dict surface,
        # but have to be explicitly checked.
        assert d1 == d2
        assert d1.annotations != d2.annotations


class TestBadlyHashable:
    def test_hash_is_constant(self):
        assert hash(BadlyHashable([1, 2])) == hash(BadlyHashable([3, 4]))

    def test_equality(self):
        assert BadlyHashable([1, 2]) == BadlyHashable([1, 2])
        assert BadlyHashable([1, 2]) != BadlyHashable([3, 4])
        assert BadlyHashable([1, 2]) != [1, 2]

    def test_usable_as_dict_key(self):
        cache = {}
        cache[BadlyHashable({"a": 1})] = "first"
        assert cache[BadlyHashable({"a": 1})] == "first"

    def test_repr(self):
        assert repr(BadlyHashable([1, 2])) == "BadlyHashable([1, 2])"


def test_reproject_geometry():
    point = Point(500000, 5661139.2)  # UTM 31N
    reprojected = reproject_geometry(point, src_crs="EPSG:32631", dst_crs="EPSG:4326")
    assert reprojected.x == pytest.approx(3.0, abs=1e-6)
    assert reprojected.y == pytest.approx(51.101743060727806, abs=1e-6)


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
def test_to_s3_url(file_or_folder_path, bucket_name, expected_url):
    assert to_s3_url(file_or_folder_path, bucketname=bucket_name) == expected_url
