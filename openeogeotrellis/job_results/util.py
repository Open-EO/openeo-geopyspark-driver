"""
Small, dependency-free helpers shared across the ``job_results`` package.
"""
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Union

import pyproj
from shapely.ops import transform

GDALINFO_SUFFIX = "_gdalinfo.json"


def json_default(obj: Any) -> Any:
    """default function for packing objects in JSON."""
    if isinstance(obj, Path):
        return str(obj)

    raise TypeError("%r is not JSON serializable" % obj)


def parse_json_from_output(output_str: str) -> Dict[str, Any]:
    lines = output_str.split("\n")
    parsing_json = False
    json_str = ""
    # reverse order to get last possible json line
    for l in reversed(lines):
        if not parsing_json:
            if l.endswith("}"):
                parsing_json = True
        json_str = l + json_str
        if l.startswith("{"):
            break

    return json.loads(json_str)


def make_set_for_key(
    data: Dict[str, Dict[str, Any]],
    key: str,
    func: callable = lambda x: x,
) -> set:
    """
    Create a set containing only the values for `key` from the dicts in data.values().

    Optionally apply func() to that value, for example to allow converting lists,
    which are not hashable and cannot be a set element, to tuples.
    """
    return {func(val.get(key)) for val in data.values() if key in val}


def _to_jsonable_float(x: float) -> Union[float, str]:
    """Replaces nan, inf and -inf with its string representation to allow JSON serialization."""
    return x if math.isfinite(x) else str(x)


def to_jsonable(x):
    if isinstance(x, float):
        return _to_jsonable_float(x)
    if isinstance(x, dict):
        return {to_jsonable(key): to_jsonable(value) for key, value in x.items()}
    elif isinstance(x, list):
        return [to_jsonable(elem) for elem in x]

    return x


def unzip(*iterables: Iterable) -> Iterator:
    # iterables are typically of equal length
    return zip(*iterables)


def md5_checksum(file: Path) -> str:
    """Computes the MD5 checksum of a (potentially large) file."""

    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class AnnotatedDict(dict):
    """
    dict subclass with room for additional annotations (extra fields basically)
    that are automatically ignored by utilities that only consider the standard dict API,

    Use case examples:
    - annotations will be ignored when comparing with another dictionary
    - pass around a dict with some extra fields that should not be included
      in JSON serialization at some later point.

    Note: to avoid any conflict, annotations are not part of the `__init__` API,
    but still can be added fluently with method chaining of the `annotate` method.
    """

    def __init__(self, *args, **kwargs):
        self.annotations = {}
        super().__init__(*args, **kwargs)

    def annotate(self, other: Optional[dict] = None, /, **kwargs):
        """
        Fluent method to add annotations, e.g. chained directly on `__init__`.
        Works like `dict.update`, but then on the annotations dict:
        - pass a single dict or iterable of pairs: `.annotate(d)`
        - or use keyword args: `.annotate(color="green")
        """
        if other:
            self.annotations.update(other)
        if kwargs:
            self.annotations.update(kwargs)
        return self

    @classmethod
    def get_annotation(cls, data: Any, key: str, *, default: Any = None) -> Any:
        """
        Helper to get an annotation value from given input when it's an AnnotatedDict.
        Return default value otherwise.
        """
        if isinstance(data, cls):
            return data.annotations.get(key, default)
        else:
            return default


class BadlyHashable:
    """
    Simplifies implementation by allowing unhashable types in a dict-based cache. The number of
    items in this cache is very small anyway.
    """

    def __init__(self, target):
        self.target = target

    def __eq__(self, other):
        equal = isinstance(other, BadlyHashable) and self.target == other.target
        return equal

    def __hash__(self):
        return 0

    def __repr__(self):
        return f"BadlyHashable({repr(self.target)})"


def reproject_geometry(geometry, src_crs, dst_crs):
    """Kind of like reprojectAsPolygon but the number of points remains the same."""

    transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return transform(transformer.transform, geometry)


def to_s3_url(file_or_dir_name: Union[Path, str], bucketname: str) -> str:
    """Get a URL for S3 to the file or directory, in the correct format."""
    # See also:
    # https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html
    #
    # file_or_dir_name, is actually the S3 key, and it should neither start nor
    # end with a slash in order to keep the S3 keys and S3 URLs uniform.
    #
    # 1) With / at the start we would get weird URLS with a // after bucketname,
    # like so: s3://my-bucket//path-to-file-or-dir
    #
    # 2) Allowing folders to end with a slash just creates confusion.
    # It keeps things simpler when S3 keys never include a slash at the end.
    file_or_dir_name = str(file_or_dir_name).strip("/")

    # Keep it robust: bucketname should not contain "/" at all but lets remove
    # the / just in case, because mistakes are easy to make.
    bucketname = bucketname.strip("/")
    return f"s3://{bucketname}/{file_or_dir_name}"
