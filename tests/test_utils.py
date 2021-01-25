import getpass
from pathlib import Path

import pytest

from openeogeotrellis.utils import dict_merge_recursive, describe_path, lonlat_to_mercator_tile_indices, nullcontext


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
    with pytest.raises(ValueError):
        result = dict_merge_recursive(a, b)
    result = dict_merge_recursive(a, b, overwrite=True)
    assert result == expected


def test_merge_recursive_preserve_input():
    a = {1: {2: 3}}
    b = {1: {4: 5}}
    result = dict_merge_recursive(a, b)
    assert result == {1: {2: 3, 4: 5}}
    assert a == {1: {2: 3}}
    assert b == {1: {4: 5}}


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
