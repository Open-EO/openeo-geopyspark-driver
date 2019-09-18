import pytest

from openeogeotrellis.utils import dict_merge_recursive


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
