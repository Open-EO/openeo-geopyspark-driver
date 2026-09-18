import collections.abc
import json

from openeogeotrellis.util.datastructures import AnnotatedDict, dict_merge_recursive
import pytest


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
    class EmptyMapping(collections.abc.Mapping):
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
