import json

from openeogeotrellis.util.datastructures import AnnotatedDict
import pytest


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
