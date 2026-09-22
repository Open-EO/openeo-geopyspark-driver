"""
Generic data structure manipulations
"""

from typing import Iterable, Any, Optional, Hashable, Mapping


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


class NoveltyTracker:
    """Utility to detect new things."""

    def __init__(self):
        self._seen: set = set()

    def is_new(self, x) -> bool:
        """Check if the item is new (not seen before)."""
        if isinstance(x, list):
            key = tuple(x)
        else:
            # TODO: wider coverage to make the thing hashable
            key = x
        if key in self._seen:
            return False
        else:
            self._seen.add(key)
            return True

    def already_seen(self, x) -> bool:
        """Check if the item was seen before."""
        return not self.is_new(x)
