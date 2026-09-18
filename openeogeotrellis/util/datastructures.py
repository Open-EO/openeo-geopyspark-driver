"""
Generic data structure manipulations
"""

import collections.abc


def dict_merge_recursive(a: collections.abc.Mapping, b: collections.abc.Mapping, overwrite=False) -> collections.abc.Mapping:
    """
    Merge two dictionaries recursively

    :param a: first dictionary
    :param b: second dictionary
    :param overwrite: whether values of b can overwrite values of a
    :return: merged dictionary
    """
    # Start with shallow copy, we'll copy deeper parts where necessary through recursion.
    result = dict(a)
    for key, value in b.items():
        if key in result:
            if isinstance(value, collections.abc.Mapping) and isinstance(result[key], collections.abc.Mapping):
                result[key] = dict_merge_recursive(result[key], value, overwrite=overwrite)
            elif overwrite:
                result[key] = value
            elif result[key] == value:
                pass
            else:
                raise ValueError("Can not automatically merge values {a!r} and {b!r} for key {k!r}"
                                 .format(a=result[key], b=value, k=key))
        else:
            result[key] = value
    return result



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
