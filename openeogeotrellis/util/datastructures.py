"""
Generic data structure manipulations
"""


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
