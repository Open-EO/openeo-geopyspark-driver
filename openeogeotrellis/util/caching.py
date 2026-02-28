import collections

from typing import Dict, Any, Optional, Callable


class GetOrCallCacheInterface:
    """
    Interface for "memoizing" function calls with an explicit cache key.

    Unlike the well known `functools.lru` cache,
    it requires to specify an explicit cache key,
    which gives more control over how to handle arguments, e.g.:
    - serialize or transform arguments that are not hashable by default
    - ignore cetain arguments
    - add cache key components that are not arguments to the function

    Usage example:

         result = cache.get_or_call(
            key=(name, str(sorted(options.items())),
            callable=lambda : function_to_cache(name, options)
        )

    """

    def get_or_call(self, key, callback: Callable[[], Any]) -> Any:
        """
        Try to get item from cache.
        If not available: call callable to build it and store result in cache.

        :param key: key to store item at (can be a simple string,
            or something more complex like a tuple of strings/ints)
        :param callback: item builder to call when item is not in cache
        :return: item (from cache or freshly built)
        """
        raise NotImplementedError()


class AlwaysCallWithoutCache(GetOrCallCacheInterface):
    """Don't cache, just call"""

    def get_or_call(self, key, callback: Callable[[], Any]) -> Any:
        return callback()


class GetOrCallCache(GetOrCallCacheInterface):
    """
    In-memory dictionary based cache for "memoizing" function calls.

    Supports a maximum size by pruning least frequently used items.
    """

    # TODO: support least recently used (LRU) pruning strategy as well?

    def __init__(self, max_size: Optional[int] = None):
        self._cache = {}
        self._usage_stats = {}
        self._max_size = max_size
        self._stats = collections.defaultdict(int)

    def get_or_call(self, key, callback: Callable[[], Any]) -> Any:
        self._stats["get_or_call"] += 1
        if key in self._cache:
            self._usage_stats[key] += 1
            self._stats["hit"] += 1
            return self._cache[key]
        else:
            value = callback()
            if self._ensure_capacity_for(count=1):
                self._cache[key] = value
                self._usage_stats[key] = 1
            self._stats["miss"] += 1
            return value

    def _ensure_capacity_for(self, count: int = 1) -> bool:
        """
        Make sure there is room (according to max_size setting)
        for adding the given number of new items
        by pruning least used items from the cache if necessary.
        """
        if self._max_size is not None:
            to_prune = len(self._cache) + count - self._max_size
            if to_prune > 0:
                least_used_keys = sorted(self._usage_stats, key=self._usage_stats.get)[:to_prune]
                for key in least_used_keys:
                    del self._cache[key]
                    del self._usage_stats[key]
                    self._stats["prune"] += 1
            return len(self._cache) + count <= self._max_size
        else:
            return True

    def stats(self) -> dict:
        return dict(self._stats)

    def __str__(self):
        return f"<GetOrCallCachesize size={len(self._cache)} stats={dict(self._stats)}>"

    def clear(self):
        self._cache.clear()
