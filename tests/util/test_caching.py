import pytest
import itertools
from openeogeotrellis.util.caching import GetOrCallCache


class TestGetOrCallCache:
    @pytest.fixture
    def call_counter(self):
        """Fixture for a function that returns the number of times it has been called."""
        return itertools.count(1).__next__

    def test_call_counter(self, call_counter):
        assert call_counter() == 1
        assert call_counter() == 2

    def test_basic(self, call_counter):
        cache = GetOrCallCache()
        assert cache.get_or_call(key="a", callback=call_counter) == 1
        assert cache.get_or_call(key="a", callback=call_counter) == 1
        assert cache.get_or_call(key="b", callback=call_counter) == 2
        assert cache.get_or_call(key="a", callback=call_counter) == 1
        assert cache.get_or_call(key="c", callback=call_counter) == 3

        assert cache.stats() == {"get_or_call": 5, "hit": 2, "miss": 3}

    def test_max_size(self, call_counter):
        cache = GetOrCallCache(max_size=2)
        assert cache.get_or_call(key="a", callback=call_counter) == 1
        assert cache.get_or_call(key="a", callback=call_counter) == 1
        assert cache.get_or_call(key="b", callback=call_counter) == 2
        assert cache.get_or_call(key="c", callback=call_counter) == 3

        # At this point, "b" should have been pruned from the cache, so it should be called again:
        assert cache.get_or_call(key="b", callback=call_counter) == 4

        assert cache.stats() == {"get_or_call": 5, "hit": 1, "miss": 4, "prune": 2}

    def test_max_size_0(self, call_counter):
        """max_size=0 means no caching, so every call should invoke the callback."""
        cache = GetOrCallCache(max_size=0)
        assert cache.get_or_call(key="a", callback=call_counter) == 1
        assert cache.get_or_call(key="a", callback=call_counter) == 2
        assert cache.get_or_call(key="b", callback=call_counter) == 3

        assert cache.stats() == {"get_or_call": 3, "miss": 3}

    def test_key_serialization(self, call_counter):
        cache = GetOrCallCache()

        name = "alice"
        options = {1: "one", 2: "two", 3: "three"}

        def fun(name: str, options: dict):
            return f"{name} {options[call_counter()]}"

        assert (
            cache.get_or_call(
                key=(name, str(sorted(options.items()))),
                callback=lambda: fun(name, options),
            )
            == "alice one"
        )
        assert (
            cache.get_or_call(
                key=(name, str(sorted(options.items()))),
                callback=lambda: fun(name, options),
            )
            == "alice one"
        )

        # Change options order
        options = {3: "three", 2: "two", 1: "one"}
        assert (
            cache.get_or_call(
                key=(name, str(sorted(options.items()))),
                callback=lambda: fun(name, options),
            )
            == "alice one"
        )

        # Change name too
        name = "bob"
        assert (
            cache.get_or_call(
                key=(name, str(sorted(options.items()))),
                callback=lambda: fun(name, options),
            )
            == "bob two"
        )

    def test_str_stats(self):
        cache = GetOrCallCache()
        assert str(cache) == "<GetOrCallCachesize size=0 stats={}>"

        cache.get_or_call(key="a", callback=lambda: "A")
        assert str(cache) == "<GetOrCallCachesize size=1 stats={'get_or_call': 1, 'miss': 1}>"
        cache.get_or_call(key="a", callback=lambda: "A")
        assert str(cache) == "<GetOrCallCachesize size=1 stats={'get_or_call': 2, 'miss': 1, 'hit': 1}>"
