import pytest
from time_machine import TimeMachineFixture

from openeogeotrellis.util.logging import TrackingIter


class TestTrackingIter:
    @pytest.mark.parametrize("factory", [list, tuple, set, iter])
    def test_basic(self, factory):
        iterable = factory([1, 2, 3])
        tracking_iter = TrackingIter()
        total = sum(x for x in tracking_iter(iterable))
        assert total == 6
        assert tracking_iter.count == 3
        assert 0 <= tracking_iter.elapsed < 0.1

    def test_elapsed(self, time_machine: TimeMachineFixture):
        time_machine.move_to("2026-01-01T00:00:00Z")
        data = [1, 2, 3]
        tracking_iter = TrackingIter()
        iterable = tracking_iter(data)
        time_machine.move_to("2026-01-01T01:02:03Z")
        assert next(iterable) == 1
        time_machine.move_to("2026-01-01T01:03:13Z")
        assert next(iterable) == 2
        time_machine.move_to("2026-01-01T01:05:33Z")
        assert next(iterable) == 3
        with pytest.raises(StopIteration):
            next(iterable)
        assert tracking_iter.count == 3
        assert tracking_iter.elapsed == 210

    def test_str(self, time_machine: TimeMachineFixture):
        time_machine.move_to("2026-01-01T00:00:00Z")
        tracking_iter = TrackingIter()
        for item in tracking_iter(range(4)):
            time_machine.move_to(f"2026-01-01T00:00:{item*item:02d}Z")

        assert str(tracking_iter) == "TrackingIter:4/9.00s"

    def test_reuse(self):
        tracking_iter = TrackingIter()
        assert sum(tracking_iter(range(4))) == 6
        assert tracking_iter.count == 4
        assert sum(tracking_iter(range(10))) == 45
        assert tracking_iter.count == 10
