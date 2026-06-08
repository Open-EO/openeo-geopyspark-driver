from __future__ import annotations
import time
from typing import Iterable, Callable, Optional


class TrackingIter:
    """
    Pass-through wrapper for iterables to collects simple stats (item count, elapsed time)
    while iterating over an iterable.

    Usage example:

        tracking_iter = TrackingIter()
        consumed = [
            consume(item)
            for item in tracking_iter(produce_items())
        ]
        print(f"Consumed {tracking_iter.count} items in {tracking_iter.elapsed} seconds")
    """

    # TODO: move this generic utility to openeo-python-driver/openeo-python-client

    __slots__ = ("count", "elapsed")

    def __init__(self):
        self.count = None
        self.elapsed = None

    def __str__(self) -> str:
        return f"TrackingIter:{self.summary()}"

    def __call__(self, iterable: Iterable, on_done: Optional[Callable[[TrackingIter], None]] = None):
        self.count = 0
        start = time.time()
        for item in iterable:
            self.count += 1
            yield item
        self.elapsed = time.time() - start

        if on_done:
            on_done(self)

    def summary(self) -> str:
        return f"{self.count}/{self.elapsed:.2f}s"


def tracking_iter(iterable: Iterable, on_done: Callable[[TrackingIter], None] = print):
    """
    Compact single call variant of `TrackingIter`, where the printing/logging
    of the tracking stats can be provided as an `on_done` callable:

        consumed = [
            consume(item)
            for item in tracking_iter(
                produce_items(),
                on_done=lambda i: log.info(f"Consume stats: {i!s}")
            )
        ]
    """
    return TrackingIter()(iterable, on_done=on_done)
