import time


class TrackingIter:
    """
    Pass-through wrapper for iterables to collects simple stats (item count, elapsed time)
    while iterating over an iterable.

    Usage example:

        tracking_iter = IterationStats()
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
        return f"TrackingIter:{self.count}/{self.elapsed:.2f}s"

    def __call__(self, iterable):
        self.count = 0
        start = time.time()
        for item in iterable:
            self.count += 1
            yield item
        self.elapsed = time.time() - start
