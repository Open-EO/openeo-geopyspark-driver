


class MetricsTracker():

    def __init__(self):
        self._trackers = {}

    def register_counter(self,name):
        from pyspark import SparkContext
        if name not in self._trackers:
            self._trackers[name] = SparkContext.getOrCreate().accumulator(0)
        return self._trackers[name]

    def add(self,name,value):
        self._trackers[name] += value

    def as_dict(self):
        return {name: tracker.value for name, tracker in self._trackers.items()}

    def clear(self):
        self._trackers = {}


_metrics_tracker = MetricsTracker()

def global_tracker():
    return _metrics_tracker