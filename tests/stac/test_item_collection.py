"""
Focused unit tests for `openeogeotrellis.stac.item_collection`, exercised
through its own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import json

import pystac.stac_io
import pytest

from openeogeotrellis.stac.item_collection import LiveStacSourceResolver
from openeogeotrellis.stac.stac_object_fetching import PollingConfig


class _NeverCompletingStacIO(pystac.stac_io.StacIO):
    """Always reports a still-running partial batch job result."""

    def read_text(self, source, *args, **kwargs) -> str:
        return json.dumps(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": "never-done",
                "properties": {"datetime": "2024-01-01T00:00:00Z"},
                "geometry": None,
                "links": [],
                "assets": {},
                "openeo:status": "running",
            }
        )

    def write_text(self, dest, txt, *args, **kwargs) -> None:
        raise NotImplementedError


def test_live_stac_source_resolver_times_out_on_never_completing_source():
    resolver = LiveStacSourceResolver(
        stac_io=_NeverCompletingStacIO(),
        polling=PollingConfig(poll_interval_seconds=0.01, max_poll_delay_seconds=0.05),
    )
    with pytest.raises(Exception, match="was not satisfied after"):
        resolver.resolve("https://stac.test/never-done", spatiotemporal_extent=None)
