"""
Focused unit tests for `openeogeotrellis.stac.target_resolution`, exercised
through its own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
from types import SimpleNamespace

from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.stac.opensearch_features import ResolutionTracker
from openeogeotrellis.stac.target_resolution import (
    apply_load_params_overrides,
    determine_cell_size,
    determine_target_epsg,
)

BBOX = BoundingBox(west=3, south=51, east=4, north=52, crs=4326)


def _tracker(*, epsg: int, res=(10.0, 10.0), key="band1") -> ResolutionTracker:
    tracker = ResolutionTracker()
    tracker.track(key=key, epsg=epsg, res=res)
    return tracker


class TestDetermineTargetEpsg:
    def test_single_epsg_from_tracker(self):
        tracker = _tracker(epsg=32631)
        epsg = determine_target_epsg(
            resolution_tracker=tracker, observed_epsgs=set(), source_band_names=["band1"], target_bbox=BBOX
        )
        assert epsg == 32631

    def test_single_epsg_from_observed_when_no_resolution_info(self):
        epsg = determine_target_epsg(
            resolution_tracker=ResolutionTracker(),
            observed_epsgs={4326},
            source_band_names=["band1"],
            target_bbox=BBOX,
        )
        assert epsg == 4326

    def test_fallback_to_best_utm(self):
        epsg = determine_target_epsg(
            resolution_tracker=ResolutionTracker(), observed_epsgs=set(), source_band_names=["band1"], target_bbox=BBOX
        )
        assert epsg == BBOX.best_utm()


class TestDetermineCellSize:
    def test_finest_resolution_from_tracker(self):
        tracker = _tracker(epsg=32631, res=(20.0, 20.0))
        cell_width, cell_height = determine_cell_size(
            resolution_tracker=tracker,
            observed_epsgs=set(),
            source_band_names=["band1"],
            target_epsg=32631,
            target_bbox=BBOX,
            feature_flags={},
        )
        assert (cell_width, cell_height) == (20.0, 20.0)

    def test_cellsize_override_feature_flag(self):
        tracker = _tracker(epsg=32631, res=(20.0, 20.0))
        cell_width, cell_height = determine_cell_size(
            resolution_tracker=tracker,
            observed_epsgs=set(),
            source_band_names=["band1"],
            target_epsg=32631,
            target_bbox=BBOX,
            feature_flags={"cellsize_override": (5.0, 5.0)},
        )
        assert (cell_width, cell_height) == (5.0, 5.0)

    def test_hardcoded_10m_fallback_for_utm(self):
        cell_width, cell_height = determine_cell_size(
            resolution_tracker=ResolutionTracker(),
            observed_epsgs={32631},
            source_band_names=["band1"],
            target_epsg=32631,
            target_bbox=BBOX,
            feature_flags={},
        )
        assert (cell_width, cell_height) == (10.0, 10.0)


class TestApplyLoadParamsOverrides:
    def test_no_overrides(self):
        load_params = SimpleNamespace(target_resolution=None, target_crs=None)
        result = apply_load_params_overrides(
            cell_width=10.0, cell_height=10.0, target_epsg=32631, target_bbox=BBOX, load_params=load_params
        )
        assert result == (10.0, 10.0, 32631)

    def test_target_resolution_override(self):
        load_params = SimpleNamespace(target_resolution=(20.0, 20.0), target_crs=None)
        result = apply_load_params_overrides(
            cell_width=10.0, cell_height=10.0, target_epsg=32631, target_bbox=BBOX, load_params=load_params
        )
        assert result == (20.0, 20.0, 32631)

    def test_target_crs_override_requires_target_resolution(self):
        # target_crs alone (without a non-zero target_resolution) is not applied.
        load_params = SimpleNamespace(target_resolution=None, target_crs=4326)
        result = apply_load_params_overrides(
            cell_width=10.0, cell_height=10.0, target_epsg=32631, target_bbox=BBOX, load_params=load_params
        )
        assert result == (10.0, 10.0, 32631)

    def test_target_crs_and_resolution_override(self):
        load_params = SimpleNamespace(target_resolution=(0.01, 0.01), target_crs=4326)
        result = apply_load_params_overrides(
            cell_width=10.0, cell_height=10.0, target_epsg=32631, target_bbox=BBOX, load_params=load_params
        )
        assert result == (0.01, 0.01, 4326)
