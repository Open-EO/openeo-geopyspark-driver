import json
from pathlib import Path
from typing import List, Optional

import pytest
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import JSONResult, SaveResult

from openeogeotrellis.job_results.finalize import finalize_job, write_failure_metadata
from openeogeotrellis.job_results.settings import JobResultsSettings, ResultCubeMetadata


def _settings(**overrides) -> JobResultsSettings:
    values = dict(
        job_id="j-123",
        stac11_mode=False,
        omit_derived_from_links=False,
        detailed_asset_metadata=True,
        concurrent_save_results=1,
        remove_exported_assets=False,
        export_workspace_enable_merge=False,
        max_soft_errors_ratio=0.0,
        institution="VITO - dummy-version",
        processing_facility="VITO - SPARK",
        processing_software="openeo-geotrellis-dummy-version",
        provider={},
        job_local_href_format="file",
        s3_bucket_name=None,
        gdalinfo_from_file=True,
        gdalinfo_use_subprocess=False,
        item_collection_glob="items_*.json",
    )
    values.update(overrides)
    return JobResultsSettings(**values)


class _RecordingHooks:
    """A recording fake `JobResultsHooks`: no JVM, no deployment branching."""

    def __init__(self):
        self.calls: List[tuple] = []

    def result_cube_metadata(self, result: SaveResult) -> ResultCubeMetadata:
        self.calls.append(("result_cube_metadata", result))
        return ResultCubeMetadata(epsg=None, instruments=[])

    def usage_metadata(self, *, omit_derived_from_links: bool = False) -> dict:
        self.calls.append(("usage_metadata", omit_derived_from_links))
        return {"usage": {}, "links": [], "auxiliary_links": []}

    def prepare_result_options(self, result: SaveResult) -> None:
        self.calls.append(("prepare_result_options", result))

    def after_assets_written(self, assets_metadata: List[dict], job_dir: Path) -> None:
        self.calls.append(("after_assets_written", list(assets_metadata), job_dir))

    def localize_asset(self, href: str, job_dir: Path) -> Optional[Path]:
        self.calls.append(("localize_asset", href, job_dir))
        return None

    def output_href(self, href: str) -> str:
        return href

    def publish_metadata_file(self, metadata_file: Path) -> None:
        self.calls.append(("publish_metadata_file", metadata_file))

    def publish_auxiliary_file(self, path: Path, job_dir: Path, *, for_export_workspace: bool) -> str:
        self.calls.append(("publish_auxiliary_file", path, job_dir, for_export_workspace))
        return f"file://{path}"

    def summarize_exception(self, e: Exception) -> str:
        return f"summarized: {e}"

    def call_names(self) -> List[str]:
        return [call[0] for call in self.calls]


class _SoftErrorHooks(_RecordingHooks):
    def __init__(self, soft_error_ratio: float):
        super().__init__()
        self._soft_error_ratio = soft_error_ratio

    def usage_metadata(self, *, omit_derived_from_links: bool = False) -> dict:
        self.calls.append(("usage_metadata", omit_derived_from_links))
        return {
            "usage": {"sar_backscatter_soft_errors": {"value": self._soft_error_ratio, "unit": "fraction"}},
            "links": [],
            "auxiliary_links": [],
        }


class _FailingResult(SaveResult):
    def write_assets(self, directory) -> dict:
        raise RuntimeError("write_assets boom")


class _FakeResult(SaveResult):
    """A `SaveResult` that writes a single, uniquely-named asset (unlike `JSONResult`,
    which always writes to the same "result.json" regardless of which result it is)."""

    def __init__(self, asset_name: str, content: dict, **kwargs):
        super().__init__(**kwargs)
        self._asset_name = asset_name
        self._content = content

    def write_assets(self, directory) -> dict:
        output_dir = Path(directory).parent
        asset_path = output_dir / self._asset_name
        asset_path.write_text(json.dumps(self._content))
        return {self._asset_name: {"href": str(asset_path), "roles": ["data"], "type": "application/json"}}


class _GeometryTracer(DryRunDataTracer):
    def __init__(self, geometry=None):
        super().__init__()
        self._geometry = geometry

    def get_last_geometry(self, operation="aggregate_spatial"):
        return self._geometry


def _job_paths(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    output_file = job_dir / "openEO.json"
    metadata_file = job_dir / "job_metadata.json"
    return job_dir, output_file, metadata_file


def _read_metadata(metadata_file: Path) -> dict:
    return json.loads(metadata_file.read_text())


def test_single_result(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    finalize_job(
        JSONResult({"a": 1}),
        tracer=DryRunDataTracer(),
        process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
        job_specification={"process_graph": {}},
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=_settings(),
        hooks=hooks,
        workspace_repository=None,
    )

    metadata = _read_metadata(metadata_file)
    assert list(metadata["assets"].keys()) == ["result.json"]
    assert "after_assets_written" in hooks.call_names()
    # early, mid and final writes
    assert hooks.call_names().count("publish_metadata_file") == 3


def test_multiple_results(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    finalize_job(
        [_FakeResult("a.json", {"a": 1}), _FakeResult("b.json", {"b": 2})],
        tracer=DryRunDataTracer(),
        process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
        job_specification={"process_graph": {}},
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=_settings(),
        hooks=hooks,
        workspace_repository=None,
    )

    metadata = _read_metadata(metadata_file)
    assert set(metadata["assets"].keys()) == {"a.json", "b.json"}


def test_threaded_writing(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    finalize_job(
        [_FakeResult("a.json", {"a": 1}), _FakeResult("b.json", {"b": 2})],
        tracer=DryRunDataTracer(),
        process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
        job_specification={"process_graph": {}},
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=_settings(concurrent_save_results=2),
        hooks=hooks,
        workspace_repository=None,
    )

    metadata = _read_metadata(metadata_file)
    assert set(metadata["assets"].keys()) == {"a.json", "b.json"}


def test_stac11_mode(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    finalize_job(
        JSONResult({"a": 1}),
        tracer=DryRunDataTracer(),
        process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
        job_specification={"process_graph": {}},
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=_settings(stac11_mode=True),
        hooks=hooks,
        workspace_repository=None,
    )

    metadata = _read_metadata(metadata_file)
    assert len(metadata["items"]) == 1
    item = metadata["items"][0]
    assert list(item["assets"].keys()) == ["result.json"]


def test_sample_by_feature_with_geometry(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()
    result = JSONResult({"a": 1})
    result.options["sample_by_feature"] = True
    geometry = object()

    finalize_job(
        result,
        tracer=_GeometryTracer(geometry=geometry),
        process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
        job_specification={"process_graph": {}},
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=_settings(),
        hooks=hooks,
        workspace_repository=None,
    )

    assert result.options["geometries"] is geometry


def test_sample_by_feature_without_geometry(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()
    result = JSONResult({"a": 1})
    result.options["sample_by_feature"] = True

    finalize_job(
        result,
        tracer=_GeometryTracer(geometry=None),
        process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
        job_specification={"process_graph": {}},
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=_settings(),
        hooks=hooks,
        workspace_repository=None,
    )

    assert "geometries" not in result.options


def test_soft_error_ratio_exceeded(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _SoftErrorHooks(soft_error_ratio=0.5)

    with pytest.raises(ValueError, match="Too many soft errors"):
        finalize_job(
            JSONResult({"a": 1}),
            tracer=DryRunDataTracer(),
            process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
            job_specification={"process_graph": {}},
            job_dir=job_dir,
            output_file=output_file,
            metadata_file=metadata_file,
            settings=_settings(max_soft_errors_ratio=0.1),
            hooks=hooks,
            workspace_repository=None,
        )

    # the `finally` write still happened
    assert metadata_file.exists()


def test_failure_during_asset_writing_still_writes_metadata(tmp_path):
    job_dir, output_file, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    with pytest.raises(RuntimeError, match="write_assets boom"):
        finalize_job(
            _FailingResult(),
            tracer=DryRunDataTracer(),
            process_graph={"nop": {"process_id": "discard_result", "arguments": {}, "result": True}},
            job_specification={"process_graph": {}},
            job_dir=job_dir,
            output_file=output_file,
            metadata_file=metadata_file,
            settings=_settings(),
            hooks=hooks,
            workspace_repository=None,
        )

    # the early write succeeded and the `finally` write re-wrote the same (still empty) metadata
    metadata = _read_metadata(metadata_file)
    assert metadata.get("assets", {}) == {}
    assert hooks.call_names().count("publish_metadata_file") == 2


def test_write_failure_metadata(tmp_path):
    _, _, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    write_failure_metadata(metadata_file=metadata_file, settings=_settings(), hooks=hooks)

    metadata = _read_metadata(metadata_file)
    assert metadata.get("assets", {}) == {}
    assert hooks.call_names() == ["usage_metadata", "publish_metadata_file"]


def test_write_failure_metadata_stac11_mode(tmp_path):
    _, _, metadata_file = _job_paths(tmp_path)
    hooks = _RecordingHooks()

    write_failure_metadata(metadata_file=metadata_file, settings=_settings(stac11_mode=True), hooks=hooks)

    metadata = _read_metadata(metadata_file)
    assert metadata["items"] == []
