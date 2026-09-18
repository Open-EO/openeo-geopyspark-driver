from pathlib import Path

from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import JSONResult, NullResult

from openeogeotrellis.job_results.result_metadata import (
    assemble_result_metadata,
    convert_asset_outputs_to_s3_urls,
    href_from_job_local_path,
)
from openeogeotrellis.job_results.settings import JobResultsSettings, ResultGrid


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
        s3_bucket_name="openeo-test-bucket",
        gdalinfo_from_file=True,
        gdalinfo_use_subprocess=False,
        item_collection_glob="items_*.json",
    )
    values.update(overrides)
    return JobResultsSettings(**values)


def test_assemble_result_metadata_without_gdal():
    tracer = DryRunDataTracer()
    result = JSONResult({"a": 1})
    metadata = assemble_result_metadata(
        tracer=tracer,
        result=result,
        job_dir=Path("/tmp/job-123"),
        unique_process_ids={"load_collection", "save_result"},
        apply_gdal=False,
        result_grid=lambda r: ResultGrid(epsg=4326, instruments=["msi"]),
        settings=_settings(),
        summarize_exception=lambda e: str(e),
        extract_asset_metadata=lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not be called")),
        asset_metadata={"openEO.tif": {"href": "openEO.tif"}},
    )

    assert metadata["assets"] == {"openEO.tif": {"href": "openEO.tif"}}
    assert metadata["epsg"] == 4326
    assert metadata["instruments"] == ["msi"]
    assert metadata["processing:facility"] == "VITO - SPARK"
    assert metadata["processing:software"] == "openeo-geotrellis-dummy-version"
    assert set(metadata["unique_process_ids"]) == {"load_collection", "save_result"}
    assert metadata["providers"] == []


def test_assemble_result_metadata_null_result_has_no_assets():
    tracer = DryRunDataTracer()
    metadata = assemble_result_metadata(
        tracer=tracer,
        result=NullResult(),
        job_dir=Path("/tmp/job-123"),
        unique_process_ids=set(),
        apply_gdal=False,
        result_grid=lambda r: ResultGrid(epsg=None, instruments=[]),
        settings=_settings(),
        summarize_exception=lambda e: str(e),
        extract_asset_metadata=lambda *a, **kw: None,
        asset_metadata={"should-be-ignored.tif": {"href": "should-be-ignored.tif"}},
    )
    assert "assets" not in metadata


def test_assemble_result_metadata_apply_gdal_calls_extract_asset_metadata():
    calls = []

    def fake_extract_asset_metadata(job_result_metadata, asset_metadata, job_dir, epsg):
        calls.append((asset_metadata, job_dir, epsg))
        job_result_metadata["assets"] = asset_metadata

    tracer = DryRunDataTracer()
    metadata = assemble_result_metadata(
        tracer=tracer,
        result=JSONResult({"a": 1}),
        job_dir=Path("/tmp/job-123"),
        unique_process_ids=set(),
        apply_gdal=True,
        result_grid=lambda r: ResultGrid(epsg=None, instruments=[]),
        settings=_settings(),
        summarize_exception=lambda e: str(e),
        extract_asset_metadata=fake_extract_asset_metadata,
        asset_metadata={"openEO.tif": {"href": "openEO.tif"}},
    )

    assert calls == [({"openEO.tif": {"href": "openEO.tif"}}, Path("/tmp/job-123"), None)]
    assert metadata["assets"] == {"openEO.tif": {"href": "openEO.tif"}}


def test_assemble_result_metadata_apply_gdal_error_is_summarized(caplog):
    def failing_extract_asset_metadata(*args, **kwargs):
        raise ValueError("boom")

    tracer = DryRunDataTracer()
    metadata = assemble_result_metadata(
        tracer=tracer,
        result=JSONResult({"a": 1}),
        job_dir=Path("/tmp/job-123"),
        unique_process_ids=set(),
        apply_gdal=True,
        result_grid=lambda r: ResultGrid(epsg=None, instruments=[]),
        settings=_settings(),
        summarize_exception=lambda e: f"summarized: {e}",
        extract_asset_metadata=failing_extract_asset_metadata,
        asset_metadata={},
    )
    # the gdal extraction failure is swallowed and logged, not raised
    assert "assets" not in metadata
    assert "summarized: boom" in caplog.text


def test_convert_asset_outputs_to_s3_urls():
    job_metadata = {
        "assets": {"a.tif": {"href": "a.tif"}, "b.tif": {"href": "s3://already/there.tif"}},
        "items": [{"assets": {"c.tif": {"href": "c.tif"}}}],
    }

    result = convert_asset_outputs_to_s3_urls(job_metadata, output_href=lambda href: f"s3://bucket/{href}")

    assert result["assets"]["a.tif"]["href"] == "s3://bucket/a.tif"
    assert result["assets"]["b.tif"]["href"] == "s3://already/there.tif"
    assert result["items"][0]["assets"]["c.tif"]["href"] == "s3://bucket/c.tif"
    # original is untouched
    assert job_metadata["assets"]["a.tif"]["href"] == "a.tif"


def test_href_from_job_local_path_file():
    assert href_from_job_local_path("/tmp/aux.txt", job_local_href_format="file", s3_bucket_name=None) == (
        "file:///tmp/aux.txt"
    )


def test_href_from_job_local_path_s3():
    href = href_from_job_local_path("/tmp/aux.txt", job_local_href_format="s3", s3_bucket_name="my-bucket")
    assert href == "s3://my-bucket/tmp/aux.txt"
