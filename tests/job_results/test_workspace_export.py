from openeo_driver.save_result import JSONResult
from openeo_driver.workspace import DiskWorkspace

from openeogeotrellis.job_results.workspace_export import export_result_to_workspaces


class _FakeWorkspaceRepository:
    def __init__(self, workspaces: dict):
        self._workspaces = workspaces

    def get_by_id(self, workspace_id: str):
        return self._workspaces[workspace_id]


def _usage_metadata(*, omit_derived_from_links: bool = False) -> dict:
    return {"usage": {}, "links": [], "auxiliary_links": []}


def _copy_auxiliary_links(*, auxiliary_links, job_dir, for_export_workspace) -> list:
    return []


def _make_job_dir_and_asset(tmp_path):
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    asset_path = job_dir / "openEO.tif"
    asset_path.write_bytes(b"dummy")
    return job_dir, asset_path


def _make_result_with_export(*, merge="target"):
    result = JSONResult({})
    result.add_workspace_export(workspace_id="ws", merge=merge)
    return result


def test_export_pre11_file_by_file(tmp_path):
    job_dir, asset_path = _make_job_dir_and_asset(tmp_path)
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    workspace_repository = _FakeWorkspaceRepository({"ws": DiskWorkspace(root_directory=workspace_dir)})

    result = _make_result_with_export()
    result_metadata = {
        "assets": {"openEO.tif": {"href": str(asset_path)}},
        "bbox": [0, 0, 1, 1],
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-01-01T00:00:00Z",
    }

    export_result_to_workspaces(
        result,
        result_metadata,
        stac11_mode=False,
        workspace_repository=workspace_repository,
        job_dir=job_dir,
        job_id="j-123",
        remove_exported_assets=False,
        enable_merge=False,
        result_assets_metadata=result_metadata["assets"],
        usage_metadata=_usage_metadata,
        copy_auxiliary_links=_copy_auxiliary_links,
    )

    from openeo_driver.backend import BatchJobs

    assert (workspace_dir / "target" / "openEO.tif").exists()
    asset_output = result_metadata["assets"]["openEO.tif"]
    assert list(asset_output["alternate"].keys()) == ["ws/target"]
    assert BatchJobs.ASSET_PUBLIC_HREF not in asset_output  # remove_exported_assets was False


def test_export_pre11_remove_exported_assets(tmp_path):
    job_dir, asset_path = _make_job_dir_and_asset(tmp_path)
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    workspace_repository = _FakeWorkspaceRepository({"ws": DiskWorkspace(root_directory=workspace_dir)})

    result = _make_result_with_export()
    result_metadata = {
        "assets": {"openEO.tif": {"href": str(asset_path)}},
        "bbox": [0, 0, 1, 1],
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-01-01T00:00:00Z",
    }

    export_result_to_workspaces(
        result,
        result_metadata,
        stac11_mode=False,
        workspace_repository=workspace_repository,
        job_dir=job_dir,
        job_id="j-123",
        remove_exported_assets=True,
        enable_merge=False,
        result_assets_metadata=result_metadata["assets"],
        usage_metadata=_usage_metadata,
        copy_auxiliary_links=_copy_auxiliary_links,
    )

    from openeo_driver.backend import BatchJobs

    asset_output = result_metadata["assets"]["openEO.tif"]
    assert asset_output[BatchJobs.ASSET_PUBLIC_HREF] == f"file:{workspace_dir / 'target' / 'openEO.tif'}"
    assert "alternate" not in asset_output  # the only workspace URI became the public_href, not an alternate


def test_export_stac11_file_by_file(tmp_path):
    job_dir, asset_path = _make_job_dir_and_asset(tmp_path)
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    workspace_repository = _FakeWorkspaceRepository({"ws": DiskWorkspace(root_directory=workspace_dir)})

    result = _make_result_with_export()
    item_metadata = {
        "item1": {
            "id": "item1",
            "assets": {"openEO.tif": {"href": f"file:{asset_path}"}},
        }
    }
    result_metadata = {
        "items": item_metadata,
        "bbox": [0, 0, 1, 1],
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-01-01T00:00:00Z",
    }

    export_result_to_workspaces(
        result,
        result_metadata,
        stac11_mode=True,
        workspace_repository=workspace_repository,
        job_dir=job_dir,
        job_id="j-123",
        remove_exported_assets=False,
        enable_merge=False,
        result_items_metadata=item_metadata,
        attach_derived_from_document=True,
        usage_metadata=_usage_metadata,
        copy_auxiliary_links=_copy_auxiliary_links,
    )

    assert (workspace_dir / "target" / "openEO.tif").exists()
    alternate = result_metadata["items"]["item1"]["assets"]["openEO.tif"]["alternate"]
    assert list(alternate.keys()) == ["ws/target"]


def test_export_merge_uses_workspace_merges_by_default(tmp_path, monkeypatch):
    job_dir, asset_path = _make_job_dir_and_asset(tmp_path)
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    disk_workspace = DiskWorkspace(root_directory=workspace_dir)
    workspace_repository = _FakeWorkspaceRepository({"ws": disk_workspace})

    result = _make_result_with_export(merge="collection.json")
    result_metadata = {
        "assets": {"openEO.tif": {"href": str(asset_path)}},
        "bbox": [0, 0, 1, 1],
        "start_datetime": "2024-01-01T00:00:00Z",
        "end_datetime": "2024-01-01T00:00:00Z",
    }

    export_result_to_workspaces(
        result,
        result_metadata,
        stac11_mode=False,
        workspace_repository=workspace_repository,
        job_dir=job_dir,
        job_id="j-123",
        remove_exported_assets=False,
        enable_merge=True,
        result_assets_metadata=result_metadata["assets"],
        usage_metadata=_usage_metadata,
        copy_auxiliary_links=_copy_auxiliary_links,
    )

    assert (workspace_dir / "collection.json").exists()
    alternate = result_metadata["assets"]["openEO.tif"]["alternate"]
    assert list(alternate.keys()) == ["ws/collection.json"]
