"""
Exports a batch job result to the workspaces requested through
`save_result(..., export_workspace=...)`, pre-1.1 and STAC 1.1.
"""
import json
from functools import partial
from pathlib import Path
from typing import Callable, List, Optional, Union
from urllib.parse import urlparse

import pystac
from openeo_driver.backend import BatchJobs
from openeo_driver.save_result import SaveResult
from openeo_driver.util.stac_utils import find_stac_root, get_files_from_stac_catalog
from openeo_driver.workspacerepository import Workspace, WorkspaceRepository

from .stac_export import write_exported_stac_collection, write_exported_stac_collection_from_item


def export_result_to_workspaces(
    result: SaveResult,
    result_metadata: dict,
    *,
    stac11_mode: bool,
    workspace_repository: WorkspaceRepository,
    job_dir: Path,
    job_id: str,
    remove_exported_assets: bool,
    enable_merge: bool,
    omit_derived_from_links: bool = False,
    attach_derived_from_document: bool = False,
    result_assets_metadata: Optional[dict] = None,
    result_items_metadata: Optional[dict] = None,
    usage_metadata: Callable[..., dict],
    copy_auxiliary_links: Callable[..., List[dict]],
) -> None:
    workspace_exports = sorted(
        list(result.workspace_exports),
        key=lambda export: export.workspace_id + (export.merge or ""),  # arbitrary but deterministic order of hrefs
    )

    if not workspace_exports:
        return

    if stac11_mode:
        stac_hrefs = [
            f"file:{path}"
            for path in write_exported_stac_collection_from_item(
                job_dir,
                result_metadata,
                item_metadata=result_items_metadata,
                omit_derived_from_links=omit_derived_from_links,
                attach_derived_from_document=attach_derived_from_document,
                job_id=job_id,
                usage_metadata=usage_metadata,
                copy_auxiliary_links=copy_auxiliary_links,
            )
        ]
    elif getattr(result, "stac_root_local", None) is not None:
        # a StacSaveResult, detected by duck typing so this package doesn't need to import it.
        stac_hrefs_raw = get_files_from_stac_catalog(result.stac_root_local, include_metadata=True)
        stac_hrefs = [href for href in stac_hrefs_raw if href.endswith(".json")] + [result.stac_root_local]
    else:
        stac_hrefs = [
            f"file:{path}"
            for path in write_exported_stac_collection(
                job_dir,
                result_metadata,
                asset_keys=list(result_assets_metadata.keys()),
                omit_derived_from_links=omit_derived_from_links,
                job_id=job_id,
                usage_metadata=usage_metadata,
            )
        ]

    # TODO: assemble pystac.STACObject and avoid file altogether?
    collection_href = find_stac_root(stac_hrefs)
    assert collection_href is not None
    collection_href_path = urlparse(collection_href).path
    collection_href_dict = json.loads(Path(collection_href_path).read_text())
    if pystac.Collection.matches_object_type(collection_href_dict):
        collection = pystac.Collection.from_file(collection_href_path)
    else:
        collection = pystac.Catalog.from_file(collection_href_path)

    workspace_uris = {}

    for i, workspace_export in enumerate(workspace_exports):
        workspace: Workspace = workspace_repository.get_by_id(workspace_export.workspace_id)
        merge = workspace_export.merge

        if merge is None:
            merge = job_id
        elif merge == "":  # TODO: puts it in root of workspace? move it there?
            merge = "."

        final_export = i >= len(workspace_exports) - 1
        remove_original = remove_exported_assets and final_export

        if enable_merge or workspace.merges_by_default:
            imported_collection = workspace.merge(collection, target=Path(merge), remove_original=remove_original)
            assert isinstance(imported_collection, pystac.Collection)

            for item in imported_collection.get_items(recursive=True):
                item_key = item.id if stac11_mode else None
                for asset_key, asset in item.get_assets().items():
                    (workspace_uri,) = asset.extra_fields["alternate"].values()
                    workspace_uris.setdefault((item_key, asset_key), []).append(
                        (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                    )
        else:
            export_to_workspace = partial(
                _export_to_workspace,
                common_path=job_dir,
                target=workspace,
                merge=merge,
                remove_original=remove_original,
            )

            for stac_href in stac_hrefs:
                # FIXME: collection.json for this result will overwrite the one for another result so
                #  multiple export_workspace to the same workspace and merge within a single process graph will not work
                export_to_workspace(source_uri=stac_href)

            if stac11_mode:
                for item_key, item in result_items_metadata.items():
                    for asset_key, asset in item["assets"].items():
                        workspace_uri = export_to_workspace(source_uri=asset["href"])
                        workspace_uris.setdefault((item_key, asset_key), []).append(
                            (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                        )
            else:
                for asset_key, asset in result_assets_metadata.items():
                    workspace_uri = export_to_workspace(source_uri=asset["href"])
                    workspace_uris.setdefault((None, asset_key), []).append(
                        (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                    )

    for (item_key, asset_key), uris in workspace_uris.items():
        asset_output = (
            result_metadata["items"][item_key]["assets"][asset_key]
            if stac11_mode
            else result_metadata["assets"][asset_key]
        )
        if remove_exported_assets:
            # the last workspace URI becomes the public_href; the rest become "alternate" hrefs
            asset_output[BatchJobs.ASSET_PUBLIC_HREF] = uris[-1][2]
            alternate = {f"{workspace_id}/{merge}": {"href": workspace_uri} for workspace_id, merge, workspace_uri in uris[:-1]}
        else:
            # the original href still applies; all workspace URIs become "alternate" hrefs
            alternate = {f"{workspace_id}/{merge}": {"href": workspace_uri} for workspace_id, merge, workspace_uri in uris}

        if alternate:
            asset_output["alternate"] = alternate


def _export_to_workspace(
    common_path: str, source_uri: Union[str, Path], target: Workspace, merge: str, remove_original: bool
) -> str:
    uri_parts = urlparse(str(source_uri))

    if not uri_parts.scheme or uri_parts.scheme.lower() == "file":
        return target.import_file(common_path, Path(uri_parts.path), merge, remove_original)
    elif uri_parts.scheme == "s3":
        return target.import_object(common_path, source_uri, merge, remove_original)
    else:
        raise ValueError(f"unsupported scheme {uri_parts.scheme} for {source_uri}; supported are: file, s3")
