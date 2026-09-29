"""
Turns a raw process-graph evaluation result into ``job_metadata.json``,
written assets and workspace exports.

This is the result-handling half of a batch job run: everything that
happens once the process graph has been evaluated (or failed to evaluate).
"""
import concurrent.futures
import json
import logging
import uuid
from copy import deepcopy
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from openeo_driver.constants import ITEM_LINK_PROPERTY
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import MlModelResult, SaveResult
from openeo_driver.workspacerepository import WorkspaceRepository

from . import raster_metadata
from .result_metadata import (
    CollectUniqueProcessIdsVisitor,
    assemble_result_metadata,
    convert_asset_outputs_to_s3_urls,
    href_from_job_local_path,
)
from .settings import JobResultsHooks, JobResultsSettings
from .util import AnnotatedDict, BadlyHashable, json_default, unzip
from .workspace_export import export_result_to_workspaces
from .wrapping import wrap_evaluation_result

logger = logging.getLogger(__name__)


def finalize_job(
    evaluation_result: Any,
    *,
    tracer: DryRunDataTracer,
    process_graph: dict,
    job_specification: dict,
    job_dir: Path,
    output_file: Path,
    metadata_file: Path,
    settings: JobResultsSettings,
    hooks: JobResultsHooks,
    workspace_repository: WorkspaceRepository,
) -> None:
    result_metadata: dict = {}
    tracker_metadata: dict = {}
    items: List[dict] = []
    auxiliary_links_cache: Dict[Tuple[str, str, bool], str] = {}
    ml_model_metadata: Optional[Dict] = None

    def assemble(*, result: SaveResult, apply_gdal: bool, asset_metadata: Dict, result_items: Optional[List[dict]] = None) -> dict:
        return assemble_result_metadata(
            tracer=tracer,
            result=result,
            job_dir=job_dir,
            unique_process_ids=unique_process_ids,
            apply_gdal=apply_gdal,
            result_cube_metadata=hooks.result_cube_metadata,
            settings=settings,
            summarize_exception=hooks.summarize_exception,
            extract_asset_metadata=partial(
                raster_metadata.extract_asset_metadata,
                job_id=settings.job_id,
                gdalinfo_from_file=settings.gdalinfo_from_file,
                gdalinfo_use_subprocess=settings.gdalinfo_use_subprocess,
                localize_asset=hooks.localize_asset,
            ),
            asset_metadata=asset_metadata,
            ml_model_metadata=ml_model_metadata,
            is_item=settings.stac11_mode,
            result_items=result_items,
        )

    def write(metadata: dict) -> None:
        _write_metadata_file(
            metadata, metadata_file, stac11_mode=settings.stac11_mode, hooks=hooks, auxiliary_links_cache=auxiliary_links_cache
        )

    try:
        results = wrap_evaluation_result(evaluation_result, job_specification=job_specification)
        unique_process_ids = CollectUniqueProcessIdsVisitor().accept_process_graph(process_graph).process_ids

        # perform a first metadata write _before_ actually computing the result. This provides a bit more info, even if the job fails.
        result_metadata = assemble(result=results[0], apply_gdal=False, asset_metadata={})
        tracker_metadata = hooks.usage_metadata(omit_derived_from_links=settings.omit_derived_from_links)
        write({**result_metadata, **tracker_metadata})

        global_metadata_attributes = {
            "title": job_specification.get("title", ""),
            "description": job_specification.get("description", ""),
            "institution": settings.institution,
        }

        for result in results:
            result.options["batch_mode"] = True
            hooks.prepare_result_options(result)
            result.options["file_metadata"] = {**global_metadata_attributes, **result.options.get("file_metadata", {})}
            if result.options.get("sample_by_feature"):
                geoms = tracer.get_last_geometry("filter_spatial")
                if geoms is None:
                    logger.warning(
                        "sample_by_feature enabled, but no geometries found. "
                        "They can be specified using filter_spatial."
                    )
                else:
                    result.options["geometries"] = geoms
                if result.options.get("geometries") is None:
                    logger.error(
                        "sample_by_feature was set, but no geometries provided through filter_spatial. "
                        "Make sure to provide geometries."
                    )
            if isinstance(result, MlModelResult):
                ml_model_metadata = result.get_model_metadata(str(output_file))
                logger.info("Extracted ml model metadata from %s" % output_file)

        extra_links: List[dict] = []

        def result_write_assets(result_arg: SaveResult) -> Tuple[dict, dict]:
            written_items = result_arg.write_assets(str(output_file))

            stac_root_local = getattr(result_arg, "stac_root_local", None)
            if stac_root_local is not None:
                # a StacSaveResult, detected by duck typing so this module doesn't need to import it.
                stac_href = stac_root_local
                if not str(stac_href).startswith("s3://"):
                    stac_href = hooks.output_href(str(stac_href))
                extra_links.append(
                    {
                        "href": stac_href,
                        # https://github.com/radiantearth/stac-spec/blob/master/commons/links.md#relation-types
                        "rel": "original",
                        "title": "Link to original STAC catalog.",
                        "type": "application/json",
                    }
                )

            if written_items and "assets" not in next(iter(written_items.values())):  # no "assets" property so assets themselves
                assets = written_items
                logger.warning(f"save_result: got an 'assets' object instead of items for {result_arg}")
                # TODO: this is here to avoid having to sync changes with openeo-python-driver
                # it can and should be removed as soon as we have introduced returning items in all SaveResult subclasses
                item_id = str(uuid.uuid4())
                written_items = {item_id: {"id": item_id, "assets": assets}}

            keys = set()

            def unique_key(asset_id, href):
                # try to make the key unique, and backwards compatible if possible
                if href is not None:
                    try:
                        if str(href).startswith("s3://"):
                            url = urlparse(str(href))
                            temp_key = str(Path(url.path).relative_to(output_file.parent))
                        else:
                            href_path = Path(str(href))
                            if href_path.is_absolute():
                                temp_key = str(href_path.relative_to(Path(str(output_file)).parent))
                            else:
                                temp_key = str(href_path)
                    except ValueError:
                        url = urlparse(str(href))
                        temp_key = url.path.split("/")[-1]
                else:
                    temp_key = asset_id
                counter = 0
                while temp_key in keys:
                    temp_key = f"{asset_id}_{counter}"
                    counter += 1
                keys.add(temp_key)
                return temp_key

            assets = {
                unique_key(asset_key, asset.get("href", None)): asset
                for item in written_items.values()
                for asset_key, asset in item.get("assets", {}).items()
            }
            return assets, written_items

        if settings.concurrent_save_results == 1:
            assets_metadata, results_items = unzip(*map(result_write_assets, results))
        elif settings.concurrent_save_results > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=settings.concurrent_save_results) as executor:
                futures = [executor.submit(result_write_assets, result) for result in results]
                for _ in concurrent.futures.as_completed(futures):
                    continue
            assets_metadata, results_items = unzip(*map(lambda f: f.result(), futures))
        else:
            raise ValueError(f"Invalid concurrent_save_results: {settings.concurrent_save_results}")
        assets_metadata = list(assets_metadata)
        results_items = list(results_items)

        # Flatten all STAC items across all results for use in metadata assembly.
        all_result_items = [item for result_items_ in results_items for item in result_items_.values()]

        if settings.stac11_mode:
            for stac_item_collection_path in Path(job_dir).glob(settings.item_collection_glob):
                extra_links.append(
                    AnnotatedDict(
                        {
                            "rel": "derived_from",
                            "href": href_from_job_local_path(
                                stac_item_collection_path.absolute(),
                                job_local_href_format=settings.job_local_href_format,
                                s3_bucket_name=settings.s3_bucket_name,
                            ),
                            "type": "application/geo+json",
                            ITEM_LINK_PROPERTY.EXPOSE_AUXILIARY: True,
                        }
                    ).annotate(copy_to_item=True)
                )

        # flattens items for each results into one list
        items = [item for result_items_ in results_items for item in result_items_.values()]

        flat_assets_metadata = [asset for result_assets in assets_metadata for asset in result_assets.values()]
        hooks.after_assets_written(flat_assets_metadata, job_dir)

        # this is subtle: the last of possibly several results (#295) corresponds to the terminal
        # save_result node of the process graph
        last_result = results[-1]
        if "file_metadata" in last_result.options:
            last_result.options["file_metadata"]["providers"] = [settings.provider]

        assets_for_result_metadata = _assets_for_result_metadata(settings.stac11_mode, assets_metadata, results_items)

        result_metadata = assemble(
            result=last_result, apply_gdal=False, asset_metadata=assets_for_result_metadata, result_items=all_result_items
        )
        tracker_metadata = hooks.usage_metadata(omit_derived_from_links=settings.omit_derived_from_links)
        # TODO: avoid writing non-tracker metadata in `tracker_metadata`
        tracker_metadata["links"].extend(extra_links)
        if "sar_backscatter_soft_errors" in tracker_metadata.get("usage", {}):
            soft_errors = tracker_metadata["usage"]["sar_backscatter_soft_errors"]["value"]
            if soft_errors > settings.max_soft_errors_ratio:
                raise ValueError(f"sar_backscatter: Too many soft errors ({soft_errors} > {settings.max_soft_errors_ratio})")

        meta = (
            {**result_metadata, **tracker_metadata, **{"items": items}} if settings.stac11_mode else {**result_metadata, **tracker_metadata}
        )
        write(meta)
        logger.debug("Starting GDAL-based retrieval of asset metadata")

        result_metadata = assemble(
            result=last_result,
            apply_gdal=settings.detailed_asset_metadata,
            asset_metadata=assets_for_result_metadata,
            result_items=all_result_items,
        )

        assert len(results) == len(assets_metadata)
        assert len(results) == len(results_items)
        for result, result_assets_metadata, result_items_metadata in zip(results, assets_metadata, results_items):
            export_result_to_workspaces(
                result,
                result_metadata,
                stac11_mode=settings.stac11_mode,
                workspace_repository=workspace_repository,
                job_id=settings.job_id,
                result_assets_metadata=result_assets_metadata,
                result_items_metadata=result_items_metadata,
                job_dir=job_dir,
                remove_exported_assets=settings.remove_exported_assets,
                enable_merge=settings.export_workspace_enable_merge,
                omit_derived_from_links=settings.omit_derived_from_links,
                attach_derived_from_document=settings.stac11_mode,
                usage_metadata=hooks.usage_metadata,
                copy_auxiliary_links=partial(_copy_auxiliary_links, hooks=hooks, cache=auxiliary_links_cache),
            )
    finally:
        if len(tracker_metadata) == 0:
            tracker_metadata = hooks.usage_metadata(omit_derived_from_links=settings.omit_derived_from_links)
        meta = (
            {**result_metadata, **tracker_metadata, **{"items": items}} if settings.stac11_mode else {**result_metadata, **tracker_metadata}
        )
        write(meta)


def write_failure_metadata(*, metadata_file: Path, settings: JobResultsSettings, hooks: JobResultsHooks) -> None:
    """Writes a best-effort metadata file for a job that failed before evaluation completed."""
    tracker_metadata = hooks.usage_metadata(omit_derived_from_links=settings.omit_derived_from_links)
    meta = {**tracker_metadata, "items": []} if settings.stac11_mode else tracker_metadata
    _write_metadata_file(meta, metadata_file, stac11_mode=settings.stac11_mode, hooks=hooks, auxiliary_links_cache={})


def _assets_for_result_metadata(stac11_mode: bool, assets_metadata: List[dict], results_items: List[dict]) -> dict:
    if stac11_mode:
        return {
            item_key: item for result_item_metadata in results_items for item_key, item in result_item_metadata.items()
        }
    else:
        # TODO: flattened instead of per-result, clean this up?
        return {
            asset_key: asset_metadata_
            for result_assets_metadata in assets_metadata
            for asset_key, asset_metadata_ in result_assets_metadata.items()
        }


def _write_metadata_file(
    metadata: dict,
    metadata_file: Path,
    *,
    stac11_mode: bool,
    hooks: JobResultsHooks,
    auxiliary_links_cache: Dict[Tuple[str, str, bool], str],
) -> None:
    def log_asset_hrefs(context: str):
        if stac11_mode:
            items_by_id = {item["id"]: item for item in metadata.get("items", [])}
            asset_hrefs = {
                item_key + ", " + asset_key: asset.get("href")
                for item_key, item in items_by_id.items()
                for asset_key, asset in item.get("assets").items()
            }
            logger.info(f"{context} asset hrefs: {asset_hrefs!r}")
        else:
            asset_hrefs = {asset_key: asset.get("href") for asset_key, asset in metadata.get("assets", {}).items()}
            logger.info(f"{context} asset hrefs: {asset_hrefs!r}")

    log_asset_hrefs("input")
    out_metadata = convert_asset_outputs_to_s3_urls(metadata, output_href=hooks.output_href)
    log_asset_hrefs("output")

    if stac11_mode:
        out_metadata = deepcopy(out_metadata)  # avoid mutating an object that is going to be reused

        for auxiliary_link in _copy_auxiliary_links(
            auxiliary_links=out_metadata.get("auxiliary_links", []),
            job_dir=metadata_file.parent,  # TODO: ugly way to get job_dir
            for_export_workspace=False,
            hooks=hooks,
            cache=auxiliary_links_cache,
        ):
            for item in out_metadata.get("items", []):
                item.setdefault("links", []).append(auxiliary_link)

        for link in (k for k in out_metadata.get("links", []) if AnnotatedDict.get_annotation(k, "copy_to_item")):
            for item in out_metadata.get("items", []):
                item.setdefault("links", []).append(link)

    with open(metadata_file, "w") as f:
        json.dump(out_metadata, f, default=json_default)
    logger.info("wrote metadata to %s" % metadata_file)
    hooks.publish_metadata_file(metadata_file)


def _copy_auxiliary_links(
    *,
    auxiliary_links,
    job_dir: Path,
    for_export_workspace: bool,
    hooks: JobResultsHooks,
    cache: Dict[Tuple[str, str, bool], str],
) -> List[dict]:
    """files should be downloadable from the web app driver"""
    links = auxiliary_links.target if isinstance(auxiliary_links, BadlyHashable) else auxiliary_links

    copied_auxiliary_links = []
    for auxiliary_link in links:
        auxiliary_file = Path(auxiliary_link["href"])
        cache_key = (str(auxiliary_file), str(job_dir), for_export_workspace)
        if cache_key not in cache:
            cache[cache_key] = hooks.publish_auxiliary_file(auxiliary_file, job_dir, for_export_workspace=for_export_workspace)
        copied_auxiliary_links.append(dict(auxiliary_link, href=cache[cache_key]))

    return copied_auxiliary_links
