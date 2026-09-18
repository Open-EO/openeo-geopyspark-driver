import json
import logging
import os
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
from urllib.parse import urlparse

from openeo_driver.constants import ITEM_LINK_PROPERTY
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import ImageCollectionResult, SaveResult
from openeo.util import dict_no_none

from openeogeotrellis._version import __version__
from openeogeotrellis.backend import JOB_METADATA_FILENAME, GeoPySparkBackendImplementation
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.integrations.gdal import localize_s3_asset
from openeogeotrellis.job_results import raster_metadata, result_metadata
from openeogeotrellis.job_results.settings import JobResultsSettings, ResultGrid
from openeogeotrellis.util.runtime import get_job_id
from openeogeotrellis.utils import get_jvm, map_optional, to_s3_url

logger = logging.getLogger(__name__)


def _result_grid(result: SaveResult) -> ResultGrid:
    def epsg_code(geotrellis_proj4_crs) -> Optional[int]:
        # We have to use the original geotrellis.proj4.CRS to avoid proj4 conversion issues.
        return geotrellis_proj4_crs.epsgCode().getOrElse(None)

    if isinstance(result, GeopysparkDataCube):
        max_level = result.pyramid.levels[result.pyramid.max_zoom]
        epsg = epsg_code(max_level.srdd.rdd().metadata().crs())
        instruments = result.metadata.get("summaries", "instruments", default=[])
    elif isinstance(result, ImageCollectionResult) and isinstance(result.cube, GeopysparkDataCube):
        max_level = result.cube.pyramid.levels[result.cube.pyramid.max_zoom]
        epsg = epsg_code(max_level.srdd.rdd().metadata().crs())
        instruments = result.cube.metadata.get("summaries", "instruments", default=[])
    else:
        epsg = None
        instruments = []

    return ResultGrid(epsg=epsg, instruments=instruments)


def _summarize_exception(e: Exception) -> str:
    return GeoPySparkBackendImplementation.summarize_exception_static(e).summary


def _build_settings() -> JobResultsSettings:
    backend_config = get_backend_config()
    return JobResultsSettings(
        job_id=get_job_id(),
        stac11_mode=False,
        omit_derived_from_links=False,
        detailed_asset_metadata=True,
        concurrent_save_results=1,
        remove_exported_assets=False,
        export_workspace_enable_merge=False,
        max_soft_errors_ratio=0.0,
        institution=f"{backend_config.processing_facility} - {backend_config.capabilities_backend_version}",
        processing_facility="VITO - SPARK",  # TODO make configurable
        processing_software="openeo-geotrellis-" + __version__,
        provider={},
        job_local_href_format=backend_config.job_local_href_format,
        s3_bucket_name=backend_config.s3_bucket_name,
        gdalinfo_from_file=backend_config.gdalinfo_from_file,
        gdalinfo_use_subprocess=backend_config.gdalinfo_use_subprocess,
    )


def _extract_asset_metadata(
    job_result_metadata: Dict,
    asset_metadata: Dict,
    job_dir: Path,
    epsg: Optional[int],
    *,
    settings: JobResultsSettings,
) -> None:
    raster_metadata.extract_asset_metadata(
        job_result_metadata,
        asset_metadata,
        job_dir,
        epsg,
        job_id=settings.job_id,
        gdalinfo_from_file=settings.gdalinfo_from_file,
        gdalinfo_use_subprocess=settings.gdalinfo_use_subprocess,
        localize_asset=localize_s3_asset,
    )


def _assemble_result_metadata(
    tracer: DryRunDataTracer,
    result: SaveResult,
    job_dir: Path,
    unique_process_ids: Set[str],
    apply_gdal,
    asset_metadata: Dict = None,  # TODO: include "items" instead of "assets"
    ml_model_metadata: Dict = None,
    is_item=False,
    result_items: Optional[List[dict]] = None,
) -> dict:
    settings = _build_settings()
    return result_metadata.assemble_result_metadata(
        tracer=tracer,
        result=result,
        job_dir=job_dir,
        unique_process_ids=unique_process_ids,
        apply_gdal=apply_gdal,
        result_grid=_result_grid,
        settings=settings,
        summarize_exception=_summarize_exception,
        extract_asset_metadata=partial(_extract_asset_metadata, settings=settings),
        asset_metadata=asset_metadata,
        ml_model_metadata=ml_model_metadata,
        is_item=is_item,
        result_items=result_items,
    )


def _convert_asset_outputs_to_s3_urls(job_metadata: dict) -> dict:
    """Convert each asset's href value to a URL on S3 in the metadata dictionary."""
    return result_metadata.convert_asset_outputs_to_s3_urls(job_metadata, output_href=to_s3_url)


def _transform_stac_metadata(job_dir: Path):
    def relativize(assets: dict) -> dict:
        def relativize_href(asset: dict) -> dict:
            absolute_href = asset["href"]
            relative_path = urlparse(absolute_href).path.split("/")[-1]
            return dict(asset, href=relative_path)

        return {asset_name: relativize_href(asset) for asset_name, asset in assets.items()}

    def drop_links(metadata: dict) -> dict:
        result = metadata.copy()
        result.pop("links", None)
        return result

    stac_metadata_files = [
        job_dir / file_name
        for file_name in os.listdir(job_dir)
        if file_name.endswith("_metadata.json") and file_name != JOB_METADATA_FILENAME
    ]

    for stac_metadata_file in stac_metadata_files:
        with open(stac_metadata_file, "rt", encoding="utf-8") as f:
            stac_metadata = json.load(f)

        relative_assets = relativize(stac_metadata.get("assets", {}))
        transformed = dict(drop_links(stac_metadata), assets=relative_assets)

        with open(stac_metadata_file, "wt", encoding="utf-8") as f:
            json.dump(transformed, f, indent=2)


def _get_tracker(tracker_id: str = ""):
    return get_jvm().org.openeo.geotrelliscommon.BatchJobMetadataTracker.tracker(tracker_id)


def _get_tracker_metadata(tracker_id: str = "", *, omit_derived_from_links: bool = False) -> dict:
    tracker = _get_tracker(tracker_id)
    usage = {}
    all_links = []
    auxiliary_links = []

    if tracker is not None:
        tracker_results = tracker.asDict()

        pu = tracker_results.get("Sentinelhub_Processing_Units", None)
        if pu is not None:
            usage["sentinelhub"] = {"value": pu, "unit": "sentinelhub_processing_unit"}

        pixels = tracker_results.get("InputPixels", None)
        if pixels is not None:
            usage["input_pixel"] = {"value": pixels / (1024 * 1024), "unit": "mega-pixel"}

        input_product_links: Dict[str, list] = tracker_results.get("links", None)
        if not omit_derived_from_links and input_product_links:
            # TODO: when in the future these links point to STAC objects we will need to update the type.
            #   https://github.com/openEOPlatform/architecture-docs/issues/327
            all_links.extend(
                {
                    "href": link.getSelfUrl(),
                    "rel": "derived_from",
                    "title": f"Derived from {link.getId()}",
                    "type": "application/json",
                }
                for links in input_product_links.values()
                for link in links
            )

        def _as_auxiliary_links(auxiliary_files) -> List[dict]:
            return [
                {
                    "href": str(auxiliary_file.getPath()),
                    "type": auxiliary_file.getMediaType(),
                    # TODO: "title", get from Java object and add
                    "rel": "aux",  # TODO: get from Java object
                    ITEM_LINK_PROPERTY.EXPOSE_AUXILIARY: True,
                }
                for auxiliary_file in auxiliary_files
            ]

        auxiliary_links = map_optional(_as_auxiliary_links, tracker_results.get("auxiliary_files"))

    from openeogeotrellis.metrics_tracking import global_tracker

    python_metrics = global_tracker().as_dict()
    sar_backscatter_errors = python_metrics.pop("orfeo_backscatter_soft_errors", 0)
    sar_backscatter_total = python_metrics.pop("orfeo_backscatter_execution_counter", 0)
    usage = {**usage, **{name: {"value": value, "unit": "count"} for name, value in python_metrics.items()}}
    if sar_backscatter_total > 0:
        usage["sar_backscatter_soft_errors"] = {
            "value": sar_backscatter_errors / sar_backscatter_total,
            "unit": "fraction",
        }
    sar_backscatter_inputpixels = python_metrics.pop("orfeo_backscatter_input_pixels", 0)
    if sar_backscatter_inputpixels > 0:
        if "input_pixel" in usage:
            usage["input_pixel"]["value"] += sar_backscatter_inputpixels / (1024 * 1024)
        else:
            usage["input_pixel"] = {"value": sar_backscatter_inputpixels / (1024 * 1024), "unit": "mega-pixel"}
    return dict_no_none(usage=usage or None, links=all_links, auxiliary_links=auxiliary_links or None)


def href_from_job_local_path(path: Union[os.PathLike, str]) -> str:
    """
    Convert a file path from a local job context
    (e.g. a path on a job-specific mount)
    to a href that also makes sense outside the job context,
    e.g. in the web app context (without the same mounts)
    """
    backend_config = get_backend_config()
    return result_metadata.href_from_job_local_path(
        path,
        job_local_href_format=backend_config.job_local_href_format,
        s3_bucket_name=backend_config.s3_bucket_name,
    )
