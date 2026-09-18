"""
Plain-data configuration and engine hooks for the ``job_results`` package.

Values that the rest of the package would otherwise read from
``get_backend_config()``/``ConfigParams()`` arrive here instead, so the
package itself never has to import those. Behavior that depends on the JVM
or on deployment-specific branching (kube vs. YARN, s3proxy, CARD4L, ...)
arrives through the ``JobResultsHooks`` protocol instead.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol

from openeo_driver.save_result import SaveResult


@dataclass(frozen=True)
class JobResultsSettings:
    job_id: str
    stac11_mode: bool
    omit_derived_from_links: bool
    detailed_asset_metadata: bool  # job option, default True
    concurrent_save_results: int  # job option, default 1
    remove_exported_assets: bool
    export_workspace_enable_merge: bool
    max_soft_errors_ratio: float
    institution: str  # "<processing_facility> - <backend_version>"
    processing_facility: str  # today "VITO - SPARK"
    processing_software: str  # today "openeo-geotrellis-<version>"
    provider: dict  # today's providers[] entry
    job_local_href_format: str
    s3_bucket_name: Optional[str]
    gdalinfo_from_file: bool
    gdalinfo_use_subprocess: bool
    item_collection_glob: str  # filename pattern for derived_from item collections (load_stac coupling)


@dataclass(frozen=True)
class ResultGrid:
    epsg: Optional[int]
    instruments: List[str]


class JobResultsHooks(Protocol):
    """
    The engine-specific half of finalizing a batch job result: the JVM,
    deployment-specific branching (kube vs. YARN, s3proxy, FUSE, CARD4L),
    and GeoPySpark-specific error formatting. ``GeoPySparkJobResultsHooks``
    (in ``openeogeotrellis.deploy.batch_job``) is the concrete
    implementation; unit tests use a recording fake instead.
    """

    def result_grid(self, result: SaveResult) -> ResultGrid: ...

    def usage_metadata(self, *, omit_derived_from_links: bool = False) -> dict: ...  # {usage, links, auxiliary_links}

    def prepare_result_options(self, result: SaveResult) -> None: ...  # s3proxy / s3_client / s3_bucket

    def after_assets_written(self, assets_metadata: List[dict], job_dir: Path) -> None: ...  # FUSE wait, permissions, CARD4L

    def localize_asset(self, href: str, job_dir: Path) -> Optional[Path]: ...  # S3 asset -> local file for gdalinfo

    def output_href(self, href: str) -> str: ...  # kube: to_s3_url, else identity

    def publish_metadata_file(self, metadata_file: Path) -> None: ...  # permissions + upload

    def publish_auxiliary_file(self, path: Path, job_dir: Path, *, for_export_workspace: bool) -> str: ...

    def summarize_exception(self, e: Exception) -> str: ...  # log text only
