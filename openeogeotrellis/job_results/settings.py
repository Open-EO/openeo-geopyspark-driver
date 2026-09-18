"""
Plain-data configuration for the ``job_results`` package.

Values that the rest of the package would otherwise read from
``get_backend_config()``/``ConfigParams()`` arrive here instead, so the
package itself never has to import those.
"""
from dataclasses import dataclass
from typing import List, Optional


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


@dataclass(frozen=True)
class ResultGrid:
    epsg: Optional[int]
    instruments: List[str]
