"""
Engine glue for the self-contained gdalinfo-based raster metadata in
``openeogeotrellis.job_results.raster_metadata``: localizes S3 assets to the
local filesystem so gdalinfo can read them.
"""
from pathlib import Path
from typing import Optional

from openeogeotrellis.job_results.raster_metadata import resolve_asset_path
from openeogeotrellis.utils import stream_s3_binary_file_contents


def localize_s3_asset(asset_href: str, job_dir: Path) -> Optional[Path]:
    """Download an S3 asset to the local filesystem and return its local path."""
    abs_asset_path = resolve_asset_path(asset_href, job_dir)
    abs_asset_path.parent.mkdir(parents=True, exist_ok=True)
    with open(abs_asset_path, "wb") as f:
        for chunk in stream_s3_binary_file_contents(asset_href):
            f.write(chunk)
    return abs_asset_path
