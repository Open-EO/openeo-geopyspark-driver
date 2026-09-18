from typing import Optional

from openeogeotrellis.job_results.workspaces.custom_stac_io import CustomStacIO as _CustomStacIO
from openeogeotrellis.utils import S3ClientBuilder


class CustomStacIO(_CustomStacIO):
    """Adds support for object storage."""

    def __init__(self, region: Optional[str] = None):
        super().__init__(s3_client_factory=S3ClientBuilder.from_bucket, region=region)
