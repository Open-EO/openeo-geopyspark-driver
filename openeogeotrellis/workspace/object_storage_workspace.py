from openeogeotrellis.job_results.workspaces.object_storage_workspace import (
    ObjectStorageWorkspace as _ObjectStorageWorkspace,
)
from openeogeotrellis.utils import S3ClientBuilder


class ObjectStorageWorkspace(_ObjectStorageWorkspace):
    def __init__(self, bucket: str, region: str):
        super().__init__(bucket, region, s3_client_factory=S3ClientBuilder.from_bucket)
