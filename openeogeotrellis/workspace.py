import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from openeo_driver.workspace import Workspace

from openeogeotrellis.utils import s3_client

_log = logging.getLogger(__name__)


class ObjectStorageWorkspace(Workspace):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def import_file(self, file: Path, merge: str):
        merge = os.path.normpath(merge)
        subdirectory = merge[1:] if merge.startswith("/") else merge

        key = subdirectory + "/" + file.name
        s3_client().upload_file(str(file), self.bucket, key)

        _log.debug(f"uploaded {file.absolute()} to s3://{self.bucket}/{key}")

    def import_object(self, s3_uri: str, merge: str):
        uri_parts = urlparse(s3_uri)

        if not uri_parts.scheme or uri_parts.scheme.lower() != "s3":
            raise ValueError(s3_uri)

        source_bucket = uri_parts.netloc
        source_key = uri_parts.path[1:]
        filename = source_key.split("/")[-1]

        target_key = f"{merge}/{filename}"

        s3_client().copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=self.bucket, Key=target_key
        )

        _log.debug(f"copied s3://{source_bucket}/{source_key} to s3://{self.bucket}/{target_key}")
