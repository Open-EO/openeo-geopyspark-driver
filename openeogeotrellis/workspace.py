import logging
import os
from pathlib import Path
from typing import Union
from urllib.parse import urlparse

from boto3.s3.transfer import TransferConfig

from openeo_driver.utils import remove_slash_prefix
from openeo_driver.workspace import Workspace

from openeogeotrellis.utils import s3_client

_log = logging.getLogger(__name__)


class ObjectStorageWorkspace(Workspace):

    MULTIPART_THRESHOLD_IN_MB = 50

    def __init__(self, bucket: str):
        self.bucket = bucket

    def import_file(self, common_path: Union[str, Path], file: Path, merge: str, remove_original: bool = False) -> str:
        merge = os.path.normpath(merge)
        subdirectory = remove_slash_prefix(merge)
        file_relative = file.relative_to(common_path)

        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD_IN_MB * MB)

        key = f"{subdirectory}/{file_relative}"
        s3_client().upload_file(str(file), self.bucket, key, Config=config)

        if remove_original:
            file.unlink()

        workspace_uri = f"s3://{self.bucket}/{key}"

        _log.debug(f"{'moved' if remove_original else 'uploaded'} {file.absolute()} to {workspace_uri}")
        return workspace_uri

    def import_object(self, common_path: str, s3_uri: str, merge: str, remove_original: bool = False) -> str:
        uri_parts = urlparse(s3_uri)

        if not uri_parts.scheme or uri_parts.scheme.lower() != "s3":
            raise ValueError(s3_uri)

        source_bucket = uri_parts.netloc
        source_key = remove_slash_prefix(uri_parts.path[1:])
        file_relative = Path(source_key).relative_to(remove_slash_prefix(common_path))

        target_key = f"{merge}/{file_relative}"

        s3 = s3_client()
        s3.copy_object(CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=self.bucket, Key=target_key)
        if remove_original:
            s3.delete_object(Bucket=source_bucket, Key=source_key)

        workspace_uri = f"s3://{self.bucket}/{target_key}"

        _log.debug(f"{'moved' if remove_original else 'copied'} s3://{source_bucket}/{source_key} to {workspace_uri}")
        return workspace_uri
