"""
Minimal object storage abstraction for exchanging batch job files (currently: the job specification)
between the web app driver and the batch job driver on Kubernetes deployments.

The bucket name (config `s3_bucket_name`, `SWIFT_BUCKET` in the batch job driver) is interpreted as:

- default: S3 bucket, through the existing `S3ClientBuilder`
  (preserving its bucket-specific endpoint/credential handling, including eodata and S3 proxy).
- when config `azure_storage_account_name` is set: Azure Blob Storage container in that storage account,
  authenticated with short-lived Microsoft Entra tokens (no account keys or client secrets).
"""

from __future__ import annotations

import abc
import io
import logging
import os
from pathlib import Path
from typing import Optional, Union

from openeogeotrellis.config import get_backend_config

_log = logging.getLogger(__name__)


class JobStorage(abc.ABC):
    @abc.abstractmethod
    def upload_bytes(self, data: bytes, key: str) -> None:
        pass

    @abc.abstractmethod
    def download_file(self, key: str, local_path: Union[str, os.PathLike]) -> None:
        pass


class S3JobStorage(JobStorage):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def upload_bytes(self, data: bytes, key: str) -> None:
        from openeogeotrellis.integrations.s3_client import S3ClientBuilder

        S3ClientBuilder.from_bucket(self.bucket).upload_fileobj(io.BytesIO(data), self.bucket, key)

    def download_file(self, key: str, local_path: Union[str, os.PathLike]) -> None:
        from openeogeotrellis.integrations.s3_client import S3ClientBuilder

        S3ClientBuilder.from_bucket(self.bucket).download_file(self.bucket, key, str(local_path))


class AzureBlobJobStorage(JobStorage):
    def __init__(self, account_name: str, container: str):
        self.account_url = f"https://{account_name}.blob.core.windows.net"
        self.container = container

    def _container_client(self):
        from azure.storage.blob import ContainerClient

        return ContainerClient(
            account_url=self.account_url, container_name=self.container, credential=azure_token_credential()
        )

    def upload_bytes(self, data: bytes, key: str) -> None:
        with self._container_client() as container_client:
            container_client.upload_blob(name=key, data=data, overwrite=True)

    def download_file(self, key: str, local_path: Union[str, os.PathLike]) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        with self._container_client() as container_client, open(local_path, "wb") as f:
            container_client.download_blob(key).readinto(f)


def azure_token_credential():
    """
    Azure credential without long-lived secrets: workload identity federation (AKS workload identity:
    auto-rotated projected service account token), managed identity, or developer tooling (local use).
    Access tokens are short-lived and refreshed by `azure-identity`.
    `EnvironmentCredential` is excluded so a client secret in the environment is never picked up.
    """
    from azure.identity import DefaultAzureCredential

    return DefaultAzureCredential(
        exclude_environment_credential=True,
        exclude_shared_token_cache_credential=True,
    )


def get_job_storage(bucket: Optional[str] = None) -> JobStorage:
    """
    :param bucket: S3 bucket or Azure container name (default: `s3_bucket_name` config).
    """
    bucket = bucket or get_backend_config().s3_bucket_name
    azure_account_name = get_backend_config().azure_storage_account_name
    if azure_account_name:
        return AzureBlobJobStorage(account_name=azure_account_name, container=bucket)
    return S3JobStorage(bucket=bucket)
