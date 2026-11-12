"""
Minimal object storage abstraction for exchanging batch job files (currently: the job specification)
between the web app driver and the batch job driver on Kubernetes deployments.

The storage backend is selected explicitly with config `job_storage_type`,
with type-specific settings in the corresponding `job_*` config fields:

- "aws_s3": S3 bucket `job_aws_s3_bucket_name`, through the existing `S3ClientBuilder`
  (preserving its bucket-specific endpoint/credential handling, including eodata and S3 proxy).
- "azure_blob": Azure Blob Storage container `job_azure_storage_container_name`
  in storage account `job_azure_storage_account_name`,
  authenticated with short-lived Microsoft Entra tokens (no account keys or client secrets).
- "unspecified" (default, deprecated): legacy fallback to S3 bucket `s3_bucket_name` when using object storage
  (Kubernetes deployments), or the local (shared) file system otherwise (e.g. YARN deployments).
"""

from __future__ import annotations

import abc
import io
import logging
import os
import shutil
from pathlib import Path
from typing import Optional, Tuple, Union
from urllib.parse import unquote, urlparse

from openeogeotrellis.config import get_backend_config

_log = logging.getLogger(__name__)


class JobStorage(abc.ABC):
    @abc.abstractmethod
    def upload_bytes(self, data: bytes, key: str) -> None:
        pass

    @abc.abstractmethod
    def download_file(self, key: str, local_path: Union[str, os.PathLike]) -> None:
        pass

    @abc.abstractmethod
    def download_bytes(self, key: str) -> bytes:
        pass

    @abc.abstractmethod
    def uri(self, key: str) -> str:
        """URI of the object with given key, that can be parsed back with `parse_job_storage_uri`."""
        pass


class S3JobStorage(JobStorage):
    def __init__(self, bucket: str):
        self.bucket = bucket

    def __repr__(self) -> str:
        return f"{type(self).__name__}(bucket={self.bucket!r})"

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key.lstrip('/')}"

    def upload_bytes(self, data: bytes, key: str) -> None:
        from openeogeotrellis.integrations.s3_client import S3ClientBuilder

        S3ClientBuilder.from_bucket(self.bucket).upload_fileobj(io.BytesIO(data), self.bucket, key)

    def download_file(self, key: str, local_path: Union[str, os.PathLike]) -> None:
        from openeogeotrellis.integrations.s3_client import S3ClientBuilder

        S3ClientBuilder.from_bucket(self.bucket).download_file(self.bucket, key, str(local_path))

    def download_bytes(self, key: str) -> bytes:
        from openeogeotrellis.integrations.s3_client import S3ClientBuilder

        response = S3ClientBuilder.from_bucket(self.bucket).get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()


class LocalFileJobStorage(JobStorage):
    """
    Job storage on a (shared) POSIX file system, e.g. for YARN deployments.
    Object keys are paths relative to `root` (default: file system root, so keys are absolute paths without leading "/").

    Reading retries on `FileNotFoundError` (e.g. to cope with file system caching/propagation delays),
    according to config `read_results_metadata_file_retry_settings` (by default: no retries).
    """

    def __init__(self, root: Union[str, os.PathLike] = "/", retry_settings: Optional[dict] = None):
        self.root = Path(root)
        self._retry_settings = retry_settings

    def __repr__(self) -> str:
        return f"{type(self).__name__}(root={str(self.root)!r})"

    def _path(self, key: str) -> Path:
        return self.root / key.lstrip("/")

    def upload_bytes(self, data: bytes, key: str) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def _with_retry(self, func):
        import reretry

        retry_settings = self._retry_settings
        if retry_settings is None:
            retry_settings = get_backend_config().read_results_metadata_file_retry_settings
        return reretry.retry(exceptions=FileNotFoundError, logger=_log, **retry_settings)(func)

    def download_file(self, key: str, local_path: Union[str, os.PathLike]) -> None:
        source = self._path(key)
        if source.resolve() == Path(local_path).resolve():
            return
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self._with_retry(shutil.copyfile)(source, local_path)

    def download_bytes(self, key: str) -> bytes:
        return self._with_retry(self._path(key).read_bytes)()

    def uri(self, key: str) -> str:
        return self._path(key).absolute().as_uri()


class AzureBlobJobStorage(JobStorage):
    HOST_SUFFIX = ".blob.core.windows.net"

    def __init__(self, account_name: str, container: str):
        self.account_url = f"https://{account_name}{self.HOST_SUFFIX}"
        self.container = container

    def __repr__(self) -> str:
        return f"{type(self).__name__}(account_url={self.account_url!r}, container={self.container!r})"

    def uri(self, key: str) -> str:
        return f"{self.account_url}/{self.container}/{key.lstrip('/')}"

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

    def download_bytes(self, key: str) -> bytes:
        with self._container_client() as container_client:
            return container_client.download_blob(key).readall()


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


def get_job_storage(job_dict: Optional[dict] = None) -> JobStorage:
    """
    Get the object storage to exchange batch job files, based on config `job_storage_type`.

    :param job_dict: job info (e.g. from the job registry), if available.
        When it contains an object storage `results_metadata_uri`, the storage is derived from that URI
        (so the storage a job was started with keeps being used, even after a config change).
    """
    if job_dict and job_dict.get("results_metadata_uri"):
        uri = job_dict["results_metadata_uri"]
        try:
            storage, _ = parse_job_storage_uri(uri)
            return storage
        except ValueError:
            _log.warning(f"Failed to derive job storage from {uri=}, falling back to config", exc_info=True)

    config = get_backend_config()
    storage_type = config.job_storage_type

    if storage_type == "aws_s3":
        return S3JobStorage(bucket=_require(config.job_aws_s3_bucket_name, "job_aws_s3_bucket_name", storage_type))
    elif storage_type == "azure_blob":
        return AzureBlobJobStorage(
            account_name=_require(
                config.job_azure_storage_account_name, "job_azure_storage_account_name", storage_type
            ),
            container=_require(
                config.job_azure_storage_container_name, "job_azure_storage_container_name", storage_type
            ),
        )
    elif storage_type == "unspecified":
        # TODO: legacy fallback, to be disallowed once all deployments set an explicit `job_storage_type`
        from openeogeotrellis.configparams import ConfigParams

        if ConfigParams().use_object_storage:
            return S3JobStorage(bucket=config.s3_bucket_name)
        return LocalFileJobStorage()
    else:
        raise ValueError(f"Unsupported job_storage_type {storage_type!r}")



def get_results_metadata_key(job_dict: Optional[dict], default: str) -> str:
    """
    Object key (in the storage given by `get_job_storage(job_dict)`) of the job's results metadata file:
    from `results_metadata_uri` in the job info if available, `default` otherwise.
    """
    uri = (job_dict or {}).get("results_metadata_uri")
    if uri:
        try:
            _, key = parse_job_storage_uri(uri)
            return key
        except ValueError:
            pass  # Already logged in `get_job_storage`
    return default.strip("/")


def _require(value: Optional[str], name: str, storage_type: str) -> str:
    if not value:
        raise ValueError(f"Config {name!r} is required for job_storage_type {storage_type!r}")
    return value


def parse_job_storage_uri(uri: str) -> Tuple[JobStorage, str]:
    """
    Parse an object storage URI (as produced by `JobStorage.uri`) into a `JobStorage` and object key.

    Supported forms:
    - `file:///<absolute path>` (key: path without leading "/", relative to file system root)
    - `s3://<bucket>/<key>`
    - `https://<account>.blob.core.windows.net/<container>/<key>`
    """
    parsed = urlparse(uri, allow_fragments=False)
    if parsed.scheme == "file" and parsed.netloc in ("", "localhost") and parsed.path.startswith("/"):
        return LocalFileJobStorage(root="/"), unquote(parsed.path).lstrip("/")
    elif parsed.scheme == "s3" and parsed.netloc:
        from openeogeotrellis.integrations.s3proxy.s3_uris import get_bucket_key_from_uri

        bucket, key = get_bucket_key_from_uri(uri)
        return S3JobStorage(bucket=bucket), key
    elif parsed.scheme == "https" and (parsed.hostname or "").endswith(AzureBlobJobStorage.HOST_SUFFIX):
        account_name = parsed.hostname[: -len(AzureBlobJobStorage.HOST_SUFFIX)]
        container, _, key = parsed.path.lstrip("/").partition("/")
        if account_name and container and key:
            return AzureBlobJobStorage(account_name=account_name, container=container), key
    raise ValueError(f"Unsupported job storage URI {uri!r}")
