from __future__ import annotations
from typing import Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from mypy_boto3_sts.client import STSClient
    from mypy_boto3_sts.type_defs import CredentialsTypeDef

import os
import logging
import boto3
from botocore.config import Config
from openeogeotrellis.integrations.identity import IDP_TOKEN_ISSUER
from openeogeotrellis.integrations.s3proxy.exceptions import (
    S3ProxyDisabled,
    DriverCannotIssueTokens,
    CredentialsException,
)
from openeogeotrellis.config.s3_config import AWSConfig
from dataclasses import dataclass
from datetime import datetime


_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class STSCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime

    @classmethod
    def from_sts_response_creds(cls, creds: CredentialsTypeDef) -> STSCredentials:
        return cls(
            access_key_id=creds["AccessKeyId"],
            secret_access_key=creds["SecretAccessKey"],
            session_token=creds["SessionToken"],
            expiration=creds["Expiration"]
        )

    def as_client_kwargs(self):
        return {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "aws_session_token": self.session_token
        }


class _STSClient:
    """Because moto does not support custom endpoints"""
    @classmethod
    def get(cls, *args, **kwargs) -> STSClient:
        if "config" not in kwargs:
            strict_timeouts = Config(connect_timeout=1, read_timeout=1)
            kwargs["config"] = strict_timeouts
        return boto3.client("sts", *args, **kwargs)

def _get_environment_sts_endpoint() -> str:
    endpoint = os.environ.get(AWSConfig.S3PROXY_STS_ENDPOINT_URL, "disabled").lower()
    if endpoint == "disabled":
        raise S3ProxyDisabled("No STS endpoint")
    return endpoint

def _get_proxy_sts_client() -> STSClient:
    return _STSClient.get(endpoint_url=_get_environment_sts_endpoint())


def _get_aws_credentials_for_proxy(token: str, role_arn: str, session_name: Optional[str] = None) -> STSCredentials:
    session_name = session_name or "openeo-geopyspark-driver"
    sts = _get_proxy_sts_client()
    return STSCredentials.from_sts_response_creds(
        sts.assume_role_with_web_identity(
            RoleArn=role_arn,
            RoleSessionName=session_name,
            WebIdentityToken=token
        )["Credentials"]
    )

def get_job_aws_credentials_for_proxy(
        job_id: str, user_id: str, role_arn: str, session_name: Optional[str] = None
) -> STSCredentials:
    token = IDP_TOKEN_ISSUER.get_job_token(sub_id="openeo-driver", user_id=user_id, job_id=job_id)
    if token is None:
        raise DriverCannotIssueTokens()
    try:
        return _get_aws_credentials_for_proxy(token=token, role_arn=role_arn, session_name=session_name)
    except Exception as e:
        _log.debug("Could not get credentials from proxy", exc_info=e)
        raise CredentialsException() from e
