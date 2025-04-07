from __future__ import annotations
import re
from configparser import ConfigParser
from io import StringIO
import os
from dataclasses import dataclass
from typing import Optional

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.utils import utcnow_epoch
from openeogeotrellis.workspace import ObjectStorageWorkspace


@dataclass(frozen=True)
class AwsProfileDetails:
    name: str
    region: str
    role_arn: str
    web_identity_token_file: str
    role_session_name: Optional[str] = None


class AWSConfig(ConfigParser):
    S3PROXY_SERVICES = "s3proxy"
    S3PROXY_STS_ENDPOINT_URL = "S3PROXY_STS_ENDPOINT_URL"
    S3PROXY_S3_ENDPOINT_URL = "S3PROXY_S3_ENDPOINT_URL"

    def add_profile(self, profile: AwsProfileDetails):
        section_details = {
            "region": profile.region,
            "role_arn": profile.role_arn,
            "services": self.S3PROXY_SERVICES,
            "web_identity_token_file": profile.web_identity_token_file
        }
        if profile.role_session_name is not None:
            section_details["role_session_name"] = profile.role_session_name

        self[f"profile {profile.name}"] = section_details

    @staticmethod
    def create_nested_config_entry(d: dict[str, str]) -> str:
        """
        AWS config files allow nested config which is not really part of the ConfigParser. Our generated config must
        however be compatible with what they expect:
        https://github.com/boto/botocore/blob/1ad32855c799456250b44c2762cacd67f5647a6e/botocore/configloader.py#L183
        """
        key_values = [f"{k}={v}" for k, v in d.items()]
        # Entry has to start with a newline
        key_values.insert(0, '')
        return '\n'.join(key_values)

    def add_s3proxy_services(self) -> None:
        self[f"services {self.S3PROXY_SERVICES}"] = {
            "sts": self.create_nested_config_entry(
                {"endpoint_url": os.environ.get(self.S3PROXY_STS_ENDPOINT_URL, "")}
            ),
            "s3": self.create_nested_config_entry(
                {"endpoint_url": os.environ.get(self.S3PROXY_S3_ENDPOINT_URL, "")}
            )
        }


@dataclass(frozen=True)
class S3Config:
    profiles: list[AwsProfileDetails]

    def to_config(self) -> AWSConfig:
        config = AWSConfig()
        for profile in self.profiles:
            config.add_profile(profile)

        config.add_s3proxy_services()
        return config

    def __str__(self):
        with StringIO() as io:
            self.to_config().write(io)
            io.seek(0)
            return io.read()

    @classmethod
    def from_backend_config(cls, session_name: str, token_file: str) -> S3Config:
        profiles = []
        sanitized_session_name = cls._sanitize_session_name(session_name)

        # Each platform has a local region which is the platform region
        platform_region = os.environ.get("PLATFORM_S3_REGION", "waw3-1")

        def add_profile(profile_name: str, region: str, role_arn: str) -> None:
            profiles.append(
                AwsProfileDetails(
                    name=profile_name,
                    region=region if region is not None else platform_region,
                    role_arn=role_arn,
                    web_identity_token_file=token_file,
                    role_session_name=sanitized_session_name
                )
            )

        for workspace_name, workspace in get_backend_config().workspaces.items():
            if isinstance(workspace, ObjectStorageWorkspace):
                add_profile(workspace_name, workspace.region, f"arn:openeows:iam:::role/{workspace_name}")

        # The base profile which can be used for normal openeo platform S3 access
        add_profile("base", platform_region, f"arn:openeo:iam:::role/base")

        # The eodata profile can be used to access Copernicus data hosted on CloudFerro
        add_profile("eodata", "eodata", f"arn:openeo:iam:::role/eodata")

        return cls(profiles=profiles)

    @staticmethod
    def _sanitize_session_name(session_name: str) -> str:
        """
        A session name has a minimum length of 2, maximum length of 64.
        Each character must match: [\w+=,.@-] so this methods remove others.
        """
        sanitized = re.sub(r"[^\w+=,.@-]", "", session_name)
        if len(sanitized) < 2:
            sanitized += str(f"-{utcnow_epoch()}")
        return sanitized[0:64]
