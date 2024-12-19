import re

import pytest

from openeogeotrellis.config.s3_config import S3Config, AwsProfileDetails, AWSConfig
from configparser import ConfigParser


def test_rendered_config_is_parseable(monkeypatch):
    # Given S3Proxy endpoints
    sts_endpoint = "my-sts.example.com"
    s3_endpoint = "s3.example.com"
    monkeypatch.setenv(AWSConfig.S3PROXY_S3_ENDPOINT_URL, s3_endpoint)
    monkeypatch.setenv(AWSConfig.S3PROXY_STS_ENDPOINT_URL, sts_endpoint)

    # Given profile details
    test_profile_name = "test"
    test_region = "waw3-1"
    test_role_arn = "arn:openeo:iam:::role/base"
    test_session_name = "test-session"
    test_token_file = "/etc/my/tokenfile"

    # When creating a config as string
    aws_s3_config_str = str(
        S3Config(
            profiles=[
                AwsProfileDetails(
                    name=test_profile_name,
                    region=test_region,
                    role_arn=test_role_arn,
                    web_identity_token_file=test_token_file,
                    role_session_name=test_session_name
                )
            ]
        )
    )

    test_profile_section_name = f"profile {test_profile_name}"
    # Then config parses and we can retrieve the fields
    cp = ConfigParser()
    cp.read_string(aws_s3_config_str)
    assert cp.get(test_profile_section_name, "region") == test_region
    assert cp.get(test_profile_section_name, "role_arn") == test_role_arn
    assert cp.get(test_profile_section_name, "services") == AWSConfig.S3PROXY_SERVICES
    assert cp.get(test_profile_section_name, "role_session_name") == test_session_name
    assert cp.get(test_profile_section_name, "web_identity_token_file") == test_token_file

    # Then services section must be there
    services_section_name = f"services {AWSConfig.S3PROXY_SERVICES}"
    assert "\nendpoint_url" in cp.get(services_section_name, "sts")
    assert sts_endpoint in cp.get(services_section_name, "sts")
    assert "\nendpoint_url" in cp.get(services_section_name, "s3")
    assert s3_endpoint in cp.get(services_section_name, "s3")


testdata = [
    # inputString, expectedChange
    ("1", True),  # Too short
    ("1" * 100, True),  # Too long
    ("valid-session123", False),
    ("invalid-ch$racter", True),
]


@pytest.mark.parametrize(["in_str", "requires_sanitization"], testdata)
def test_sanitization_should_always_arrive_at_a_valid_session_name(in_str: str, requires_sanitization: bool):
    sanitized = S3Config._sanitize_session_name(in_str)
    is_sanitized = sanitized != in_str
    assert is_sanitized == requires_sanitization
    assert 2 <= len(sanitized) <= 64
    assert re.match(r"^[\w+=,.@-]*$", sanitized) is not None, f"post sanitization still mismatch pattern {sanitized}"

