import json
import tempfile
import time
from pathlib import Path
from typing import Generator

import jwt
import pytest

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from openeogeotrellis.identity import IDPTokenIssuer, IDP_TOKEN_ISSUER


def test_idptokenissuer_should_not_be_instantiated_manually():
    # WHEN someone wants to manually create an instance of the IDPTokenIssuer
    # THEN they should get an exception
    with pytest.raises(RuntimeError):
        IDPTokenIssuer()


def test_without_valid_config_token_should_just_be_none(monkeypatch):
    # GIVEN no valid config
    monkeypatch.setattr(IDP_TOKEN_ISSUER, '_IDP_DETAILS_FILE', Path("/tmp/file_that_do3s_not_exist.arr"))
    # WHEN we get a token for a job of a specific user
    token = IDP_TOKEN_ISSUER.get_identity_token("userA", "j-20250120231301301301013")
    # THEN the token is None
    assert token is None


def create_idp_config(file_path: Path) -> str:
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )

    pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    pem_public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.PKCS1
    ).decode("utf-8")
    with open(file_path, 'w') as fh:
        json.dump(
            {
              "issuer": "https://pytests.localhost",
              "private_key": pem_private_key,
              "public_key": pem_public_key,
            },
            fh
        )
    return pem_public_key


@pytest.fixture
def idp_token_issuer(monkeypatch, tmp_path) -> Generator[str, None, None]:
    idp_cfg_file = tmp_path / "idp.json"
    monkeypatch.setattr(IDP_TOKEN_ISSUER, '_IDP_DETAILS_FILE', idp_cfg_file)
    public_key = create_idp_config(idp_cfg_file)
    yield public_key


def test_a_valid_token_should_be_produced_if_there_is_proper_config(idp_token_issuer):
    # GIVEN valid config (idp_token_issuer fixture)
    # GIVEN claims we want to certify
    claims = {
        "user_id": "userA",
        "job_id": "j-20250120231301301301013",
    }
    # WHEN we get a token for a job of a specific user
    token = IDP_TOKEN_ISSUER.get_identity_token(**claims)
    # THEN the token is not None
    assert token is not None
    # THEN the token should be valid
    decoded_token = jwt.decode(token, algorithms=["RS256"], key=idp_token_issuer)
    # THEN it should contain the appropriate claims in nested claim format
    # https://github.com/VITObelgium/fakes3pp/issues/4
    assert "https://aws.amazon.com/tags" in decoded_token
    tags = decoded_token["https://aws.amazon.com/tags"]
    assert "principal_tags" in tags
    principal_tags = tags["principal_tags"]
    for claim_key, claim_value in claims.items():
        assert claim_key in principal_tags
        assert claim_value in principal_tags[claim_key]
    assert "transitive_tag_keys" in tags
    for key in claims.keys():
        assert key in tags["transitive_tag_keys"]


def test_the_validity_of_a_token_should_be_restricted_in_time(idp_token_issuer, fast_sleep):
    # GIVEN valid config (idp_token_issuer fixture)
    # GIVEN claims we want to certify
    claims = {
        "user_id": "userA",
        "job_id": "j-20250120231301301301013",
    }
    # WHEN we get a token for a job of a specific user
    token = IDP_TOKEN_ISSUER.get_identity_token(**claims)
    # THEN the token is not None
    assert token is not None
    # THEN the token should be initially valid
    jwt.decode(token, algorithms=["RS256"], key=idp_token_issuer)

    # WHEN time passes
    time.sleep(IDP_TOKEN_ISSUER._TOKEN_EXPIRES_IN_SECONDS + 1)

    # THEN we should get a token expired exception upon decoding
    with pytest.raises(jwt.ExpiredSignatureError):
        jwt.decode(token, algorithms=["RS256"], key=idp_token_issuer)
