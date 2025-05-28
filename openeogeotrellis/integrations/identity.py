from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Literal, Optional

import jwt

from openeo_driver.util.date_math import now_utc
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.utils import FileChangeWatcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IDPDetails:
    """Identity Provider Details to create identities"""
    issuer: str
    """URI used to symbolize the issuer of the issued tokens"""
    private_key: str
    """The private key in PEM format used to sign the tokens"""
    public_key: str
    """The public key in PEM format which can be used to verify the tokens"""

    @classmethod
    def from_file(cls, file_path: Path) -> IDPDetails:
        with open(file_path) as fh:
            file_dict = json.load(fh)
        private_key = file_dict["private_key"].strip()
        cls.assert_valid_pem(private_key, "private")
        public_key = file_dict["public_key"].strip()
        cls.assert_valid_pem(public_key, "public")
        return cls(
            file_dict["issuer"],
            private_key,
            public_key
        )

    @staticmethod
    def assert_valid_pem(key: str, key_type: Literal["private", "public"]):
        """Check if a string follows the expected structure of a PEM formatted key"""
        template = '-----{location} RSA {key_type} KEY-----'
        pem_header = template.format(location="BEGIN", key_type=key_type.upper())
        pem_footer = template.format(location="END", key_type=key_type.upper())
        assert key.startswith(pem_header), f"Key did not start with {pem_header}"
        assert key.endswith(pem_footer), f"Key did not end with {pem_footer}"


class IDPTokenIssuer:
    """
    An issuer of signed tokens. This is a singleton so to avoid errors having multiple instances you can call the
    class method `instance() to get the singleton object`.

    This is to get tokens from the OpenEO instance itself. So let's say the openeo instance is openeo.example.com then
    this issuer will create tokens that assert claims as per openeo.example.com. If different types of tokens are
    required then the issuer could get support for multiple types of tokens but there should only be one token issuer.
    """
    _TOKEN_EXPIRES_IN_SECONDS = 48 * 3600
    _SINGLETON: Optional[IDPTokenIssuer] = None

    def __init__(self):
        if self.__class__._SINGLETON is not None:
            raise RuntimeError("IDPTokenIssuer is meant as a singleton")
        self._IDP_DETAILS: Optional[IDPDetails] = None
        self.__class__._SINGLETON = self
        self.watcher = FileChangeWatcher()

    @classmethod
    def instance(cls) -> IDPTokenIssuer:
        if cls._SINGLETON is not None:
            return cls._SINGLETON
        else:
            return cls()

    def _hard_reset(self) -> None:
        """
        This is only for testing purpose because different tests can actually require different idp config.
        """
        self._IDP_DETAILS = None
        self._reload_idp_details_if_needed()

    def _reload_idp_details_if_needed(self) -> None:
        idp_details_file = get_backend_config().openeo_idp_details_file
        register_reload = self.watcher.get_file_reload_register_func_if_changed(idp_details_file)
        if register_reload is None:
            return
        try:
            self._IDP_DETAILS = IDPDetails.from_file(idp_details_file)
            register_reload()
        except AssertionError as ae:
            logger.fatal(f"Invalid config staged for idp: {ae}")
        except Exception as e:
            logger.warning(f"Could not reload IDP details from {idp_details_file} due to {e}")

    def get_job_token(self, sub_id: str, user_id: str, job_id: str) -> Optional[str]:
        """
        Get a JWT token that can be used by a job's execution environment to proof it is
        really the job it claims to be and that it is issued by a specific user.
        """
        self._reload_idp_details_if_needed()
        if self._IDP_DETAILS is None:
            return None
        else:
            idp_details = self._IDP_DETAILS
            now = now_utc()
            return jwt.encode(
                {
                    "sub": sub_id,
                    "exp": now + timedelta(seconds=self._TOKEN_EXPIRES_IN_SECONDS),
                    "nbf": now,
                    "iss": idp_details.issuer,
                    "iat": now,
                    "https://aws.amazon.com/tags": {
                        "principal_tags": {
                            "user_id": [user_id],
                            "job_id": [job_id],
                        },
                        "transitive_tag_keys": [
                            "user_id",
                            "job_id",
                        ]
                    }
                },
                idp_details.private_key,
                algorithm="RS256"
            )


IDP_TOKEN_ISSUER = IDPTokenIssuer.instance()


def main():
    aws_config_dir = os.environ.get("AWS_CONFIG_DIR", "/opt/spark/work-dir")
    aws_config_path = Path(aws_config_dir)
    aws_token_filename = os.environ.get("AWS_TOKEN_FILENAME", "token")
    aws_config_path.mkdir(parents=True, exist_ok=True)
    with open(aws_config_path.joinpath(aws_token_filename), 'w') as token_file:
        token_file.write(IDP_TOKEN_ISSUER.get_job_token("0", "none", "j-0"))


if __name__ == '__main__':
    main()
