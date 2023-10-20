import logging
import subprocess
from subprocess import CalledProcessError, PIPE
from typing import NamedTuple, Optional

import hvac
import requests

from openeo_driver.util.auth import ClientCredentials
from openeogeotrellis.config import get_backend_config

_log = logging.getLogger(__name__)


class OAuthCredentials(NamedTuple):
    # TODO: migrate this to openeo_driver.util.auth.ClientCredentials?
    client_id: str
    client_secret: str


class VaultLoginError(Exception):
    pass


class Vault:
    def __init__(self, url: str, requests_session: Optional[requests.Session] = None):
        self._url = url
        self._session = requests_session or requests.Session()

    def get_sentinel_hub_credentials(self, sentinel_hub_client_alias: str, vault_token: str) -> OAuthCredentials:
        client_credentials = self._get_kv_credentials(
            f"TAP/big_data_services/openeo/sentinelhub-oauth-{sentinel_hub_client_alias}", vault_token)

        _log.debug(f'{self._url}: Sentinel Hub client ID for "{sentinel_hub_client_alias}" is '
                   f'{client_credentials.client_id}')

        return client_credentials

    def get_etl_api_credentials(self, vault_token: str) -> ClientCredentials:
        # TODO: eliminate this hardcoded vault path? Use config instead?
        creds = self._get_kv_credentials(f"TAP/big_data_services/openeo/etl-api-oauth", vault_token)
        return ClientCredentials(
            oidc_issuer=get_backend_config().etl_api_oidc_issuer,  # TODO: this ideally comes from the secret as well instead of config
            client_id=creds.client_id,
            client_secret=creds.client_secret,
        )

    def _get_kv_credentials(self, vault_secret_path, vault_token: str) -> OAuthCredentials:
        client = self._client()
        client.token = vault_token

        secret = client.secrets.kv.v2.read_secret_version(vault_secret_path, mount_point="kv")

        credentials = secret['data']['data']
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']

        return OAuthCredentials(client_id, client_secret)

    def login_jwt(self, access_token: str) -> str:
        client = self._client()
        client.auth.jwt.jwt_login(role=None, jwt=access_token)
        return client.token

    def login_kerberos(
        self,
        # TODO: eliminate hardcoded defaults?
        principal: str = "openeo@VGT.VITO.BE",
        keytab: str = "/opt/openeo.keytab",
    ) -> str:
        # hvac has no Kerberos support, use CLI instead
        username, realm = principal.split("@")

        cmd = [
            "vault",
            "login",
            f"-address={self._url}",
            "-token-only",
            "-method=kerberos",
            f"username={username}",
            "service=vault-prod",  # TODO: parameterize this too?
            f"realm={realm}",
            f"keytab_path={keytab}",
            "krb5conf_path=/etc/krb5.conf"
        ]

        try:
            vault_token = subprocess.check_output(cmd, text=True, stderr=PIPE)
            return vault_token
        except CalledProcessError as e:
            raise VaultLoginError(
                f"Vault login (Kerberos) failed: {e!s}. stderr: {e.stderr.strip()!r}"
            ) from e

    def _client(self, token: Optional[str] = None):
        return hvac.Client(self._url, token=token, session=self._session)

    def __str__(self):
        return self._url

    def get_elastic_job_registry_credentials(self, vault_token: Optional[str] = None) -> ClientCredentials:
        client = self._client(token=vault_token or self.login_kerberos())
        secret = client.secrets.kv.v2.read_secret_version(
            get_backend_config().ejr_credentials_vault_path,
            mount_point="kv",
        )
        return ClientCredentials.from_mapping(secret["data"]["data"])
