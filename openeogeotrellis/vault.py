import logging
import subprocess
from subprocess import CalledProcessError, PIPE
from typing import NamedTuple, Optional

import hvac

from openeogeotrellis.configparams import ConfigParams
from openeo_driver.jobregistry import ElasticJobRegistryCredentials

_log = logging.getLogger(__name__)


class OAuthCredentials(NamedTuple):
    client_id: str
    client_secret: str


class Vault:
    def __init__(self, url: str):
        self._url = url

    def get_sentinel_hub_credentials(self, sentinel_hub_client_alias: str, vault_token: str) -> OAuthCredentials:
        client_credentials = self._get_kv_credentials(
            f"TAP/big_data_services/openeo/sentinelhub-oauth-{sentinel_hub_client_alias}", vault_token)

        _log.debug(f'{self._url}: Sentinel Hub client ID for "{sentinel_hub_client_alias}" is '
                   f'{client_credentials.client_id}')

        return client_credentials

    def get_etl_api_credentials(self, vault_token: str) -> OAuthCredentials:
        # TODO: the dev account is just a temporary situation so this dumb thing will disappear eventually
        secret_leaf = "etl-api-oauth" if "etl.terrascope.be" in ConfigParams().etl_api else "etl-api-oauth-dev"
        return self._get_kv_credentials(f"TAP/big_data_services/openeo/{secret_leaf}", vault_token)

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
            _log.error(msg=f"{e} stderr: {e.stderr.strip()}", exc_info=True)
            raise

    def _client(self, token: Optional[str] = None):
        return hvac.Client(self._url, token=token)

    def __str__(self):
        return self._url

    def get_elastic_job_registry_credentials(
        self, vault_token: Optional[str] = None
    ) -> ElasticJobRegistryCredentials:
        client = self._client(token=vault_token or self.login_kerberos())

        secret = client.secrets.kv.v2.read_secret_version(
            f"TAP/big_data_services/openeo/openeo-job-registry-elastic-api",
            mount_point="kv",
        )

        data = secret["data"]["data"]
        return ElasticJobRegistryCredentials(
            oidc_issuer=data["oidc_issuer"],
            client_id=data["client_id"],
            client_secret=data["client_secret"],
        )
