import logging
import os
import subprocess
from typing import NamedTuple

import hvac


_log = logging.getLogger(__name__)


class SentinelHubCredentials(NamedTuple):
    client_id: str
    client_secret: str


class Vault:
    def __init__(self, url: str):
        self._url = url

    def get_sentinel_hub_credentials(self, sentinel_hub_client_alias: str, vault_token: str) -> SentinelHubCredentials:
        client = self._client()
        client.token = vault_token

        secret = client.secrets.kv.v2.read_secret_version(
            f"TAP/big_data_services/openeo/sentinelhub-oauth-{sentinel_hub_client_alias}",
            mount_point="kv")

        credentials = secret['data']['data']
        client_id = credentials['client_id']
        client_secret = credentials['client_secret']

        _log.debug(f'{self._url}: Sentinel Hub client ID for "{sentinel_hub_client_alias}" is {client_id}')

        return SentinelHubCredentials(client_id, client_secret)

    def login_jwt(self, access_token: str) -> str:
        client = self._client()
        client.auth.jwt.jwt_login(role=None, jwt=access_token)
        return client.token

    def login_kerberos(self) -> str:
        # hvac has no Kerberos support, use CLI instead
        vault_kerberos_login = [
            "vault",
            "login",
            "-token-only",
            "-method=kerberos",
            "username=openeo",
            "service=vault-prod",
            "realm=VGT.VITO.BE",
            "keytab_path=/opt/openeo.keytab",
            "krb5conf_path=/etc/krb5.conf"
        ]

        env = {**os.environ, **{"VAULT_ADDR": self._url}}
        vault_token = subprocess.check_output(vault_kerberos_login, env=env, text=True)

        return vault_token

    def _client(self):
        return hvac.Client(self._url)

    def __str__(self):
        return self._url
