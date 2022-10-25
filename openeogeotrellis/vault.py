from typing import NamedTuple

import hvac


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
        return SentinelHubCredentials(client_id=credentials['client_id'], client_secret=credentials['client_secret'])

    def login_jwt(self, access_token: str) -> str:
        client = self._client()
        client.auth.jwt.jwt_login(role=None, jwt=access_token)
        return client.token

    def _client(self):
        return hvac.Client(self._url)
