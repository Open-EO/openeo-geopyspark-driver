from __future__ import annotations
import logging
from typing import NamedTuple, Optional, Union, Tuple

import hvac


_log = logging.getLogger(__name__)


class OAuthCredentials(NamedTuple):
    # TODO: migrate this to openeo_driver.util.auth.ClientCredentials?
    client_id: str
    client_secret: str


class VaultClient:
    """
    Like legacy `openeogeotrellis.vault.Vault`, but with support for "cert" auth
    which requires the `hvac.Client` instance to be kept around after login
    (e.g. for proper reuse of `verify` param inside the request session).
    """

    # TODO: eliminate need for `get_sentinel_hub_credentials`
    #       (which is the only remaining use case)
    #       and get rid of all vault integration entirely?
    #       https://github.com/Open-EO/openeo-geopyspark-driver/issues/1341

    def __init__(self, url: str):
        self._url = url
        self._client = None

    def __repr__(self):
        return f"{type(self).__name__}({self._url!r})"

    def login_jwt(self, access_token: str) -> VaultClient:
        self._client = hvac.Client(url=self._url)
        self._client.auth.jwt.jwt_login(role=None, jwt=access_token)
        return self

    def login_cert(self, *, name: str, cert: Optional[Tuple[str, str]], verify: Union[str, bool] = None) -> VaultClient:
        self._client = hvac.Client(url=self._url, cert=cert, verify=verify)
        self._client.auth.cert.login(name=name)
        return self

    def _kv_read_secret(self, path: str) -> dict:
        response = self._client.secrets.kv.v2.read_secret_version(path, mount_point="kv")
        return response["data"]["data"]

    def get_sentinel_hub_credentials(self, alias: str) -> OAuthCredentials:
        # TODO: eliminate hardcoded Terrascope internals
        path = f"TAP/big_data_services/openeo/sentinelhub-oauth-{alias}"
        data = self._kv_read_secret(path)
        creds = OAuthCredentials(client_id=data["client_id"], client_secret=data["client_secret"])
        _log.debug(f"{self!r}.get_sentinel_hub_credentials({alias!r}): {creds.client_id=}")
        return creds
