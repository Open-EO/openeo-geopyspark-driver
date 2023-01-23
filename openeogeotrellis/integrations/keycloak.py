import logging

import requests

# TODO: point this to prod
KEYCLOAK = "https://sso-int.terrascope.be"

_log = logging.getLogger(__name__)


def authenticate_oidc(client_id: str, client_secret: str, logging_context=None) -> str:
    if logging_context is None:
        logging_context = {}

    with requests.post(f"{KEYCLOAK}/auth/realms/terrascope/protocol/openid-connect/token", data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }) as resp:
        if not resp.ok:
            _log.error(f"{resp.request.method} {resp.request.url} returned {resp.status_code}: {resp.text}",
                       extra=logging_context)

        resp.raise_for_status()
        return resp.json()["access_token"]
