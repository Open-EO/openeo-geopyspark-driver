import requests

# TODO: point this to prod
KEYCLOAK = "https://sso-int.terrascope.be"


def authenticate_oidc(client_id: str, client_secret: str) -> str:  # access token
    with requests.post(f"{KEYCLOAK}/auth/realms/terrascope/protocol/openid-connect/token",
                       headers={'Content-Type': 'application/x-www-form-urlencoded'},
                       data={
                           'grant_type': 'client_credentials',
                           'client_id': client_id,
                           'client_secret': client_secret
                       }) as resp:
        if not resp.ok:
            print(resp.text)

        resp.raise_for_status()
        return resp.json()["access_token"]
