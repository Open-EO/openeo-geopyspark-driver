import requests
import sys

KEYCLOAK = "https://sso-int.terrascope.be"
ETL_API = "https://etl-dev.terrascope.be"


def _authenticate_oidc(client_id: str, client_secret: str) -> str:  # access token
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


def _can_execute_request(access_token: str) -> bool:
    with requests.get(f"{ETL_API}/user/permissions", headers={'Authorization': f"Bearer {access_token}"}) as resp:
        if not resp.ok:
            print(resp.text)

        resp.raise_for_status()

        return resp.json()["execution"]


def main(argv):
    client_id, client_secret = argv[1:3]

    access_token = _authenticate_oidc(client_id, client_secret)
    print(access_token)

    assert _can_execute_request(access_token)


if __name__ == '__main__':
    main(sys.argv)
