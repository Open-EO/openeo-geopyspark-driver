import requests
import sys


def _authenticate_oidc(client_id: str, client_secret: str) -> str:  # access token
    with requests.post("https://sso-int.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
                       headers={'Content-Type': 'application/x-www-form-urlencoded'},
                       data={
                           'grant_type': 'client_credentials',
                           'client_id': client_id,
                           'client_secret': client_secret
                       }) as resp:
        resp.raise_for_status()
        return resp.json()["access_token"]


def main(argv):
    client_id, client_secret = argv[1:]

    access_token = _authenticate_oidc(client_id, client_secret)
    print(access_token)


if __name__ == '__main__':
    main(sys.argv)
