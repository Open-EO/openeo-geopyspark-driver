from openeo_driver.users.oidc import OidcProvider

from openeogeotrellis.config import GpsBackendConfig

oidc_providers = [
    OidcProvider(
        id="testid",
        issuer="https://oidc.test",
        scopes=["openid"],
        title="Test ID",
        default_clients=[
            {
                "id": "badcafef00d",
                "grant_types": [
                    "urn:ietf:params:oauth:grant-type:device_code+pkce",
                    "refresh_token",
                ],
            }
        ],
    ),
]


config = GpsBackendConfig(
    id="gps-test-dummy",
    capabilities_title="Dummy GeoPysSpark Backend",
    capabilities_description="Dummy GeoPysSpark Backend",
    oidc_providers=oidc_providers,
)
