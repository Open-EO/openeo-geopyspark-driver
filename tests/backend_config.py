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
    opensearch_enrich=False,
    # TODO: avoid hardcoded reference to VITO/Terrascope resource
    default_opensearch_endpoint="https://services.terrascope.be/catalogue/",
    oidc_providers=oidc_providers,
    etl_api="https://etl-api.test",
    vault_addr="https://vault.test",
    etl_api_oidc_issuer="https://oidc.test",
    enable_basic_auth=True,
    valid_basic_auth=lambda u, p: p == f"{u}123",
)
