from pathlib import Path

from openeo_driver.users.oidc import OidcProvider
from openeo_driver.workspace import DiskWorkspace

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

workspaces = {
    "tmp": DiskWorkspace(root_directory=Path("/tmp"))
}


config = GpsBackendConfig(
    id="gps-test-dummy",
    capabilities_title="Dummy GeoPysSpark Backend",
    capabilities_description="Dummy GeoPysSpark Backend",
    opensearch_enrich=False,
    # TODO: avoid hardcoded reference to VITO/Terrascope resource
    default_opensearch_endpoint="https://services.terrascope.be/catalogue/",
    oidc_providers=oidc_providers,
    zookeeper_hosts=["zk.test"],
    zookeeper_root_path="/openeo-test",
    etl_api="https://etl-api.test",
    etl_source_id="openeo-gps-tests",
    vault_addr="https://vault.test",
    enable_basic_auth=True,
    valid_basic_auth=lambda u, p: p == f"{u}123",
    workspaces=workspaces,
)
