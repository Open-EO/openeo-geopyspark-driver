import os
from pathlib import Path

from openeo_driver.users.oidc import OidcProvider
from openeo_driver.workspace import DiskWorkspace
from openeogeotrellis.config import GpsBackendConfig

oidc_default_client_egi = {
    "id": "vito-default-client",
    "grant_types": [
        "urn:ietf:params:oauth:grant-type:device_code+pkce",
        "refresh_token",
    ],
    "redirect_urls": [],
}

oidc_providers = [
    OidcProvider(
        id="egi",
        issuer="https://aai.egi.eu/auth/realms/egi/",
        scopes=["openid"],
        title="EGI Check-in",
        default_clients=[oidc_default_client_egi],
    ),
]

os.makedirs("/tmp/workspace", exist_ok=True)
workspaces = {"tmp_workspace": DiskWorkspace(root_directory=Path("/tmp/workspace"))}

config = GpsBackendConfig(
    id="gps-local",
    capabilities_title="Local GeoPySpark openEO Backend",
    capabilities_description="Local GeoPySpark openEO Backend",
    oidc_providers=oidc_providers,
    opensearch_enrich=False,
    use_zk_job_registry=False,
    workspaces=workspaces,
)
