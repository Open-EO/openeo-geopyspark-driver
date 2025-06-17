import os
from pathlib import Path

from openeo_driver.users.oidc import OidcProvider
from openeo_driver.workspace import DiskWorkspace
from openeogeotrellis.config import GpsBackendConfig
from openeogeotrellis.config.integrations.calrissian_config import CalrissianConfig

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

if "OPENEO_CATALOG_FILES" in os.environ:
    layer_catalog_files = os.environ["OPENEO_CATALOG_FILES"].split(",")
else:
    layer_catalog_files = [str(Path(__file__).parent / "simple_layercatalog.json")]

config = GpsBackendConfig(
    id="gps-local",
    layer_catalog_files=layer_catalog_files,
    capabilities_title="Local GeoPySpark openEO Backend",
    capabilities_description="Local GeoPySpark openEO Backend",
    oidc_providers=oidc_providers,
    opensearch_enrich=False,
    use_zk_job_registry=False,
    workspaces=workspaces,
    calrissian_config=CalrissianConfig(
        namespace="calrissian-demo-project",
        input_staging_image="registry.stag.waw3-1.openeo-int.v1.dataspace.copernicus.eu/rand/alpine:3",
        calrissian_image="registry.stag.waw3-1.openeo-int.v1.dataspace.copernicus.eu/rand/calrissian:latest",
        s3_bucket="calrissian",
    ),
)
