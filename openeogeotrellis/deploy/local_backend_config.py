import os
from pathlib import Path

from openeo_driver.users.oidc import OidcProvider
from openeo_driver.workspace import DiskWorkspace
from openeogeotrellis.config import GpsBackendConfig
from openeogeotrellis.config.integrations.calrissian_config import CalrissianConfig


# TODO: avoid VITO default client here. Only provide simple basic auth by default?
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
        input_staging_image="alpine:3.17.0",
        calrissian_image="vito-docker.artifactory.vgt.vito.be/calrissian:vito.35baac4e",
        s3_bucket="calrissian",
    ),
    enable_basic_auth=True,
    valid_basic_auth=lambda name, password: (name == "openeo" and password == "openeo"),
    s3_bucket_name="openeo-data",
)
