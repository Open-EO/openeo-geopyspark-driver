import os
from typing import List, Optional

import attrs
from openeo_driver.config import OpenEoBackendConfig
from openeo_driver.users.oidc import OidcProvider

from openeogeotrellis import get_backend_version
from openeogeotrellis.deploy import build_gps_backend_deploy_metadata, find_geotrellis_jars


def _default_capabilities_deploy_metadata() -> dict:
    metadata = build_gps_backend_deploy_metadata(
        packages=[
            "openeo",
            "openeo_driver",
            "openeo-geopyspark",
            "geopyspark",
        ],
        jar_paths=find_geotrellis_jars(),
    )
    return metadata


@attrs.frozen(kw_only=True)
class GpsBackendConfig(OpenEoBackendConfig):
    """
    Configuration for GeoPySpark backend.

    Meant to gradually replace ConfigParams (Open-EO/openeo-geopyspark-driver#285)
    """

    # identifier for this config
    id: Optional[str] = None

    capabilities_backend_version: str = get_backend_version()
    capabilities_deploy_metadata: dict = attrs.Factory(_default_capabilities_deploy_metadata)
    processing_software = f"openeo-geopyspark-driver-{get_backend_version()}"

    oidc_providers: List[OidcProvider] = attrs.Factory(list)

    # Temporary feature flag for preventing to run UDFs in driver process (https://github.com/Open-EO/openeo-geopyspark-driver/issues/404)
    # TODO: remove this temporary feature flag
    allow_run_udf_in_driver: bool = False

    # TODO: avoid KUBE env var and just default to False in general
    setup_kerberos_auth: bool = not os.environ.get("KUBE", False)

    # TODO: possible to disable enrichment by default?
    opensearch_enrich: bool = True
    # TODO: eliminate hardcoded VITO/Terrascope resources
    default_opensearch_endpoint: str = "https://services.terrascope.be/catalogue"

    # TODO: eliminate hardcoded VITO-specific defaults?
    logging_es_hosts: List[str] = os.environ.get("LOGGING_ES_HOSTS", "https://es-infra.vgt.vito.be").split(",")
    logging_es_index_pattern: str = os.environ.get("LOGGING_ES_INDEX_PATTERN", "openeo-*-index-1m*")

    vault_addr: Optional[str] = os.environ.get("VAULT_ADDR")

    ejr_api: Optional[str] = os.environ.get("OPENEO_EJR_API")
    ejr_backend_id: str = "unknown"
    ejr_credentials_vault_path: Optional[str] = os.environ.get("OPENEO_EJR_CREDENTIALS_VAULT_PATH")

    etl_source_id: str = "TerraScope/MEP"  # TODO: eliminate hardcoded VITO reference

    prometheus_api: Optional[str] = os.environ.get("OPENEO_PROMETHEUS_API")
