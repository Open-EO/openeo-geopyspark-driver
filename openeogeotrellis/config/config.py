from typing import List, Optional

import attrs
import os

from openeo_driver.config import OpenEoBackendConfig
from openeo_driver.users.oidc import OidcProvider


@attrs.frozen
class GpsBackendConfig(OpenEoBackendConfig):
    """
    Configuration for GeoPySpark backend.

    Meant to gradually replace ConfigParams (Open-EO/openeo-geopyspark-driver#285)
    """

    # identifier for this config
    id: Optional[str] = None

    oidc_providers: List[OidcProvider] = attrs.Factory(list)

    # Temporary feature flag for preventing to run UDFs in driver process (https://github.com/Open-EO/openeo-geopyspark-driver/issues/404)
    # TODO: remove this temporary feature flag
    allow_run_udf_in_driver: bool = False

    # TODO: eliminate hardcoded VITO-specific defaults?
    logging_es_hosts: str = os.environ.get("LOGGING_ES_HOSTS", "https://es-infra.vgt.vito.be")
    logging_es_index_pattern: str = os.environ.get("LOGGING_ES_INDEX_PATTERN", "openeo-*-index-1m*")

    vault_addr: Optional[str] = os.environ.get("VAULT_ADDR")

    ejr_api: Optional[str] = os.environ.get("OPENEO_EJR_API")
    ejr_backend_id: str = "unknown"
    ejr_credentials_vault_path: Optional[str] = os.environ.get("OPENEO_EJR_CREDENTIALS_VAULT_PATH")
