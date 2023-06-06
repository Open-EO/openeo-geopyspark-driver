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

    logging_es_hosts = os.environ.get("LOGGING_ES_HOSTS","https://es-infra.vgt.vito.be")
    logging_es_index_pattern = os.environ.get("LOGGING_ES_INDEX_PATTERN","openeo-*-index-1m*")