from __future__ import annotations

import abc
import os
from typing import List, Optional, Union

import attrs
from openeo_driver.config import OpenEoBackendConfig, from_env_as_list
from openeo_driver.users import User
from openeo_driver.users.oidc import OidcProvider
from openeo_driver.util.auth import ClientCredentials

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


class EtlApiConfig(metaclass=abc.ABCMeta):
    """
    Interface for configuration of ETL API access (possibly dynamic based on user, ...).

    The basic design idea of this interface is to use the ETL API root URL as full identifier of an ETL API.
    Being a simple string, instead of a complex object (e.g. an `EtlApi` instance), it does not raise
    serialization challenges when there is need to pass it from the web app context to the batch job tracker context.

    The ETL API selection strategy is to be implemented in `get_root_url()`,
    which can use the provided user object or job options to pick the appropriate ETL API root URL.
    Note that there is no guarantee that either of these inputs is available.
    Typically, the job options will be provided in a job tracker context
    and the user object will be provided in a synchronous processing request context.

    Additional dependencies to (re)construct an operational `EtlApi` instance can be obtained with dedicated methods
    using the ETL API root URL identifier, e.g. client credentials with `get_client_credentials(root_url)`.
    """

    @abc.abstractmethod
    def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
        """Get root URL of the ETL API"""
        ...

    def get_client_credentials(self, root_url: str) -> Union[ClientCredentials, None]:
        """Get client credentials corresponding to root URL."""
        return None


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
    # TODO: this index pattern is specifically used for fetching batch job logs, which is not obvious from the name (and env var)
    logging_es_index_pattern: str = os.environ.get("LOGGING_ES_INDEX_PATTERN", "openeo-*-index-1m*")

    # TODO eliminate hardcoded VITO reference
    vault_addr: Optional[str] = os.environ.get("VAULT_ADDR", "https://vault.vgt.vito.be")

    zookeeper_hosts: List[str] = attrs.Factory(
        from_env_as_list(
            "ZOOKEEPERNODES",
            # TODO: eliminate hardcoded default once config is set where necessary
            default="epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181",
        )
    )
    zookeeper_root_path: str = attrs.field(
        default="/openeo", validator=attrs.validators.matches_re("^/.+"), converter=lambda s: s.rstrip("/")
    )

    # TODO #236/#498/#632 long term goal is to fully disable ZK job registry, but for now it's configurable.
    use_zk_job_registry: bool = True

    ejr_api: Optional[str] = os.environ.get("OPENEO_EJR_API")
    ejr_backend_id: str = "unknown"
    ejr_credentials_vault_path: Optional[str] = os.environ.get("OPENEO_EJR_CREDENTIALS_VAULT_PATH")

    # TODO: eliminate hardcoded Terrascope references
    # TODO #531 eliminate this config favor of etl_api_config strategy below
    etl_api: Optional[str] = os.environ.get("OPENEO_ETL_API", "https://etl.terrascope.be")
    etl_source_id: str = "TerraScope/MEP"
    use_etl_api_on_sync_processing: bool = False
    etl_dynamic_api_flag: Optional[str] = None  # TODO #531 eliminate this temporary feature flag?

    # TODO #531 this config is meant to replace `etl_api` from above
    etl_api_config: Optional[EtlApiConfig] = None

    prometheus_api: Optional[str] = os.environ.get("OPENEO_PROMETHEUS_API")

    max_executor_or_driver_memory: str = "64G"  # Executors and drivers have the same amount of memory

    default_usage_cpu_seconds: float = 1 * 3600
    default_usage_byte_seconds: float = 2 * 1024 * 1024 * 1024 * 3600

    default_soft_errors: float = 0.1

    s1backscatter_elev_geoid: Optional[str] = os.environ.get("OPENEO_S1BACKSCATTER_ELEV_GEOID")

    s3_bucket_name: str = os.environ.get("SWIFT_BUCKET", "OpenEO-data")

    fuse_mount_batchjob_s3_bucket: bool = os.environ.get("FUSE_MOUNT_BATCHJOB_S3_BUCKET", False)
