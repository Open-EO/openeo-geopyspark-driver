from __future__ import annotations

import abc
import os
from typing import List, Optional, Union

import attrs
from openeo_driver.config import OpenEoBackendConfig, from_env_as_list
from openeo_driver.users import User
from openeo_driver.users.oidc import OidcProvider
from openeo_driver.util.auth import ClientCredentials
from openeo_driver.utils import smart_bool

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
    setup_kerberos_auth: bool = not smart_bool(os.environ.get("KUBE", False))

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
    zk_job_registry_max_specification_size: Optional[int] = None

    ejr_api: Optional[str] = os.environ.get("OPENEO_EJR_API")
    ejr_backend_id: str = "unknown"
    ejr_credentials_vault_path: Optional[str] = os.environ.get("OPENEO_EJR_CREDENTIALS_VAULT_PATH")

    # TODO: eliminate hardcoded Terrascope references
    # TODO #531 eliminate this config favor of etl_api_config strategy below
    etl_api: Optional[str] = os.environ.get("OPENEO_ETL_API", "https://etl.terrascope.be")
    etl_source_id: str = "TerraScope/MEP"
    use_etl_api_on_sync_processing: bool = False
    etl_dynamic_api_flag: Optional[str] = None  # TODO #531 eliminate this temporary feature flag? Unused now

    # TODO #531 this config is meant to replace `etl_api` from above
    etl_api_config: Optional[EtlApiConfig] = None

    prometheus_api: Optional[str] = os.environ.get("OPENEO_PROMETHEUS_API")

    max_executor_or_driver_memory: str = "64G"  # Executors and drivers have the same amount of memory

    # TODO #734: these "usage" defaults are a hack in absence of more accurate credit attribution
    #   for synchronous processing. Can they be eliminated?
    default_usage_cpu_seconds: float = 1.5 * 3600
    default_usage_byte_seconds: float = 3 * 1024 * 1024 * 1024 * 3600
    report_usage_sentinelhub_pus: bool = True
    batch_job_base_fee_credits: Optional[float] = None

    default_soft_errors: float = 0.1

    s1backscatter_elev_geoid: Optional[str] = os.environ.get("OPENEO_S1BACKSCATTER_ELEV_GEOID")

    s3_bucket_name: str = os.environ.get("SWIFT_BUCKET", "OpenEO-data")

    fuse_mount_batchjob_s3_bucket: bool = smart_bool(os.environ.get("FUSE_MOUNT_BATCHJOB_S3_BUCKET", False))
    fuse_mount_batchjob_s3_mounter: str = os.environ.get("FUSE_MOUNT_BATCHJOB_S3_MOUNTER", "s3fs")
    fuse_mount_batchjob_s3_mount_options: str = os.environ.get("FUSE_MOUNT_BATCHJOB_S3_MOUNT_OPTIONS", "-o uid=18585 -o gid=18585 -o compat_dir")
    fuse_mount_batchjob_s3_storage_class: str = os.environ.get("FUSE_MOUNT_BATCHJOB_S3_STORAGE_CLASS", "csi-s3")

    batch_scheduler: str = "default-scheduler"
    yunikorn_queue: str = os.environ.get("YUNIKORN_QUEUE", "root.default")
    yunikorn_scheduling_timeout: str = os.environ.get("YUNIKORN_SCHEDULING_TIMEOUT", "10800")

    """
    Reading strategy for load_collection and load_stac processes:
    load_by_target_partition: first apply a partitioner, then simply read data for each partition
    This avoids moving data around after reading, allowing the reading step to be followed immediately by next processes.
    load_per_product: group by source product filename, then perform reads, then regroup data according to optimal partitioner for subsequent processes.
    This strategy is faster when there is a high overhead/latency of opening products, for instance observed when reading jpeg2000 on S3. It does require
    the data to be moved around in the cluster for subsequent processing.

    The default can be overridden by feature_flags.
    """
    default_reading_strategy: str = "load_by_target_partition"

    """
    Controls the default number of threads for the batch job executors Java Virtual Machine.
    These threads are used for additional parallelism, for instance when reading data, but may cause threading issues if
    used in combination with not thread-safe libraries.
    When reading with GDAL, a lower number of threads may be beneficial, as GDAL also performs its own threading.
    """
    default_executor_threads_jvm: int = 10

    """
    The default number of datasets that will be cached in batch jobs. A large cache will increase memory usage.
    """
    default_gdal_dataset_cache_size: int = 16

    """
    The default cache size in megabytes, which is used by GDAL. This setting applies to batch jobs.
    """
    default_gdal_cachemax: int = 150

    """
    The default maximum number of executors for batch jobs. A high number of executors can cause high costs, as allocated
    executors may be idle for a relatively high amount of time.
    We set this to 20 in kubernetes environments.
    """
    default_max_executors: int = 100

    default_driver_memory: str = "8G"

    default_driver_memoryOverhead: str = "2G"

    default_executor_memory: str = "2G"

    default_executor_memoryOverhead: str = "3G"

    default_executor_cores: int = 2

    """
    The default tile size to use for processing. By default, it is not set and the backend tries to determine a value.
    To minimize memory use, a small default size like 128 can be set. For cases with more memory per cpu, larger sizes are relevant.
    """
    default_tile_size:Optional[int] = None

    job_dependencies_poll_interval_seconds: float = 60  # poll every x seconds
    job_dependencies_max_poll_delay_seconds: float = 60 * 60 * 24 * 7  # for a maximum delay of y seconds

    udf_dependencies_sleep_after_install: Optional[float] = None

    """
    Only used by YARN, allows to specify paths to mount in batch job docker containers.
    """
    batch_docker_mounts: str = (
        "/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,"
        "/var/lib/sss/pipes:/var/lib/sss/pipes:rw,"
        "/usr/hdp/current/:/usr/hdp/current/:ro,"
        "/etc/hadoop/conf/:/etc/hadoop/conf/:ro,"
        "/etc/krb5.conf:/etc/krb5.conf:ro,"
        "/data/MTDA:/data/MTDA:ro,"
        "/data/projects/OpenEO:/data/projects/OpenEO:rw,"
        "/data/MEP:/data/MEP:ro,"
        "/data/users:/data/users:rw,"
        "/tmp_epod:/tmp_epod:rw,"
        "/opt/tensorflow:/opt/tensorflow:ro"
    )
    batch_user_docker_mounts: dict[str, List[str]] = {}
