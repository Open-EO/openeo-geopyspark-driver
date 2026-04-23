"""
Lightweight REST API server for Kubernetes that does NOT start a SparkContext.

This is intended to serve light-weight endpoints (e.g. /conformance, /collections,
/processes) on dedicated pods. A load balancer routes those requests here, while
heavier requests that require a SparkContext (e.g. POST /jobs/<id>/results) are
routed to pods started via kube.py.

Because GeoPySparkBackendImplementation.__init__ calls get_jvm() and
SparkContext.getOrCreate() unconditionally, we define a subclass here that
skips those calls and omits GpsBatchJobs entirely.
"""

import logging
import os

from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import (
    LOG_HANDLER_STDERR_JSON,
    get_logging_config,
    setup_logging,
)
from openeo_driver.views import build_app
from openeogeotrellis import deploy
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.deploy import get_socket, patch_sar_backscatter_spec

log = logging.getLogger(__name__)


def create_lite_backend_implementation():
    """
    Build a backend implementation that is safe to construct without a SparkContext
    or JVM. GpsBatchJobs (which requires both) is intentionally omitted.
    """
    from openeo_driver import backend as openeo_driver_backend
    from openeo_driver.util.http import requests_with_retry
    from openeogeotrellis.backend import (
        GeoPySparkBackendImplementation,
        GpsProcessing,
        GpsUdfRuntimes,
        get_elastic_job_registry,
    )
    from openeogeotrellis.configparams import ConfigParams
    from openeogeotrellis.layercatalog import get_layer_catalog
    from openeogeotrellis.user_defined_process_repository import (
        InMemoryUserDefinedProcessRepository,
        ZooKeeperUserDefinedProcessRepository,
    )
    from openeogeotrellis.vault import Vault

    class LiteBackendImplementation(GeoPySparkBackendImplementation):
        """
        Subclass of GeoPySparkBackendImplementation that skips JVM/Spark
        initialisation so it can run in a lightweight pod.
        """

        def __init__(self):
            use_zookeeper = True
            if not use_zookeeper or ConfigParams().is_ci_context:
                user_defined_processes = InMemoryUserDefinedProcessRepository()
            else:
                user_defined_processes = ZooKeeperUserDefinedProcessRepository(
                    hosts=ConfigParams().zookeepernodes,
                    zk_client_reuse=get_backend_config().udp_registry_zookeeper_client_reuse,
                )

            requests_session = requests_with_retry(total=3, backoff_factor=2)
            vault = Vault(get_backend_config().vault_addr, requests_session)
            catalog = get_layer_catalog(vault=vault)
            udf_runtimes = GpsUdfRuntimes()

            # Skip get_jvm(), SparkContext and GpsBatchJobs – call grandparent directly.
            openeo_driver_backend.OpenEoBackendImplementation.__init__(
                self,
                catalog=catalog,
                batch_jobs=None,  # No Spark-based batch jobs in lite pods
                user_defined_processes=user_defined_processes,
                processing=GpsProcessing(),
                udf_runtimes=udf_runtimes,
            )

            # Spark fields. These are set by GeoPySparkBackendImplementation.__init__ normally;
            # provide safe defaults so any inherited method that reads them won't crash.
            self._principal = None
            self._key_tab = None

    return LiteBackendImplementation()


def main():
    root_handler = os.environ.get("OPENEO_LOGGING_ROOT_HANDLER", LOG_HANDLER_STDERR_JSON)

    setup_logging(
        get_logging_config(
            root_handlers=[root_handler],
            loggers={
                "openeo": {"level": "DEBUG"},
                "openeo_driver": {"level": "DEBUG"},
                "openeogeotrellis": {"level": "DEBUG"},
                "flask": {"level": "DEBUG"},
                "werkzeug": {"level": "DEBUG"},
                "gunicorn": {"level": "INFO"},
                "kazoo": {"level": "WARN"},
            },
        )
    )

    backend_implementation = create_lite_backend_implementation()
    app = build_app(backend_implementation=backend_implementation)
    patch_sar_backscatter_spec(backend_implementation)

    def on_started():
        app.logger.setLevel("DEBUG")
        deploy.load_custom_processes()

    host, _ = get_socket()
    port = os.environ.get("KUBE_OPENEO_API_PORT", 50001)

    run_gunicorn(
        app,
        threads=30,
        host=host,
        port=port,
        on_started=on_started,
    )


if __name__ == "__main__":
    main()