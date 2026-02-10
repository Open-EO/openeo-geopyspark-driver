"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
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
from openeogeotrellis.deploy import get_socket
from openeogeotrellis.integrations.kubernetes import kube_client
from openeogeotrellis.job_registry import EagerlyK8sTrackingInMemoryJobRegistry

log = logging.getLogger(__name__)


def main():
    # By default, use JSON logging to stderr,
    # but allow overriding this with an environment variable
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

    from pyspark import SparkContext

    log.info("starting spark context")
    SparkContext.getOrCreate()

    def on_started():
        app.logger.setLevel("DEBUG")
        deploy.load_custom_processes()

    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    backend_implementation = GeoPySparkBackendImplementation(
        use_job_registry=bool(get_backend_config().ejr_api),
        elastic_job_registry=(
            None if get_backend_config().ejr_api  # instantiates an ElasticJobRegistry from the environment
            else EagerlyK8sTrackingInMemoryJobRegistry(kube_client("CustomObject"))  # an in-memory one for free
        ),
    )
    app = build_app(backend_implementation=backend_implementation)

    # https://github.com/Open-EO/openeo-python-driver/issues/242
    # A more generic deployment specific override system does not yet exist, so do it here.
    processes = backend_implementation.processing.get_process_registry("1.2.0")
    backscatter_spec = processes.get_spec("sar_backscatter")
    backscatter_spec["experimental"] = False
    backscatter_spec["description"] = (
        backscatter_spec["description"]
        + """
    \n\n ## Backend notes \n\n The implementation in this backend is based on Orfeo Toolbox.
    """
    )
    parameters = {p["name"]: p for p in backscatter_spec["parameters"]}
    parameters["coefficient"]["default"] = "sigma0-ellipsoid"
    parameters["coefficient"][
        "description"
    ] = "Select the radiometric correction coefficient. The following options are available:\n\n* `sigma0-ellipsoid`: ground area computed with ellipsoid earth model\n"
    parameters["coefficient"]["schema"] = [
        {"type": "string", "enum": ["sigma0-ellipsoid"]},
        {"title": "Non-normalized backscatter", "type": "null"},
    ]
    backscatter_spec["links"].append(
        {
            "rel": "about",
            "href": "https://www.orfeo-toolbox.org/CookBook/Applications/app_SARCalibration.html",
            "title": "Orfeo toolbox backscatter processor.",
        }
    )

    host = os.environ.get('SPARK_LOCAL_IP', None)
    if host is None:
        host, _ = get_socket()
    port = os.environ.get('KUBE_OPENEO_API_PORT', 50001)

    run_gunicorn(
        app,
        threads=30,
        host=host,
        port=port,
        on_started=on_started
    )



if __name__ == '__main__':
    main()
