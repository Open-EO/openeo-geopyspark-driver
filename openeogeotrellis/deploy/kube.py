"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
"""

import logging
import os

from openeo_driver.server import run_gunicorn, build_backend_deploy_metadata
from openeo_driver.util.logging import get_logging_config, setup_logging
from openeo_driver.views import build_app
from openeogeotrellis import deploy
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.deploy import flask_config, get_socket
from openeogeotrellis.job_registry import ZkJobRegistry

log = logging.getLogger(__name__)


def main():
    setup_logging(get_logging_config(
        root_handlers=["stderr_json"],
        loggers={
            "openeo": {"level": "DEBUG"},
            "openeo_driver": {"level": "DEBUG"},
            'openeogeotrellis': {'level': 'DEBUG'},
            "flask": {"level": "DEBUG"},
            "werkzeug": {"level": "DEBUG"},
            "gunicorn": {"level": "INFO"},
            'kazoo': {'level': 'WARN'},
        },
    ))

    from pyspark import SparkContext
    log.info("starting spark context")
    SparkContext.getOrCreate()

    def setup_batch_jobs():
        if not ConfigParams().is_ci_context:
            with ZkJobRegistry() as job_registry:
                job_registry.ensure_paths()

    def on_started():
        app.logger.setLevel('DEBUG')
        deploy.load_custom_processes()
        setup_batch_jobs()

    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    app = build_app(backend_implementation=GeoPySparkBackendImplementation())
    # TODO avoid need for `.from_object(flask_config)`? Automatically handle this from OpenEoBackendConfig? (Open-EO/openeo-python-driver#204)
    app.config.from_object(flask_config)

    host = os.environ.get('SPARK_LOCAL_IP', None)
    if host is None:
        host, _ = get_socket()
    port = os.environ.get('KUBE_OPENEO_API_PORT', 50001)

    run_gunicorn(
        app,
        threads=10,
        host=host,
        port=port,
        on_started=on_started
    )


if __name__ == '__main__':
    main()
