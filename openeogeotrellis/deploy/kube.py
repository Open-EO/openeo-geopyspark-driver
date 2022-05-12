"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
"""

import logging
import os
import threading

from openeo_driver.server import run_gunicorn, build_backend_deploy_metadata
from openeo_driver.util.logging import get_logging_config, setup_logging
from openeo_driver.views import build_app
from openeogeotrellis import deploy
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.deploy import flask_config, get_socket
from openeogeotrellis.job_registry import JobRegistry
from openeogeotrellis.job_tracker import JobTracker

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
            with JobRegistry() as job_registry:
                job_registry.ensure_paths()

            job_tracker = JobTracker(JobRegistry, principal="", keytab="")
            threading.Thread(target=job_tracker.loop_update_statuses, daemon=True).start()

    def on_started():
        app.logger.setLevel('DEBUG')
        deploy.load_custom_processes()
        setup_batch_jobs()

    from openeogeotrellis.backend import GeoPySparkBackendImplementation
    app = build_app(backend_implementation=GeoPySparkBackendImplementation())
    app.config.from_object(flask_config)
    app.config.from_mapping(
        # TODO: move this VITO/CreoDIAS specific description to CreoDIAS deploy repo.
        OPENEO_DESCRIPTION="""
            [UNSTABLE] OpenEO API running on CreoDIAS (using GeoPySpark driver). This endpoint runs openEO on a Kubernetes cluster.
            The main component can be found here: https://github.com/Open-EO/openeo-geopyspark-driver
            The deployment is configured using Terraform and Kubernetes configs: https://github.com/Open-EO/openeo-geotrellis-kubernetes
            Data is read directly from the CreoDIAS data offer through object storage. Processing is limited by the processing
            capacity of the Kubernetes cluster running on DIAS. Contact VITO for experiments with higher resource needs.
        """,
        OPENEO_BACKEND_DEPLOY_METADATA=build_backend_deploy_metadata(
            packages=[
                "openeo",
                "openeo_driver",
                "openeo-geopyspark",
                "openeo_udf",
                "geopyspark",
                "cropsar",
                "nextland_services",
                "biopar",
                "cropsar_px",
            ]
        ),
        SIGNED_URL=True,
        SIGNED_URL_SECRET=os.environ.get("SIGNED_URL_SECRET"),
        SIGNED_URL_EXPIRATION=int(os.environ.get( "SIGNED_URL_EXPIRATION",str(7 * 24 * 60 * 60)))
    )

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
