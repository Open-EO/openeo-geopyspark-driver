"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
"""

import logging
import os
import threading
from logging.config import dictConfig

from openeo_driver.server import run_gunicorn
from openeo_driver.views import build_app
from openeogeotrellis import deploy
from openeogeotrellis.deploy import flask_config, get_socket
from openeogeotrellis.job_registry import JobRegistry
from openeogeotrellis.job_tracker import JobTracker

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(name)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    },
    'loggers': {
        "gunicorn": {"level": "INFO"},
        'werkzeug': {'level': 'DEBUG'},
        'flask': {'level': 'DEBUG'},
        'openeo': {'level': 'DEBUG'},
        'openeo_driver': {'level': 'DEBUG'},
        'openeogeotrellis': {'level': 'INFO'}
    }
})


log = logging.getLogger(__name__)


def main():
    from pyspark import SparkContext
    print("starting spark context")
    SparkContext.getOrCreate()

    def setup_batch_jobs():
        with JobRegistry() as job_registry:
            job_registry.ensure_paths()

        job_tracker = JobTracker(JobRegistry, "", "")
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
