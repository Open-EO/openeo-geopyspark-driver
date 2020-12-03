"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
"""

import logging
from logging.config import dictConfig
import sys
import threading
import os

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
        'werkzeug': {'level': 'DEBUG'},
        'flask': {'level': 'DEBUG'},
        'openeo': {'level': 'DEBUG'},
    }
})


from openeo_driver import server
from openeogeotrellis.job_tracker import JobTracker
from openeogeotrellis.job_registry import JobRegistry

log = logging.getLogger("openeo-geopyspark-driver.kubernetes")

def main():
    from pyspark import SparkContext
    print("starting spark context")
    sc = SparkContext.getOrCreate()

    from openeogeotrellis import get_backend_version, deploy
    from openeo_driver.views import build_backend_deploy_metadata

    host, _ = deploy.get_socket()
    port = 50001 if not 'KUBE_OPENEO_API_PORT' in os.environ else os.environ['KUBE_OPENEO_API_PORT']

    def setup_batch_jobs() -> None:
#        principal = sc.getConf().get("spark.yarn.principal")
#        keytab = sc.getConf().get("spark.yarn.keytab")

        with JobRegistry() as job_registry:
            job_registry.ensure_paths()

        job_tracker = JobTracker(JobRegistry, "", "")
        threading.Thread(target=job_tracker.update_statuses, daemon=True).start()

    def on_started() -> None:
        from openeo_driver.views import app

        app.logger.setLevel('DEBUG')
        deploy.load_custom_processes(app.logger)

        setup_batch_jobs()

    server.run(title="OpenEO API",
               description="""[UNSTABLE] OpenEO API running on CreoDIAS (using GeoPySpark driver). This endpoint runs openEO on a Kubernetes cluster.
                The main component can be found here: https://github.com/Open-EO/openeo-geopyspark-driver
                The deployment is configured using Terraform and Kubernetes configs: https://github.com/Open-EO/openeo-geotrellis-kubernetes
                Data is read directly from the CreoDIAS data offer through object storage. Processing is limited by the processing
                capacity of the Kubernetes cluster running on DIAS. Contact VITO for experiments with higher resource needs.
                """,
               deploy_metadata=build_backend_deploy_metadata(
                   packages=["openeo", "openeo_driver", "openeo-geopyspark", "openeo_udf", "geopyspark"]
                    # TODO: add version info about geotrellis-extensions jar?
                ),
               backend_version=get_backend_version(),
               threads=10,
               host=host,
               port=port,
               on_started=on_started)


if __name__ == '__main__':
    main()
