"""
Script to start a production server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
from logging.config import dictConfig
import sys
import threading


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


sys.path.insert(0, 'py4j-0.10.7-src.zip')
sys.path.insert(0, 'pyspark.zip')
from openeo_driver import server
from openeogeotrellis.job_tracker import JobTracker
from openeogeotrellis.job_registry import JobRegistry


log = logging.getLogger("openeo-geopyspark-driver.probav-mep")


def main():
    from pyspark import SparkContext, SparkConf

    conf = (SparkConf()
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set(key='spark.kryo.registrator', value='geopyspark.geotools.kryo.ExpandedKryoRegistrator,'
                                                     'org.openeo.geotrellis.png.KryoRegistrator'))
    print("starting spark context")
    sc = SparkContext(conf=conf)

    from openeogeotrellis import get_backend_version, deploy
    from openeo_driver.views import build_backend_deploy_metadata

    host, port = deploy.get_socket()

    def setup_batch_jobs() -> None:
        principal = sc.getConf().get("spark.yarn.principal")
        keytab = sc.getConf().get("spark.yarn.keytab")

        with JobRegistry() as job_registry:
            job_registry.ensure_paths()

        job_tracker = JobTracker(JobRegistry, principal, keytab)
        threading.Thread(target=job_tracker.update_statuses, daemon=True).start()

    def on_started() -> None:
        from openeo_driver.views import app

        app.logger.setLevel('DEBUG')
        deploy.load_custom_processes(app.logger)

        setup_batch_jobs()

    server.run(title="VITO Remote Sensing openEO API",
               description="OpenEO API to the VITO Remote Sensing product catalog and processing services (using "
                           "GeoPySpark driver).",
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
