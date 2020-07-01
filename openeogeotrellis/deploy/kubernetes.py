"""
Script to start a production server. This script can serve as the entry-point for doing spark-submit.
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

log = logging.getLogger("openeo-geopyspark-driver.kubernetes")

def main():
    from pyspark import SparkContext
    print("starting spark context")
    sc = SparkContext.getOrCreate()

    from openeogeotrellis import get_backend_version, deploy
    from openeo_driver.views import build_backend_deploy_metadata

    host, _ = deploy.get_socket()
    port = 50001 if not 'KUBE_OPENEO_API_PORT' in os.environ else os.environ['KUBE_OPENEO_API_PORT']

    def on_started() -> None:
        from openeo_driver.views import app

        app.logger.setLevel('DEBUG')

    server.run(title="OpenEO API",
               description="OpenEO API (using GeoPySpark driver).",
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

