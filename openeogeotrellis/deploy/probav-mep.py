"""
Script to start a production server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
import sys
from logging.config import dictConfig

from openeo_driver.server import run_gunicorn
from openeo_driver.views import build_app

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
        'openeogeotrellis': {'level': 'DEBUG'}
    }
})

sys.path.insert(0, 'py4j-0.10.7-src.zip')
sys.path.insert(0, 'pyspark.zip')
from openeogeotrellis.deploy import flask_config, get_socket, load_custom_processes
from openeogeotrellis.job_registry import JobRegistry


log = logging.getLogger("openeogeotrellis.deploy.probav-mep")


def main():
    from pyspark import SparkContext, SparkConf

    conf = (SparkConf()
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set(key='spark.kryo.registrator', value='geopyspark.geotools.kryo.ExpandedKryoRegistrator')
            .set("spark.kryo.classesToRegister","org.openeo.geotrellisaccumulo.SerializableConfiguration,ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion"))
    print("starting spark context")
    SparkContext(conf=conf)

    def setup_batch_jobs() -> None:
        with JobRegistry() as job_registry:
            job_registry.ensure_paths()

    def on_started() -> None:
        app.logger.setLevel('DEBUG')
        load_custom_processes()
        setup_batch_jobs()

    from openeogeotrellis.backend import GeoPySparkBackendImplementation
    app = build_app(backend_implementation=GeoPySparkBackendImplementation())
    app.config.from_object(flask_config)

    host, port = get_socket()

    run_gunicorn(
        app,
        threads=10,
        host=host,
        port=port,
        on_started=on_started
    )


if __name__ == '__main__':
    main()
