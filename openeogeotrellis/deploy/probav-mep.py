"""
Script to start a production server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
import sys

from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import get_logging_config, setup_logging
from openeo_driver.views import build_app

sys.path.insert(0, 'py4j-0.10.7-src.zip')
sys.path.insert(0, 'pyspark.zip')
from openeogeotrellis.deploy import flask_config, get_socket, load_custom_processes
from openeogeotrellis.job_registry import JobRegistry


log = logging.getLogger("openeogeotrellis.deploy.probav-mep")


def main():
    setup_logging(get_logging_config(
        root_handlers=["json"],
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

    from pyspark import SparkContext, SparkConf

    conf = (SparkConf()
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set(key='spark.kryo.registrator', value='geopyspark.geotools.kryo.ExpandedKryoRegistrator')
            .set("spark.kryo.classesToRegister","org.openeo.geotrellisaccumulo.SerializableConfiguration,ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion"))
    log.info("starting spark context")
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
