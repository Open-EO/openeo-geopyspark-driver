"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
import os
import sys
from glob import glob
from pathlib import Path

import openeo_driver.config.load
from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import get_logging_config, setup_logging, show_log_level, LOG_HANDLER_STDERR_JSON
from openeo_driver.utils import smart_bool
from openeo_driver.views import build_app
from openeogeotrellis.config import get_backend_config



_log = logging.getLogger(__name__)


def setup_local_spark(additional_jar_dirs=[]):
    # TODO: make this more reusable (e.g. also see `_setup_local_spark` in tests/conftest.py)
    from pyspark import SparkContext, find_spark_home

    spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]
    _log.debug('sys.path: {p!r}'.format(p=sys.path))
    master_str = "local[2]"

    from geopyspark import geopyspark_conf
    conf = geopyspark_conf(master=master_str, appName="openeo-geotrellis-local", additional_jar_dirs=additional_jar_dirs)
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    conf.set(key='spark.kryo.registrator', value='geotrellis.spark.store.kryo.KryoRegistrator')
    conf.set('spark.ui.enabled', True)
    # Some options to allow attaching a Java debugger to running Spark driver
    conf.set('spark.driver.extraJavaOptions', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009')

    conf.set(key='spark.driver.memory', value='2G')
    conf.set(key='spark.executor.memory', value='2G')

    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable

    _log.info('Creating Spark context with config:')
    for k, v in conf.getAll():
        _log.info("Spark config: {k!r}: {v!r}".format(k=k, v=v))
    pysc = SparkContext.getOrCreate(conf)
    _log.info('Created Spark Context {s}'.format(s=pysc))
    _log.info('Spark web UI: http://localhost:{p}/'.format(p=pysc.getConf().get('spark.ui.port') or 4040))

    return pysc


def on_started() -> None:
    show_log_level(logging.getLogger('gunicorn.error'))
    show_log_level(logging.getLogger('flask'))
    show_log_level(logging.getLogger('werkzeug'))


if __name__ == '__main__':
    root_handlers = [LOG_HANDLER_STDERR_JSON]
    if smart_bool(os.environ.get("OPENEO_DRIVER_SIMPLE_LOGGING")):
        root_handlers = None

    setup_logging(get_logging_config(
        root_handlers=root_handlers,
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
    _log.info(repr({"pid": os.getpid(), "interpreter": sys.executable, "version": sys.version, "argv": sys.argv}))

    setup_local_spark()

    os.environ.setdefault(
        openeo_driver.config.load.ConfigGetter.OPENEO_BACKEND_CONFIG,
        str(Path(__file__).parent / "local_config.py"),
    )

    # Note: local import is necessary because `openeogeotrellis.backend` requires `SPARK_HOME` env var
    # which we want to set up just in time
    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    backend_implementation = GeoPySparkBackendImplementation(
        use_zookeeper=False,
        use_job_registry=bool(get_backend_config().ejr_api),
    )
    app = build_app(backend_implementation=backend_implementation)

    show_log_level(logging.getLogger('openeo'))
    show_log_level(logging.getLogger('openeo_driver'))
    show_log_level(logging.getLogger('openeogeotrellis'))
    show_log_level(app.logger)

    host = os.environ.get("OPENEO_DEV_GUNICORN_HOST", "127.0.0.1")

    run_gunicorn(app, threads=4, host=host, port=8080, on_started=on_started)
