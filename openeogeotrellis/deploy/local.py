"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
import os
import sys
from glob import glob
from logging.config import dictConfig

from openeo_driver.server import show_log_level, run_gunicorn
from openeo_driver.views import build_app
from openeogeotrellis.deploy import flask_config

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(process)s %(levelname)s in %(name)s: %(message)s',
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
        'openeo_driver': {'level': 'DEBUG'},
        'openeogeotrellis': {'level': 'DEBUG'},
        'kazoo': {'level': 'WARN'},
    }
})

_log = logging.getLogger(__name__)


def setup_local_spark(additional_jar_dirs=[]):
    from pyspark import find_spark_home, SparkContext

    spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]
    _log.debug('sys.path: {p!r}'.format(p=sys.path))
    if 'TRAVIS' in os.environ:
        master_str = "local[2]"
    else:
        master_str = "local[*]"

    from geopyspark import geopyspark_conf
    conf = geopyspark_conf(master=master_str, appName="openeo-geotrellis-local", additional_jar_dirs=additional_jar_dirs)
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    conf.set('spark.ui.enabled', True)
    # Some options to allow attaching a Java debugger to running Spark driver
    conf.set('spark.driver.extraJavaOptions', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009')

    if 'TRAVIS' in os.environ:
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
    _log.info(repr({"pid": os.getpid(), "interpreter": sys.executable, "version": sys.version, "argv": sys.argv}))

    setup_local_spark()

    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    app = build_app(backend_implementation=GeoPySparkBackendImplementation())
    app.config.from_object(flask_config)
    app.config.from_mapping(
        OPENEO_TITLE="Local GeoPySpark",
        OPENEO_DESCRIPTION="Local openEO API using GeoPySpark driver",
    )

    show_log_level(logging.getLogger('openeo'))
    show_log_level(logging.getLogger('openeo_driver'))
    show_log_level(logging.getLogger('openeogeotrellis'))
    show_log_level(app.logger)

    run_gunicorn(
        app,
        threads=4,
        host="127.0.0.1",
        port=8080,
        on_started=on_started
    )
