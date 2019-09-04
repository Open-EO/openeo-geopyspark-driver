"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
from logging.config import dictConfig

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
        'openeo_driver': {'level': 'DEBUG'},
        'openeogeotrellis': {'level': 'DEBUG'},
        'kazoo': {'level': 'WARN'},
    }
})

import os
import sys
import threading
from glob import glob

import gunicorn.app.base
from gunicorn.six import iteritems

_log = logging.getLogger('openeo-geotrellis-local')


def setup_local_spark():
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
    conf = geopyspark_conf(master=master_str, appName="openeo-geotrellis-local")
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    conf.set('spark.ui.enabled', True)
    # Some options to allow attaching a Java debugger to running Spark driver
    conf.set('spark.driver.extraJavaOptions', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009')

    if 'TRAVIS' in os.environ:
        conf.set(key='spark.driver.memory', value='2G')
        conf.set(key='spark.executor.memory', value='2G')

    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable

    _log.info('Creating Spark context with config: {c!r}'.format(c=conf.getAll()))
    pysc = SparkContext.getOrCreate(conf)
    _log.info('Created Spark Context {s}'.format(s=pysc))


def number_of_workers():
    return 3#(multiprocessing.cpu_count() * 2) + 1


def show_log_level(logger: logging.Logger):
    """Helper to show threshold log level of a logger."""
    level = logger.getEffectiveLevel()
    logger.log(level, 'Logger {n!r} level: {t}'.format(n=logger.name, t=logging.getLevelName(level)))


def when_ready(server):
    _log.info('When ready: {s}'.format(s=server))
    from pyspark import SparkContext

    sc = SparkContext.getOrCreate()

    principal = sc.getConf().get("spark.yarn.principal")
    keytab = sc.getConf().get("spark.yarn.keytab")
    from openeogeotrellis.job_tracker import JobTracker
    from openeogeotrellis.job_registry import JobRegistry

    show_log_level(logging.getLogger('gunicorn.error'))
    show_log_level(logging.getLogger('flask'))
    show_log_level(logging.getLogger('werkzeug'))

    job_tracker = JobTracker(JobRegistry, principal, keytab)
    threading.Thread(target=job_tracker.update_statuses, daemon=True).start()


class StandaloneApplication(gunicorn.app.base.BaseApplication):

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super(StandaloneApplication, self).__init__()

    def load_config(self):
        config = dict([(key, value) for key, value in iteritems(self.options)
                       if key in self.cfg.settings and value is not None])
        config['when_ready'] = when_ready
        for key, value in iteritems(config):
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


if __name__ == '__main__':
    _log.info(repr({"pid": os.getpid(), "interpreter": sys.executable, "version": sys.version, "argv": sys.argv}))

    setup_local_spark()

    # Modification 3: pass Flask app instead of handler_app
    options = {
        'bind': '%s:%s' % ("127.0.0.1", 8080),
        'workers': number_of_workers(),
        'worker_class':'gthread',   #'gaiohttp',
        'timeout':1000,
        'loglevel': 'DEBUG',
        'accesslog':'-',
        'errorlog': '-'#,
        #'certfile': 'test.pem',
        #'keyfile': 'test.key'
    }

    from openeo_driver.views import app
    from flask_cors import CORS
    CORS(app)
    from openeogeotrellis import get_backend_version

    show_log_level(logging.getLogger('openeo'))
    show_log_level(logging.getLogger('openeo_driver'))
    show_log_level(logging.getLogger('openeogeotrellis'))
    show_log_level(app.logger)

    app.config['OPENEO_BACKEND_VERSION'] = get_backend_version()
    app.config['OPENEO_TITLE'] = 'Local GeoPySpark'
    app.config['OPENEO_DESCRIPTION'] = 'Local openEO API using GeoPySpark driver'
    application = StandaloneApplication(app, options)

    application.run()
