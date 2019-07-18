"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
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
    'werkzeug': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    },
    'flask': {
        'level': 'DEBUG',
        'handlers': ['wsgi']
    }
})

import gunicorn.app.base

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
    conf.set('spark.driver.extraJavaOptions', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009')

    if 'TRAVIS' in os.environ:
        conf.set(key='spark.driver.memory', value='2G')
        conf.set(key='spark.executor.memory', value='2G')

    _log.info('Creating Spark context with config: {c!r}'.format(c=conf.getAll()))
    pysc = SparkContext.getOrCreate(conf)
    _log.info('Created Spark Context {s}'.format(s=pysc))


def number_of_workers():
    return 3#(multiprocessing.cpu_count() * 2) + 1


def when_ready(server):
    _log.info('When ready: {s}'.format(s=server))
    from pyspark import SparkContext

    sc = SparkContext.getOrCreate()

    principal = sc.getConf().get("spark.yarn.principal")
    keytab = sc.getConf().get("spark.yarn.keytab")
    from openeogeotrellis.job_tracker import JobTracker
    from openeogeotrellis.job_registry import JobRegistry

    logging.getLogger('gunicorn.error').info('Gunicorn info logging enabled!')
    logging.getLogger('flask').info('Flask info logging enabled!')

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

    setup_local_spark()

    # Modification 3: pass Flask app instead of handler_app
    options = {
        'bind': '%s:%s' % ("127.0.0.1", 8080),
        'workers': number_of_workers(),
        'worker_class':'gaiohttp',
        'timeout':1000,
        'loglevel': 'DEBUG',
        'accesslog':'-',
        'errorlog': '-'
    }

    from openeo_driver.views import app
    #app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel('DEBUG')
    application = StandaloneApplication(app, options)

    app.logger.info('App info logging enabled!')
    app.logger.debug('App debug logging enabled!')

    application.run()
    _log.info('application: {a}'.format(a=application))
