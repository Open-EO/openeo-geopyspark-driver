import gunicorn.app.base
from gunicorn.six import iteritems

import json
import sys
import datetime

import threading


"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""


def setup_local_spark():
    from pyspark import find_spark_home
    import os, sys
    from glob import glob

    spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]
    if 'TRAVIS' in os.environ:
        master_str = "local[2]"
    else:
        master_str = "local[*]"

    from geopyspark import geopyspark_conf, Pyramid, TiledRasterLayer
    conf = geopyspark_conf(master=master_str, appName="test")
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    conf.set('spark.ui.enabled', True)
    conf.set('spark.driver.extraJavaOptions', '-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009')

    if 'TRAVIS' in os.environ:
        conf.set(key='spark.driver.memory', value='2G')
        conf.set(key='spark.executor.memory', value='2G')

    pysc = SparkContext.getOrCreate(conf)

def number_of_workers():
    return 3#(multiprocessing.cpu_count() * 2) + 1

def when_ready(server):
    print(server)

    sc = SparkContext.getOrCreate()

    principal = sc.getConf().get("spark.yarn.principal")
    keytab = sc.getConf().get("spark.yarn.keytab")
    from openeogeotrellis.job_tracker import JobTracker
    from openeogeotrellis.job_registry import JobRegistry

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


    from pyspark import SparkContext
    print("starting spark context")
    setup_local_spark()
    # Modification 3: pass Flask app instead of handler_app

    options = {
        'bind': '%s:%s' % ("127.0.0.1", 8080),
        'workers': number_of_workers(),
        'worker_class':'sync',
        'timeout':1000
    }
    from openeo_driver import app
    application = StandaloneApplication(app, options)


    application.run()
    print(application)

