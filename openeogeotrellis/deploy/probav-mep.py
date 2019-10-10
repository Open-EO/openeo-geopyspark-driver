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
    }
})


import gunicorn.app.base
from gunicorn.six import iteritems
import sys
sys.path.insert(0,'py4j-0.10.7-src.zip')
sys.path.insert(0,'pyspark.zip')
import json
import sys
import datetime
import threading
from openeogeotrellis.job_tracker import JobTracker
from openeogeotrellis.job_registry import JobRegistry



"""
Script to start a production server. This script can serve as the entry-point for doing spark-submit.
"""

def number_of_workers():
    return 4#(multiprocessing.cpu_count() * 2) + 1

def when_ready(server):
    print(server)
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()

    principal = sc.getConf().get("spark.yarn.principal")
    keytab = sc.getConf().get("spark.yarn.keytab")

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


def update_zookeeper(host: str, port):
    print("Registering with zookeeper.")
    from kazoo.client import KazooClient
    from openeogeotrellis.configparams import ConfigParams
    zk = KazooClient(hosts=','.join(ConfigParams().zookeepernodes))
    zk.start()
    zk.ensure_path("discovery/services/openeo-test")
    #id = uuid.uuid4()
    #print(id)
    id = 0
    zk.ensure_path("discovery/services/openeo-test/"+str(id))
    zk.set("discovery/services/openeo-test/"+str(id),str.encode(json.dumps({"name":"openeo-test","id":str(id),"address":host,"port":port,"sslPort":None,"payload":None,"registrationTimeUTC":datetime.datetime.utcnow().strftime('%s'),"serviceType":"DYNAMIC"})))
    zk.stop()
    zk.close()
    print("Zookeeper node created: discovery/services/openeo-test/"+str(id))

def main():
    from pyspark import SparkContext
    print("starting spark context")
    pysc = SparkContext.getOrCreate()
    # Modification 3: pass Flask app instead of handler_app
    import socket
    local_ip = socket.gethostbyname(socket.gethostname())
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    host, port = tcp.getsockname()

    #note the use of 1 worker and multiple threads
    # we were seeing strange py4j errors when executing multiple requests in parallel
    # this seems to be related by the type and configuration of worker that gunicorn uses, aiohttp also gave very bad results
    options = {
        'bind': '%s:%s' % (local_ip, port),
        'workers': 1,
        'threads': 10,
        'worker_class': 'gthread',
        'timeout': 1000,
        'loglevel': 'DEBUG',
        'accesslog': '-',
        'errorlog': '-'
    }
    tcp.close()
    from openeo_driver.views import app
    from flask_cors import CORS
    CORS(app)
    from openeogeotrellis import get_backend_version

    app.logger.setLevel('DEBUG')
    app.config['OPENEO_BACKEND_VERSION'] = get_backend_version()
    app.config['OPENEO_TITLE'] = 'VITO Remote Sensing openEO API'
    app.config['OPENEO_DESCRIPTION'] = 'OpenEO API to the VITO Remote Sensing product catalog and processing services (using GeoPySpark driver).'
    application = StandaloneApplication(app, options)

    app.logger.info('App info logging enabled!')
    app.logger.debug('App debug logging enabled!')

    zookeeper = len(sys.argv) <= 1 or sys.argv[1] != "no-zookeeper"

    if zookeeper:
        update_zookeeper(local_ip, port)

    application.run()
    print(application)


if __name__ == '__main__':
    main()

