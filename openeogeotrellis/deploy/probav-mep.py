import gunicorn.app.base
from gunicorn.six import iteritems

from openeo_driver import app

"""
Script to start a production server. This script can serve as the entry-point for doing spark-submit.
"""

def number_of_workers():
    return 4#(multiprocessing.cpu_count() * 2) + 1

def when_ready(server):
    print(server)

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
    options = {
        'bind': '%s:%s' % ('127.0.0.1', '0'),
        'workers': number_of_workers(),
    }

    from pyspark import SparkContext
    print("starting spark context")
    pysc = SparkContext.getOrCreate()
    print("context created: " + pysc)
    # Modification 3: pass Flask app instead of handler_app
    application = StandaloneApplication(app, options)

    application.run()
    print(application)



def service_started(url:str):
    from kazoo.client import KazooClient
    import json
    import uuid
    import datetime
    zk = KazooClient(hosts='epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181')
    zk.ensure_path("discovery/services/openeo-test")
    id = uuid.uuid4()
    print(id)
    zk.ensure_path("discovery/services/openeo-test/"+str(id))
    zk.set("discovery/services/openeo-test/"+str(id),json.dumps({"name":"openeo-test","id":str(id),"address":"epodX.vgt.vito.be","port":123,"sslPort":None,"payload":None,"registrationTimeUTC":datetime.datetime.utcnow().strftime('%s'),"serviceType":"DYNAMIC"}))

