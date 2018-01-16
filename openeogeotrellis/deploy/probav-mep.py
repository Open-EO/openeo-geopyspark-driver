import gunicorn.app.base
from gunicorn.six import iteritems
from kazoo.client import KazooClient
import json
import uuid
import datetime
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




def service_started(host:str, port):
    print("Registering with zookeeper.")
    zk = KazooClient(hosts='epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181')
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



if __name__ == '__main__':


    from pyspark import SparkContext
    print("starting spark context")
    pysc = SparkContext.getOrCreate()
    # Modification 3: pass Flask app instead of handler_app
    import socket
    local_ip = socket.gethostbyname(socket.gethostname())
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    host, port = tcp.getsockname()

    options = {
        'bind': '%s:%s' % (local_ip, port),
        'workers': number_of_workers(),
    }
    tcp.close()
    application = StandaloneApplication(app, options)
    service_started(local_ip,port)
    application.run()
    print(application)

