from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .testlayers import TestLayers

def getImageCollection(product_id:str, viewingParameters):
    #TODO create real rdd from accumulo
    print("Creating layer for: "+product_id)

    return GeotrellisTimeSeriesImageCollection(TestLayers().create_spacetime_layer())

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

