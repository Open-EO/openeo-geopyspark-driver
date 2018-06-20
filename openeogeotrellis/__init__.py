import os
import pandas as pd

from typing import Dict
from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .layercatalog import LayerCatalog


def health_check():
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()
    count = sc.parallelize([1,2,3]).count()
    return 'Health check: ' + str(count)



def kerberos():
    import geopyspark as gps

    sc = gps.get_spark_context()
    jvm = sc._gateway.jvm
    hadoopconf = jvm.org.apache.hadoop.conf.Configuration()
    hadoopconf.set("hadoop.security.authentication", "kerberos")
    #jvm.org.apache.hadoop.security.UserGroupInformation.setConfiguration(hadoopconf);
    #jvm.org.apache.hadoop.security.UserGroupInformation.loginUserFromSubject(None)
    currentUser = jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
    print(currentUser.toString())
    print(jvm.org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
    #print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getAuthenticationMethod().toString())

    principal = sc.getConf().get("spark.yarn.principal")
    sparkKeytab = sc.getConf().get("spark.yarn.keytab")
    if principal is not None and sparkKeytab is not None:
        jvm.org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principal,sparkKeytab)
        jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().setAuthenticationMethod(jvm.org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS);
    #print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().toString())
    #loginUser = jvm.org.apache.hadoop.security.UserGroupInformation.getLoginUser()
    #print(loginUser.toString())
    #print(loginUser.hasKerberosCredentials())
    #currentUser.addCredentials(loginUser.getCredentials())
    #print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().hasKerberosCredentials())

def get_layers()->Dict:
    from pyspark import SparkContext
    print("starting spark context")
    pysc = SparkContext.getOrCreate()
    kerberos()
    return LayerCatalog().layers()

def getImageCollection(product_id:str, viewingParameters):
    print("Creating layer for %s with viewingParameters %s" % (product_id, viewingParameters))
    kerberos()
    import geopyspark as gps
    from_date = viewingParameters.get("from",None)
    to_date = viewingParameters.get("to",None)
    time_intervals = None


    left = viewingParameters.get("left",None)
    right = viewingParameters.get("right",None)
    top = viewingParameters.get("top",None)
    bottom = viewingParameters.get("bottom",None)
    srs = viewingParameters.get("srs",None)
    bbox = None
    if(left is not None and right is not None and top is not None and bottom is not None):
        bbox = gps.Extent(left,bottom,right,top)

    store = gps.AttributeStore("accumulo+kerberos://epod6.vgt.vito.be:2181/hdp-accumulo-instance")
    zoomlevels = [layer.layer_zoom for layer in store.layers() if layer.layer_name == product_id]
    pyramid = {}
    for level in zoomlevels:
        if from_date is not None and to_date is not None:
            #time_intervals is changed in-place to a str by geopyspark
            time_intervals = [pd.to_datetime(from_date),pd.to_datetime(to_date)]
        tiledrasterlayer = gps.query(uri="accumulo+kerberos://epod6.vgt.vito.be:2181/hdp-accumulo-instance", layer_name=product_id,
                      layer_zoom=level, query_geom=bbox, query_proj=srs, time_intervals=time_intervals,num_partitions=20)
        pyramid[level] = tiledrasterlayer
    return GeotrellisTimeSeriesImageCollection(gps.Pyramid(pyramid))


