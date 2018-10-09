import os
import pandas as pd
import subprocess
from subprocess import CalledProcessError
import uuid
import json
import re

from typing import Dict,List

from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .GeotrellisCatalogImageCollection import GeotrellisCatalogImageCollection
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

def get_layers()->List:
    from pyspark import SparkContext
    print("starting spark context")
    pysc = SparkContext.getOrCreate()
    kerberos()
    return LayerCatalog().layers()

def get_layer(product_id)->Dict:
    from pyspark import SparkContext
    print("starting spark context")
    pysc = SparkContext.getOrCreate()
    kerberos()
    return LayerCatalog().layer(product_id)

def getImageCollection(product_id:str, viewingParameters):
    print("Creating layer for %s with viewingParameters %s" % (product_id, viewingParameters))
    kerberos()

    catalog = LayerCatalog()
    if product_id not in catalog.catalog:
        raise ValueError("Product id not available, list of available data can be retrieved at /data.")

    service_type = viewingParameters.get('service_type', '')

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
    if 'wmts' == service_type.lower():
        return GeotrellisCatalogImageCollection(product_id)
    else:
        return GeotrellisTimeSeriesImageCollection(gps.Pyramid(pyramid),catalog.catalog[product_id])


def get_batch_job_info(job_id: str) -> Dict:
    """Returns detailed information about a submitted batch job,
    or None if the batch job with this job_id is unknown."""
    from kazoo.exceptions import NoNodeError
    from .job_registry import JobRegistry
    try:
        with JobRegistry() as registry:
            status = registry.get_job(job_id)['status']

        return {
            'job_id': job_id,
            'status': status
        }
    except NoNodeError:
        return None


def get_batch_job_result_filenames(job_id: str) -> List[str]:
    job_info = get_batch_job_info(job_id)
    results_available = job_info and job_info.get('status') == 'finished'

    return ["out"] if results_available else None


def get_batch_job_result_output_dir(job_id: str) -> str:
    return "/mnt/ceph/Projects/OpenEO/%s" % job_id


def create_batch_job(specification: Dict) -> str:
    job_id = str(uuid.uuid4())

    from .job_registry import JobRegistry
    with JobRegistry() as registry:
        registry.register(job_id, specification)

    return job_id


def run_batch_job(job_id: str) -> None:
    from pyspark import SparkContext

    from .job_registry import JobRegistry
    with JobRegistry() as registry:
        job_info = registry.get_job(job_id)

        # FIXME: mark_undone in case of re-queue

        kerberos()

        output_dir = get_batch_job_result_output_dir(job_id)

        try:
            os.mkdir(output_dir)
        except FileExistsError:
            pass  # when i.e. this job's process graph was updated

        input_file = "%s/in" % output_dir
        output_file = "%s/out" % output_dir

        with open(input_file, 'w') as f:
            f.write(job_info['specification'])

        conf = SparkContext.getOrCreate().getConf()
        principal, key_tab = conf.get("spark.yarn.principal"), conf.get("spark.yarn.keytab")

        args = ["./submit_batch_job.sh", "OpenEO batch job %s" % job_id, input_file, output_file, principal, key_tab]

        batch_job = subprocess.Popen(args, stderr=subprocess.PIPE)

        # note: a job_id is returned as soon as an application ID is found in stderr, not when the job is finished
        application_id = _extract_application_id(batch_job.stderr)

        if application_id:
            print("mapped job_id %s to application ID %s" % (job_id, application_id))
        else:
            raise CalledProcessError(batch_job.wait(), batch_job.args)

        registry.update(job_id, application_id=application_id)


def _extract_application_id(stream) -> str:
    while True:
        line = stream.readline()

        if line:
            text = line.decode('utf8').strip()

            match = re.match(r".*Application report for (application_\d{13}_\d+)\s\(state:.*", text)
            if match:
                return match.group(1)
        else:
            return None
