import os
import pandas as pd
import subprocess
from subprocess import CalledProcessError
import uuid
import json
import re

from typing import Dict,List

import pytz
from geopyspark import TiledRasterLayer, LayerType

from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .GeotrellisCatalogImageCollection import GeotrellisCatalogImageCollection
from .layercatalog import LayerCatalog
from dateutil.parser import parse

from py4j.java_gateway import *

import logging
logger = logging.getLogger("openeo")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s - THREAD: %(threadName)s - %(name)s] : %(message)s")

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_formatter)
logger.addHandler( log_stream_handler )


def health_check():
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()
    count = sc.parallelize([1,2,3]).count()
    return 'Health check: ' + str(count)



def kerberos():
    import geopyspark as gps

    sc = gps.get_spark_context()
    gateway = JavaGateway(gateway_parameters=sc._gateway.gateway_parameters)
    jvm = gateway.jvm
    currentUser = jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
    if currentUser.hasKerberosCredentials():
        return
    logger.info(currentUser.toString())
    logger.info(jvm.org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled())
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
    kerberos()
    return LayerCatalog().layers()

def get_layer(product_id)->Dict:
    from pyspark import SparkContext
    kerberos()
    return LayerCatalog().layer(product_id)

def normalize_date(date_string):
    if (date_string is not None):
        date = parse(date_string)
        if date.tzinfo is None:
            date = date.replace(tzinfo=pytz.UTC)
        return date.isoformat()
    return None


def getImageCollection(product_id:str, viewingParameters):
    print("Creating layer for %s with viewingParameters %s" % (product_id, viewingParameters))
    kerberos()

    catalog = LayerCatalog()
    if product_id not in catalog.catalog:
        raise ValueError("Product id not available, list of available data can be retrieved at /data.")
    layer_config = catalog.layer(product_id)
    internal_layer_name = layer_config.data_id

    service_type = viewingParameters.get('service_type', '')

    import geopyspark as gps
    from_date = normalize_date(viewingParameters.get("from",None))
    to_date = normalize_date(viewingParameters.get("to",None))

    left = viewingParameters.get("left",None)
    right = viewingParameters.get("right",None)
    top = viewingParameters.get("top",None)
    bottom = viewingParameters.get("bottom",None)
    srs = viewingParameters.get("srs",None)
    pysc = gps.get_spark_context()
    extent = None

    gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
    jvm = gateway.jvm
    if(left is not None and right is not None and top is not None and bottom is not None):
        extent = jvm.geotrellis.vector.Extent(float(left), float(bottom), float(right), float(top))

    pyramidFactory = jvm.org.openeo.geotrellisaccumulo.PyramidFactory("hdp-accumulo-instance",
                                                                            "epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181")

    pyramid = pyramidFactory.pyramid_seq(internal_layer_name, extent,srs, from_date, to_date)
    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    option = jvm.scala.Option
    levels = {pyramid.apply(index)._1():TiledRasterLayer(LayerType.SPACETIME,temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()),pyramid.apply(index)._2())) for index in range(0,pyramid.size())}
    return GeotrellisTimeSeriesImageCollection(gps.Pyramid(levels), catalog.catalog[product_id])

def create_process_visitor():
    from .geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
    return GeotrellisTileProcessGraphVisitor()


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
