import os
import pandas as pd
import subprocess
from subprocess import CalledProcessError
import uuid
import json
import re
from collections import deque
import traceback
import pkg_resources

from typing import Union, List

import pytz
from geopyspark import TiledRasterLayer, LayerType

from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .GeotrellisCatalogImageCollection import GeotrellisCatalogImageCollection
from .layercatalog import LayerCatalog
from .service_registry import *
from openeo.error_summary import *
from dateutil.parser import parse

from py4j.java_gateway import *
from py4j.protocol import Py4JJavaError
from ._version import __version__

import logging
logger = logging.getLogger("openeo")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s - THREAD: %(threadName)s - %(name)s] : %(message)s")

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_formatter)
logger.addHandler( log_stream_handler )

_service_registry = InMemoryServiceRegistry() if 'TRAVIS' in os.environ else ZooKeeperServiceRegistry()


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
    data_source_type = layer_config.get('data_source', {}).get('type', 'Accumulo')

    service_type = viewingParameters.get('service_type', '')

    import geopyspark as gps
    from_date = normalize_date(viewingParameters.get("from",None))
    to_date = normalize_date(viewingParameters.get("to",None))

    left = viewingParameters.get("left",None)
    right = viewingParameters.get("right",None)
    top = viewingParameters.get("top",None)
    bottom = viewingParameters.get("bottom",None)
    srs = viewingParameters.get("srs",None)
    band_indices = viewingParameters.get("bands")
    pysc = gps.get_spark_context()
    extent = None

    gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
    jvm = gateway.jvm
    if(left is not None and right is not None and top is not None and bottom is not None):
        extent = jvm.geotrellis.vector.Extent(float(left), float(bottom), float(right), float(top))

    def accumulo_pyramid():
        pyramidFactory = jvm.org.openeo.geotrellisaccumulo.PyramidFactory("hdp-accumulo-instance",
                                                                                "epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181")
        accumulo_layer_name = layer_config['data_id']
        return pyramidFactory.pyramid_seq(accumulo_layer_name, extent,srs, from_date, to_date)

    def s3_pyramid():
        endpoint = layer_config['data_source']['endpoint']
        region = layer_config['data_source']['region']
        bucket_name = layer_config['data_source']['bucket_name']

        return jvm.org.openeo.geotrelliss3.PyramidFactory(endpoint, region, bucket_name) \
            .pyramid_seq(extent, srs, from_date, to_date)

    def file_pyramid():
        return jvm.org.openeo.geotrellis.file.Sentinel2RadiometryPyramidFactory() \
            .pyramid_seq(extent, srs, from_date, to_date, band_indices)

    if data_source_type.lower() == 's3':
        pyramid = s3_pyramid()
    elif data_source_type.lower() == 'file':
        pyramid = file_pyramid()
    else:
        pyramid = accumulo_pyramid()

    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    option = jvm.scala.Option
    levels = {pyramid.apply(index)._1():TiledRasterLayer(LayerType.SPACETIME,temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()),pyramid.apply(index)._2())) for index in range(0,pyramid.size())}

    image_collection = GeotrellisTimeSeriesImageCollection(gps.Pyramid(levels), _service_registry, catalog.catalog[product_id])
    return image_collection.band_filter(band_indices) if band_indices else image_collection

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


def create_batch_job(api_version: str, specification: Dict) -> str:
    job_id = str(uuid.uuid4())

    from .job_registry import JobRegistry
    with JobRegistry() as registry:
        registry.register(job_id, api_version, specification)

    return job_id


class _BatchJobError(Exception):
    def __init__(self, message):
        super().__init__(message)


def run_batch_job(job_id: str) -> None:
    from pyspark import SparkContext

    from .job_registry import JobRegistry
    with JobRegistry() as registry:
        job_info = registry.get_job(job_id)
        api_version = job_info.get('api_version')

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

        script_location = pkg_resources.resource_filename('openeogeotrellis.deploy', 'submit_batch_job.sh')

        args = [script_location, "OpenEO batch job %s" % job_id, input_file, output_file]
        if principal is not None and key_tab is not None:
            args.append(principal)
            args.append(key_tab)
        else:
            args.append("no_principal")
            args.append("no_keytab")
        if api_version:
            args.append(api_version)

        try:
            output_string = subprocess.check_output(args, stderr=subprocess.STDOUT,universal_newlines=True)
        except CalledProcessError as e:
            logger.exception(e)
            logger.error(e.stdout)
            logger.error(e.stderr)
            raise e


        try:
            # note: a job_id is returned as soon as an application ID is found in stderr, not when the job is finished
            logger.info(output_string)
            application_id = _extract_application_id(output_string)
            print("mapped job_id %s to application ID %s" % (job_id, application_id))

            registry.update(job_id, application_id=application_id)
        except _BatchJobError as e:
            traceback.print_exc(file=sys.stderr)
            raise CalledProcessError(1,str(args),output=output_string)


def _extract_application_id(stream) -> str:
    match = re.match(r".*Application report for (application_\d{13}_\d+)\s\(state:.*", stream)
    if match:
        return match.group(1)
    else:
        raise _BatchJobError(stream)


def cancel_batch_job(job_id: str):
    from .job_registry import JobRegistry

    with JobRegistry() as registry:
        application_id = registry.get_job(job_id)['application_id']

    subprocess.call(["yarn", "application", "-kill", application_id])


def get_secondary_services_info() -> List[Dict]:
    return [_merge(details['specification'], 'service_id', service_id) for service_id, details in _service_registry.get_all().items()]


def get_secondary_service_info(service_id: str) -> Dict:
    details = _service_registry.get(service_id)
    return _merge(details['specification'], 'service_id', service_id)


def _merge(original: Dict, key, value) -> Dict:
    copy = dict(original)
    copy[key] = value
    return copy


def summarize_exception(error: Exception) -> Union[ErrorSummary, Exception]:
    if isinstance(error, Py4JJavaError):
        java_exception = error.java_exception
        while(java_exception.getCause() != None and java_exception != java_exception.getCause()):
            java_exception = java_exception.getCause()
        java_exception_class_name = java_exception.getClass().getName()
        is_client_error = java_exception_class_name == 'java.lang.IllegalArgumentException'

        return ErrorSummary(error, is_client_error, summary=java_exception.getMessage())

    return error
