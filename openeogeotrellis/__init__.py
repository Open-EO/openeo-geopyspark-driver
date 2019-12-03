import logging
import os
import re
import subprocess
import sys
import traceback
import uuid
from subprocess import CalledProcessError
from typing import Union, List

import pkg_resources
from py4j.protocol import Py4JJavaError

from openeo.error_summary import ErrorSummary
from openeogeotrellis.GeotrellisCatalogImageCollection import GeotrellisCatalogImageCollection
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis._version import __version__
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.utils import kerberos
from openeogeotrellis.errors import SpatialBoundsMissingException
from openeo_driver.errors import JobNotFoundException

logger = logging.getLogger("openeo")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s - THREAD: %(threadName)s - %(name)s] : %(message)s")

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_formatter)
logger.addHandler( log_stream_handler )


def get_backend_version() -> str:
    return __version__


def create_process_visitor():
    from .geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
    return GeotrellisTileProcessGraphVisitor()


def get_batch_job_info(job_id: str, user_id: str) -> dict:
    """Returns detailed information about a submitted batch job,
    or None if the batch job with this job_id is unknown."""
    from .job_registry import JobRegistry
    try:
        with JobRegistry() as registry:
            status = registry.get_job(job_id, user_id)['status']

        return {
            'job_id': job_id,
            'status': status
        }
    except JobNotFoundException:
        return None


def get_batch_jobs_info(user_id: str) -> List[dict]:
    from .job_registry import JobRegistry

    with JobRegistry() as registry:
        return [{
            'job_id': job_info['job_id'],
            'status': job_info['status']
        } for job_info in registry.get_user_jobs(user_id)]


def get_batch_job_result_filenames(job_id: str, user_id: str) -> List[str]:
    job_info = get_batch_job_info(job_id, user_id)
    results_available = job_info and job_info.get('status') == 'finished'

    return ["out"] if results_available else None


def get_batch_job_result_output_dir(job_id: str) -> str:
    return "/data/projects/OpenEO/%s" % job_id


def create_batch_job(user_id: str, api_version: str, specification: dict) -> str:
    job_id = str(uuid.uuid4())

    from .job_registry import JobRegistry
    with JobRegistry() as registry:
        registry.register(job_id, user_id, api_version, specification)

    return job_id


class _BatchJobError(Exception):
    def __init__(self, message):
        super().__init__(message)


def run_batch_job(job_id: str, user_id: str) -> None:
    from pyspark import SparkContext

    from .job_registry import JobRegistry
    with JobRegistry() as registry:
        job_info = registry.get_job(job_id, user_id)
        api_version = job_info.get('api_version')

        extra_options = job_info.get('job_options', {})

        driver_memory = extra_options.get("driver-memory", "22G")
        executor_memory = extra_options.get("executor-memory", "5G")
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
        else:
            args.append("0.4.0")

        args.append(driver_memory)
        args.append(executor_memory)

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

            registry.set_application_id(job_id, user_id, application_id)
        except _BatchJobError as e:
            traceback.print_exc(file=sys.stderr)
            raise CalledProcessError(1,str(args),output=output_string)


def _extract_application_id(stream) -> str:
    match = re.compile(r"^.*Application report for (application_\d{13}_\d+)\s\(state:.*", re.MULTILINE).search(stream)
    if match:
        return match.group(1)
    else:
        raise _BatchJobError(stream)


def cancel_batch_job(job_id: str, user_id: str):
    from .job_registry import JobRegistry

    with JobRegistry() as registry:
        application_id = registry.get_job(job_id, user_id)['application_id']

    subprocess.call(["yarn", "application", "-kill", application_id])


def summarize_exception(error: Exception) -> Union[ErrorSummary, Exception]:
    if isinstance(error, Py4JJavaError):
        java_exception = error.java_exception

        while java_exception.getCause() is not None and java_exception != java_exception.getCause():
            java_exception = java_exception.getCause()

        java_exception_class_name = java_exception.getClass().getName()
        java_exception_message = java_exception.getMessage()

        no_data_found = (java_exception_class_name == 'java.lang.AssertionError'
                         and "Cannot stitch empty collection" in java_exception_message)

        is_client_error = java_exception_class_name == 'java.lang.IllegalArgumentException' or no_data_found
        summary = "Cannot construct an image because the given boundaries resulted in an empty image collection" if no_data_found else java_exception_message

        return ErrorSummary(error, is_client_error, summary)

    if isinstance(error, SpatialBoundsMissingException):
        return ErrorSummary(error, is_client_error=True, summary="The process graph is too complex for for synchronous processing. Please use a batch job instead.")

    return error


# Late import to avoid circular dependency issues.
# TODO avoid this. Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/12
from openeogeotrellis.backend import get_openeo_backend_implementation

