import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import time
import traceback
from typing import List

import kazoo.client
import openeogeotrellis
from kafka import KafkaProducer
from openeo_driver.util.logging import JSON_LOGGER_DEFAULT_FORMAT
from openeogeotrellis import sentinel_hub
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.layercatalog import get_layer_catalog
from py4j.java_gateway import JavaGateway

from openeogeotrellis.job_registry import JobRegistry
from pythonjsonlogger.jsonlogger import JsonFormatter

# TODO: include job_id in log statements not issued by our own code e.g. Py4J  # 141
_log = logging.getLogger(__name__)

SENTINEL_HUB_BATCH_PROCESSES_POLL_INTERVAL_S = 60

TASK_DELETE_BATCH_PROCESS_DEPENDENCY_SOURCES = 'delete_batch_process_dependency_sources'
TASK_POLL_SENTINELHUB_BATCH_PROCESSES = 'poll_sentinelhub_batch_processes'


def schedule_delete_batch_process_dependency_sources(batch_job_id: str, user_id: str, dependency_sources: List[str]):
    _schedule_task(task_id=TASK_DELETE_BATCH_PROCESS_DEPENDENCY_SOURCES,
                   arguments={
                       'batch_job_id': batch_job_id,
                       'user_id': user_id,
                       'dependency_sources': dependency_sources
                   })


def schedule_poll_sentinelhub_batch_processes(batch_job_id: str, user_id: str):
    _schedule_task(task_id=TASK_POLL_SENTINELHUB_BATCH_PROCESSES,
                   arguments={
                       'batch_job_id': batch_job_id,
                       'user_id': user_id
                   })


def _schedule_task(task_id: str, arguments: dict):
    task = {
        'task_id': task_id,
        'arguments': arguments
    }

    env = ConfigParams().async_task_handler_environment

    def encode(s: str) -> bytes:
        return s.encode('utf-8')

    producer = KafkaProducer(
        bootstrap_servers="epod-master1.vgt.vito.be:6668,epod-master2.vgt.vito.be:6668,epod-master3.vgt.vito.be:6668",
        security_protocol='PLAINTEXT',
        acks='all'
    )

    try:
        task_message = json.dumps(task)
        producer.send(topic="openeo-async-tasks",
                      value=encode(task_message),
                      headers=[('env', encode(env))] if env else None).get(timeout=120)

        _log.info(f"scheduled task {task_message} on env {env}")
    finally:
        producer.close()


# TODO: DRY this, cleaner.sh and job_tracker.sh
def main():
    import argparse

    logging.basicConfig(level=logging.INFO)
    openeogeotrellis.backend.logger.setLevel(logging.DEBUG)
    kazoo.client.log.setLevel(logging.WARNING)

    # Note: The Java logging is also supposed to match.
    json_formatter = JsonFormatter(JSON_LOGGER_DEFAULT_FORMAT)

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.formatter = json_formatter

    rolling_file_handler = RotatingFileHandler("logs/async_task_python.log", maxBytes=10 * 1024 * 1024, backupCount=1)
    rolling_file_handler.formatter = json_formatter

    root_logger = logging.getLogger()
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(rolling_file_handler)

    _log.info("argv: {a!r}".format(a=sys.argv))
    _log.info("ConfigParams(): {c}".format(c=ConfigParams()))

    try:
        parser = argparse.ArgumentParser(usage="OpenEO AsyncTask --task <task>",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("--py4j-jarpath", default="venv/share/py4j/py4j0.10.7.jar", help='Path to the Py4J jar')
        parser.add_argument("--py4j-classpath", default="geotrellis-extensions-2.2.0-SNAPSHOT.jar",
                            help='Classpath used to launch the Java Gateway')
        parser.add_argument("--py4j-maximum-heap-size", default="1G",
                            help='Maximum heap size for the Java Gateway JVM')
        parser.add_argument("--principal", default="openeo@VGT.VITO.BE", help="Principal to be used to login to KDC")
        parser.add_argument("--keytab", default="openeo-deploy/mep/openeo.keytab",
                            help="The full path to the file that contains the keytab for the principal")
        parser.add_argument("--task", required=True, dest="task_json", help="The task description in JSON")

        args = parser.parse_args()

        task = json.loads(args.task_json)
        task_id = task['task_id']
        if task_id not in [TASK_POLL_SENTINELHUB_BATCH_PROCESSES, TASK_DELETE_BATCH_PROCESS_DEPENDENCY_SOURCES]:
            raise ValueError(f'unsupported task_id "{task_id}"')

        arguments: dict = task.get('arguments', {})

        def get_batch_jobs(batch_job_id: str, user_id: str) -> GpsBatchJobs:
            os.environ['OPENEO_BATCH_JOB_ID'] = batch_job_id
            os.environ['OPENEO_USER_ID'] = user_id

            java_opts = [
                "-client",
                f"-Xmx{args.py4j_maximum_heap_size}",
                "-Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService",
                "-Dlog4j.configuration=file:async_task_log4j.properties"
            ]

            java_gateway = JavaGateway.launch_gateway(jarpath=args.py4j_jarpath,
                                                      classpath=args.py4j_classpath,
                                                      javaopts=java_opts,
                                                      die_on_exit=True,
                                                      redirect_stdout=sys.stdout,
                                                      redirect_stderr=sys.stderr)

            return GpsBatchJobs(get_layer_catalog(opensearch_enrich=True), java_gateway.jvm, args.principal,
                                args.keytab)

        if task_id == TASK_DELETE_BATCH_PROCESS_DEPENDENCY_SOURCES:
            batch_job_id = arguments['batch_job_id']
            user_id = arguments.get('user_id')
            dependency_sources = (arguments.get('dependency_sources')
                                  or [f"s3://{sentinel_hub.OG_BATCH_RESULTS_BUCKET}/{subfolder}"
                                      for subfolder in arguments['subfolders']])

            _log.info(f"removing dependency sources {dependency_sources} for batch job {batch_job_id}...",
                      extra={'job_id': batch_job_id})

            batch_jobs = get_batch_jobs(batch_job_id, user_id)
            batch_jobs.delete_batch_process_dependency_sources(
                job_id=batch_job_id,
                dependency_sources=dependency_sources,
                propagate_errors=True)
        elif task_id == TASK_POLL_SENTINELHUB_BATCH_PROCESSES:
            batch_job_id = arguments['batch_job_id']
            user_id = arguments['user_id']

            batch_jobs = get_batch_jobs(batch_job_id, user_id)

            while True:
                time.sleep(SENTINEL_HUB_BATCH_PROCESSES_POLL_INTERVAL_S)

                with JobRegistry() as registry:
                    job_info = registry.get_job(batch_job_id, user_id)

                if job_info.get('dependency_status') not in ['awaiting', "awaiting_retry"]:
                    break
                else:
                    try:
                        batch_jobs.poll_sentinelhub_batch_processes(job_info)
                    except Exception:
                        # TODO: retry in Nifi? How to mark this job as 'error' then?
                        # TODO: don't put the stack trace in the message but add exc_info  # 141
                        _log.error("failed to handle polling batch processes for batch job {j}:\n{e}"
                                   .format(j=batch_job_id, e=traceback.format_exc()),
                                   extra={'job_id': batch_job_id})

                        with JobRegistry() as registry:
                            registry.set_status(batch_job_id, user_id, 'error')
                            registry.mark_done(batch_job_id, user_id)

                        raise  # TODO: this will get caught by the exception handler below which will just log it again  # 141

        else:
            raise AssertionError(f'unexpected task_id "{task_id}"')
    except Exception as e:
        _log.error(e, exc_info=True)  # TODO: add a more descriptive message instead of the exception itself  # 141
        raise e


if __name__ == '__main__':
    main()
