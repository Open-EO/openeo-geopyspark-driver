import json
import logging
import sys
import time
from typing import List

import openeogeotrellis
from kafka import KafkaProducer
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.configparams import ConfigParams
from py4j.java_gateway import JavaGateway

from openeogeotrellis.job_registry import JobRegistry
from pythonjsonlogger.jsonlogger import JsonFormatter

_log = logging.getLogger(__name__)

SENTINEL_HUB_BATCH_PROCESSES_POLL_INTERVAL_S = 60

TASK_DELETE_BATCH_PROCESS_RESULTS = 'delete_batch_process_results'
TASK_POLL_SENTINELHUB_BATCH_PROCESSES = 'poll_sentinelhub_batch_processes'


def schedule_delete_batch_process_results(batch_job_id: str, subfolders: List[str]):
    _schedule_task(task_id=TASK_DELETE_BATCH_PROCESS_RESULTS,
                   arguments={
                       'batch_job_id': batch_job_id,
                       'subfolders': subfolders
                   })


def schedule_poll_sentinelhub_batch_processes(batch_job_id: str, user_id: str):
    _schedule_task(task_id=TASK_POLL_SENTINELHUB_BATCH_PROCESSES,
                   arguments={
                       'batch_job_id': batch_job_id,
                       'user_id': user_id
                   })


def _schedule_task(task_id: str, arguments: dict):
    message = {
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
        producer.send(topic="openeo-async-tasks",
                      value=encode(json.dumps(message)),
                      headers=[('env', encode(env))] if env else None).get(timeout=120)
    finally:
        producer.close()


# TODO: DRY this, cleaner.sh and job_tracker.sh
def main():
    import argparse

    logging.basicConfig(level=logging.INFO)
    openeogeotrellis.backend.logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.formatter = JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z")

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    _log.info("argv: {a!r}".format(a=sys.argv))
    _log.info("ConfigParams(): {c}".format(c=ConfigParams()))

    try:
        parser = argparse.ArgumentParser(usage="OpenEO AsyncTask --task <task>",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("--py4j-jarpath", default="venv/share/py4j/py4j0.10.9.2.jar", help='Path to the Py4J jar')
        parser.add_argument("--py4j-classpath", default="geotrellis-extensions-2.2.0-SNAPSHOT.jar",
                            help='Classpath used to launch the Java Gateway')
        parser.add_argument("--task", required=True, dest="task_json", help="The task description in JSON")

        args = parser.parse_args()

        task = json.loads(args.task_json)
        task_id = task['task_id']
        if task_id not in [TASK_DELETE_BATCH_PROCESS_RESULTS, TASK_POLL_SENTINELHUB_BATCH_PROCESSES]:
            raise ValueError(f'unsupported task_id "{task_id}"')

        arguments = task.get('arguments', {})

        def batch_jobs() -> GpsBatchJobs:
            java_opts = [
                "-client",
                "-Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService"
            ]

            java_gateway = JavaGateway.launch_gateway(jarpath=args.py4j_jarpath,
                                                      classpath=args.py4j_classpath,
                                                      javaopts=java_opts,
                                                      die_on_exit=True)

            return GpsBatchJobs(catalog=None, jvm=java_gateway.jvm, principal=None, key_tab=None)

        if task_id == TASK_DELETE_BATCH_PROCESS_RESULTS:
            batch_job_id = arguments['batch_job_id']
            subfolders = arguments['subfolders']

            _log.info(f"removing subfolders {subfolders} for batch job {batch_job_id}...",
                      extra={'job_id': batch_job_id})
            batch_jobs().delete_batch_process_results(job_id=batch_job_id, subfolders=subfolders, propagate_errors=True)
        elif task_id == TASK_POLL_SENTINELHUB_BATCH_PROCESSES:
            batch_job_id = arguments['batch_job_id']
            user_id = arguments['user_id']

            while True:
                time.sleep(SENTINEL_HUB_BATCH_PROCESSES_POLL_INTERVAL_S)

                with JobRegistry() as registry:
                    job_info = registry.get_job(batch_job_id, user_id)

                if job_info.get('dependency_status') not in ['awaiting', "awaiting_retry"]:
                    break
                else:
                    batch_jobs().poll_sentinelhub_batch_processes(job_info)
        else:
            raise AssertionError(f'unexpected task_id "{task_id}"')
    except Exception as e:
        _log.error(e, exc_info=True)
        raise e


if __name__ == '__main__':
    main()
