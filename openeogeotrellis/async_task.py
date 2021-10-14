import argparse
import json
import logging
from typing import List

import openeogeotrellis
import requests
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.configparams import ConfigParams
from py4j.java_gateway import JavaGateway

logging.basicConfig(level=logging.INFO)
openeogeotrellis.backend.logger.setLevel(logging.DEBUG)

_log = logging.getLogger(__name__)

ASYNC_TASK_ENDPOINT = "http://127.0.0.1:7180/asynctask"


def schedule_delete_batch_process_results(batch_job_id: str, subfolders: List[str]):
    _schedule_task(
        task_id="delete_batch_process_results",
        arguments={
            "batch_job_id": batch_job_id,
            "subfolders": subfolders
        })


def _schedule_task(task_id: str, arguments: dict):
    resp = requests.post(url=ASYNC_TASK_ENDPOINT, json={
        "task_id": task_id,
        "arguments": arguments
    })

    resp.raise_for_status()


# TODO: DRY this, cleaner.sh and job_tracker.sh
def main():
    _log.info("ConfigParams(): {c}".format(c=ConfigParams()))

    parser = argparse.ArgumentParser(usage="OpenEO AsyncTask --task <task>",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--py4j-jarpath", default="venv/share/py4j/py4j0.10.9.2.jar", help='Path to the Py4J jar')
    parser.add_argument("--py4j-classpath", default="geotrellis-extensions-2.2.0-SNAPSHOT.jar",
                        help='Classpath used to launch the Java Gateway')
    parser.add_argument("--task", required=True, dest="task_json", help="The task description in JSON")

    args = parser.parse_args()

    task = json.loads(args.task_json)
    task_id = task["task_id"]
    if task_id not in ['delete_batch_process_results']:
        raise ValueError(f'unsupported task_id "{task_id}"')

    arguments = task['arguments']
    batch_job_id = arguments['batch_job_id']
    subfolders = arguments['subfolders']

    java_opts = [
        "-client",
        "-Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService"
    ]

    java_gateway = JavaGateway.launch_gateway(jarpath=args.py4j_jarpath,
                                              classpath=args.py4j_classpath,
                                              javaopts=java_opts,
                                              die_on_exit=True)

    batch_jobs = GpsBatchJobs(catalog=None, jvm=java_gateway.jvm, principal=None, key_tab=None)

    _log.info(f"removing subfolders {subfolders} for batch job {batch_job_id}...")
    batch_jobs.delete_batch_process_results(job_id=batch_job_id, subfolders=subfolders, propagate_errors=True)


if __name__ == '__main__':
    main()
