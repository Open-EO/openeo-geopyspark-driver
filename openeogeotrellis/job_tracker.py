import os
import kazoo.client
import logging
from decimal import Decimal
from logging.handlers import RotatingFileHandler
import subprocess
import sys
from subprocess import CalledProcessError
from typing import Callable, Union
import time
from collections import namedtuple
from datetime import datetime

import openeogeotrellis.backend
from openeo.util import date_to_rfc3339
import re
from pythonjsonlogger.jsonlogger import JsonFormatter

from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import JOB_STATUS, ElasticJobRegistry
from openeo_driver.util.logging import JSON_LOGGER_DEFAULT_FORMAT
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis import async_task

_log = logging.getLogger(__name__)

# TODO: current implementation mixes YARN and Kubernetes logic. Instead use composition/inheritance for better separation of concerns?
#       Especially because the job registry storage will also get different options: legacy ZooKeeper and ElasticJobRegistry (and maybe even a simple in-memory option)


class JobTracker:
    class _UnknownApplicationIdException(ValueError):
        pass

    _YarnStatus = namedtuple('YarnStatus', ['state', 'final_state', 'start_time', 'finish_time',
                                            'aggregate_resource_allocation'])
    _KubeStatus = namedtuple('KubeStatus', ['state', 'start_time', 'finish_time'])

    def __init__(self, job_registry: Callable[[], ZkJobRegistry], principal: str, keytab: str):
        self._job_registry = job_registry
        self._principal = principal
        self._keytab = keytab
        self._batch_jobs = GpsBatchJobs(catalog=None, jvm=None, principal=principal, key_tab=keytab, vault=None)

    def loop_update_statuses(self, interval_s: int = 60):
        # TODO: this method seems to be unused
        with self._job_registry() as registry:
            registry.ensure_paths()

        try:
            i = 0

            while True:
                try:
                    _log.info("tracking statuses...")

                    if i % 60 == 0:
                        self._refresh_kerberos_tgt()

                    self.update_statuses()
                except Exception:
                    _log.warning("scheduling new run after failing to track batch jobs", exc_info=True)

                time.sleep(interval_s)

                i += 1
        except KeyboardInterrupt:
            pass

    def update_statuses(self) -> None:
        with self._job_registry() as registry:
            registry.ensure_paths()

            jobs_to_track = registry.get_running_jobs()

            for job_info in jobs_to_track:
                job_id = None
                user_id = None
                try:
                    job_id = job_info["job_id"]
                    user_id = job_info["user_id"]
                    application_id = job_info["application_id"]
                    current_status = job_info["status"]

                    if application_id:
                        try:
                            if ConfigParams().is_kube_deploy:
                                from openeogeotrellis.utils import s3_client, download_s3_dir
                                state, start_time, finish_time = JobTracker._kube_status(job_id, user_id)

                                new_status = JobTracker._kube_status_parser(state)

                                registry.patch(job_id, user_id,
                                               status=new_status,
                                               started=start_time,
                                               finished=finish_time)
                                with ElasticJobRegistry.just_log_errors(f"job_tracker update status {new_status}"):
                                    # TODO: also set started/finished
                                    self._batch_jobs._elastic_job_registry.set_status(job_id, new_status)

                                if current_status != new_status:
                                    _log.info("changed job %s status from %s to %s" %
                                              (job_id, current_status, new_status), extra={'job_id': job_id})

                                if state == "COMPLETED":
                                    # TODO: do we support SHub batch processes in this environment? The AWS
                                    #  credentials conflict.

                                    result_metadata = self._batch_jobs.get_results_metadata(job_id, user_id)
                                    usage = self.get_kube_usage(job_id, user_id)
                                    if usage is not None:
                                        result_metadata["usage"] = usage
                                    registry.patch(job_id, user_id, **result_metadata)

                                    registry.mark_done(job_id, user_id)
                                    _log.info("marked %s as done" % job_id, extra={'job_id': job_id})
                            else:
                                state, final_state, start_time, finish_time, aggregate_resource_allocation =\
                                    JobTracker._yarn_status(application_id)

                                memory_time_megabyte_seconds, cpu_time_seconds =\
                                    JobTracker._parse_resource_allocation(aggregate_resource_allocation)

                                new_status = JobTracker._to_openeo_status(state, final_state)

                                registry.patch(job_id, user_id,
                                               status=new_status,
                                               started=JobTracker._to_serializable_datetime(start_time),
                                               finished=JobTracker._to_serializable_datetime(finish_time),
                                               memory_time_megabyte_seconds=memory_time_megabyte_seconds,
                                               cpu_time_seconds=cpu_time_seconds)
                                with ElasticJobRegistry.just_log_errors(f"job_tracker update status from YARN"):
                                    # TODO: also set started/finished, ...
                                    self._batch_jobs._elastic_job_registry.set_status(job_id, new_status)

                                if current_status != new_status:
                                    _log.info("changed job %s status from %s to %s" %
                                              (job_id, current_status, new_status), extra={'job_id': job_id})

                                if final_state != "UNDEFINED":
                                    result_metadata = self._batch_jobs.get_results_metadata(job_id, user_id)
                                    # TODO: skip patching the job znode and read from this file directly?
                                    registry.patch(job_id, user_id, **result_metadata)

                                    registry.remove_dependencies(job_id, user_id)

                                    # there can be duplicates if batch processes are recycled
                                    dependency_sources = list(set(ZkJobRegistry.get_dependency_sources(job_info)))

                                    if dependency_sources:
                                        async_task.schedule_delete_batch_process_dependency_sources(
                                            job_id, user_id, dependency_sources)

                                    registry.mark_done(job_id, user_id)

                                    sentinelhub_processing_units = (result_metadata.get("usage", {})
                                                                    .get("sentinelhub", {}).get("value", 0.0))

                                    sentinelhub_batch_processing_units = (ZkJobRegistry.get_dependency_usage(job_info)
                                                                          or Decimal("0.0"))

                                    _log.info("marked %s as done" % job_id, extra={
                                        'job_id': job_id,
                                        'area': result_metadata.get('area'),
                                        'unique_process_ids': result_metadata.get('unique_process_ids'),
                                        'cpu_time_seconds': cpu_time_seconds,
                                        'sentinelhub': float(Decimal(sentinelhub_processing_units) +
                                                             sentinelhub_batch_processing_units)
                                    })
                        except JobTracker._UnknownApplicationIdException:
                            registry.mark_done(job_id, user_id)
                except Exception:
                    _log.warning(
                        f"resuming with remaining jobs after failing to handle batch job {job_id}",
                        exc_info=True,
                        extra={"job_id": job_id},
                    )
                    if job_id and user_id:
                        registry.set_status(job_id, user_id, JOB_STATUS.ERROR)
                        registry.mark_done(job_id, user_id)

    def get_kube_usage(self,job_id,user_id):
        usage = None
        try:
            import requests
            url = "http://kubecost.kube-dev.vgt.vito.be/model/allocation"
            namespace = "spark-jobs"
            window = "5d"
            pod = f"{JobTracker._kube_prefix(job_id,user_id)}*"
            params = (
                ('aggregate', 'namespace'),
                ('filterNamespaces', namespace),
                ('filterPods', pod),
                ('window', window),
                ('accumulate', 'true'),
            )

            total_cost = requests.get(url, params=params).json()
            if total_cost['code'] == 200:
                cost = total_cost['data'][0][namespace]
                _log.info(f"Successfully retrieved total cost {cost}")
                usage = {}
                usage["cpu"] = {"value": cost["cpuCoreHours"], "unit": "cpu-hours"}
                usage["memory"] = {"value": cost["ramByteHours"] / (1024 * 1024),
                                   "unit": "mb-seconds"}

                return usage
            else:
                _log.error(f"Unable to retrieve job cost {total_cost}")
        except Exception:
            _log.error(f"error while handling creo usage", exc_info=True, extra={'job_id': job_id})
        return usage

    @staticmethod
    def yarn_available() -> bool:
        """Check if YARN tools are available."""
        # TODO: this methods seems to be unused?
        try:
            _log.info("Checking if Hadoop 'yarn' tool is available")
            output = subprocess.check_output(["yarn", "version"]).decode("ascii")
            hadoop_yarn_available = "hadoop" in output.lower()
            _log.info("Hadoop yarn available: {a}".format(a=hadoop_yarn_available))
            return hadoop_yarn_available
        except Exception:
            _log.info("Failed to run 'yarn'", exc_info=True)
            return False

    @staticmethod
    def _kube_prefix(job_id: str, user_id: str):
        from openeogeotrellis.utils import truncate_user_id_k8s, truncate_job_id_k8s
        user_id_truncated = truncate_user_id_k8s(user_id)
        job_id_truncated = truncate_job_id_k8s(job_id)
        return "job-{id}-{user}".format(id=job_id_truncated, user=user_id_truncated)

    @staticmethod
    def _kube_status(job_id: str, user_id: str) -> '_KubeStatus':
        from openeogeotrellis.utils import kube_client

        api_instance = kube_client()
        status = api_instance.get_namespaced_custom_object(
                "sparkoperator.k8s.io",
                "v1beta2",
                "spark-jobs",
                "sparkapplications",
                JobTracker._kube_prefix(job_id,user_id))

        return JobTracker._KubeStatus(
            status['status']['applicationState']['state'],
            status['status']['lastSubmissionAttemptTime'],
            status['status']['terminationTime']
        )

    @staticmethod
    def _yarn_status(application_id: str) -> '_YarnStatus':
        """Returns (State, Final-State) of a job as reported by YARN."""

        try:
            application_report = subprocess.check_output(
                ["yarn", "application", "-status", application_id]).decode()

            props = re.findall(r"\t(.+) : (.+)", application_report)

            def prop_value(name: str) -> str:
                return next(value for key, value in props if key == name)

            return JobTracker._YarnStatus(
                prop_value("State"),
                prop_value("Final-State"),
                prop_value("Start-Time"),
                prop_value("Finish-Time"),
                prop_value("Aggregate Resource Allocation")
            )
        except CalledProcessError as e:
            stdout = e.stdout.decode()
            if "doesn't exist in RM or Timeline Server" in stdout:
                raise JobTracker._UnknownApplicationIdException(stdout)
            else:
                raise

    @staticmethod
    def _parse_resource_allocation(aggregate_resource_allocation) -> (int, int):
        match = re.fullmatch(r"^(\d+) MB-seconds, (\d+) vcore-seconds$", aggregate_resource_allocation)

        return int(match.group(1)), int(match.group(2)) if match else (None, None)

    @staticmethod
    def _to_openeo_status(state: str, final_state: str) -> str:
        # TODO: encapsulate status
        if state == 'ACCEPTED':
            new_status = JOB_STATUS.QUEUED
        elif state == 'RUNNING':
            new_status = JOB_STATUS.RUNNING
        else:
            new_status = JOB_STATUS.CREATED

        if final_state == 'KILLED':
            new_status = JOB_STATUS.CANCELED
        elif final_state == 'SUCCEEDED':
            new_status = JOB_STATUS.FINISHED
        elif final_state == 'FAILED':
            new_status = JOB_STATUS.ERROR

        return new_status

    @staticmethod
    def _kube_status_parser(state: str) -> str:
        if state == 'PENDING':
            new_status = JOB_STATUS.QUEUED
        elif state == 'RUNNING':
            new_status = JOB_STATUS.RUNNING
        elif state == 'COMPLETED':
            new_status = JOB_STATUS.FINISHED
        elif state == 'FAILED':
            new_status = JOB_STATUS.ERROR
        else:
            new_status = JOB_STATUS.CREATED

        return new_status

    @staticmethod
    def _to_serializable_datetime(epoch_millis: str) -> Union[str, None]:
        if epoch_millis == "0":
            return None

        utc_datetime = datetime.utcfromtimestamp(int(epoch_millis) / 1000)
        return date_to_rfc3339(utc_datetime)

    def _refresh_kerberos_tgt(self):
        if self._keytab and self._principal:
            cmd = ["kinit", "-V", "-kt", self._keytab, self._principal]

            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

            for line in p.stdout:
                _log.info(line.rstrip().decode())

            p.wait()
            if p.returncode:
                _log.warning("{c} returned exit code {r}".format(c=" ".join(cmd), r=p.returncode))
        else:
            _log.warning("No Kerberos principal/keytab: will not refresh TGT")


if __name__ == '__main__':
    import argparse

    # TODO: (re)use central logging setup helpers from `openeo_driver.util.logging
    logging.basicConfig(level=logging.INFO)
    openeogeotrellis.backend.logger.setLevel(logging.DEBUG)
    kazoo.client.log.setLevel(logging.WARNING)

    root_logger = logging.getLogger()
    json_formatter = JsonFormatter(JSON_LOGGER_DEFAULT_FORMAT)  # Note: The Java logging is also supposed to match.

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.formatter = json_formatter

    root_logger.addHandler(stdout_handler)

    if not ConfigParams().is_kube_deploy:
        rolling_file_handler = RotatingFileHandler("logs/job_tracker_python.log", maxBytes=10 * 1024 * 1024,
                                                   backupCount=1)
        rolling_file_handler.formatter = json_formatter
        root_logger.addHandler(rolling_file_handler)

    _log.info("ConfigParams(): {c}".format(c=ConfigParams()))

    parser = argparse.ArgumentParser(usage="OpenEO JobTracker", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--principal", default="openeo@VGT.VITO.BE", help="Principal to be used to login to KDC")
    parser.add_argument("--keytab", default="openeo-deploy/mep/openeo.keytab",
                        help="The full path to the file that contains the keytab for the principal")
    args = parser.parse_args()

    try:
        JobTracker(ZkJobRegistry, args.principal, args.keytab).update_statuses()
    except JobNotFoundException as e:
        _log.error(e, exc_info=True, extra={'job_id': e.job_id})
    except Exception as e:
        _log.error(e, exc_info=True)
        raise e
