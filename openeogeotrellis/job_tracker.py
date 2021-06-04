import logging
import subprocess
import sys
from subprocess import CalledProcessError
from typing import Callable, Union, List
import traceback
import time
from collections import namedtuple
from datetime import datetime
from openeo.util import date_to_rfc3339
import re
from py4j.java_gateway import JavaGateway, JVMView
from py4j.protocol import Py4JJavaError

from openeogeotrellis.job_registry import JobRegistry
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.layercatalog import get_layer_catalog
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis import utils

_log = logging.getLogger(__name__)

# TODO: make this job tracker logic an internal implementation detail of JobRegistry?


class JobTracker:
    class _UnknownApplicationIdException(ValueError):
        pass

    _YarnStatus = namedtuple('YarnStatus', ['state', 'final_state', 'start_time', 'finish_time',
                                            'aggregate_resource_allocation'])
    _KubeStatus = namedtuple('KubeStatus', ['state', 'start_time', 'finish_time'])

    def __init__(self, job_registry: Callable[[], JobRegistry], principal: str, keytab: str,
                 jvm: JVMView = utils.get_jvm()):
        self._job_registry = job_registry
        self._principal = principal
        self._keytab = keytab
        # TODO: get rid of dependency on catalog
        self._batch_jobs = GpsBatchJobs(catalog=get_layer_catalog(opensearch_enrich=False))
        self._jvm = jvm

    def loop_update_statuses(self, interval_s: int = 60):
        try:
            i = 0

            while True:
                try:
                    _log.info("tracking statuses...")

                    if i % 60 == 0:
                        self._refresh_kerberos_tgt()

                    self.update_statuses()
                except Exception:
                    _log.warning("scheduling new run after failing to track batch jobs:\n{e}"
                                 .format(e=traceback.format_exc()))

                time.sleep(interval_s)

                i += 1
        except KeyboardInterrupt:
            pass

    def update_statuses(self) -> None:
        with self._job_registry() as registry:
            jobs_to_track = registry.get_running_jobs()

            for job in jobs_to_track:
                try:
                    job_id, user_id = job['job_id'], job['user_id']
                    application_id, current_status = job['application_id'], job['status']

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

                                if current_status != new_status:
                                    _log.info("changed job %s status from %s to %s" %
                                              (job_id, current_status, new_status), extra={'job_id': job_id})

                                if state == "COMPLETED":
                                    # FIXME: do we support SHub batch processes in this environment? The AWS
                                    #  credentials conflict.
                                    download_s3_dir("OpenEO-data", "batch_jobs/{j}".format(j=job_id))

                                    result_metadata = self._batch_jobs.get_results_metadata(job_id, user_id)
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

                                if current_status != new_status:
                                    _log.info("changed job %s status from %s to %s" %
                                              (job_id, current_status, new_status), extra={job_id: job_id})

                                if final_state != "UNDEFINED":
                                    result_metadata = self._batch_jobs.get_results_metadata(job_id, user_id)
                                    # TODO: skip patching the job znode and read from this file directly?
                                    registry.patch(job_id, user_id, **result_metadata)

                                    if new_status == 'finished':
                                        subfolders = [dependency.get('subfolder')
                                                      or dependency['batch_request_id']
                                                      for dependency in job.get('dependencies') or []]

                                        self._delete_batch_process_results(job_id, subfolders)
                                        registry.remove_dependencies(job_id, user_id)

                                    registry.mark_done(job_id, user_id)
                                    _log.info("marked %s as done" % job_id, extra={'job_id': job_id})
                        except JobTracker._UnknownApplicationIdException:
                            registry.mark_done(job_id, user_id)
                except Exception:
                    _log.warning("resuming with remaining jobs after failing to handle batch job {j}:\n{e}"
                                 .format(j=job_id, e=traceback.format_exc()), extra={'job_id': job_id})
                    # TODO: mark it as done to keep it from being considered further?


    @staticmethod
    def yarn_available() -> bool:
        """Check if YARN tools are available."""
        try:
            _log.info("Checking if Hadoop 'yarn' tool is available")
            output = subprocess.check_output(["yarn", "version"]).decode("ascii")
            hadoop_yarn_available = "hadoop" in output.lower()
            _log.info("Hadoop yarn available: {a}".format(a=hadoop_yarn_available))
            return hadoop_yarn_available
        except Exception as e:
            _log.info("Failed to run 'yarn': {e!r}".format(e=e))
            return False

    @staticmethod
    def _kube_status(job_id: str, user_id: str) -> '_KubeStatus':
        from openeogeotrellis.utils import kube_client
        try:
            api_instance = kube_client()
            status = api_instance.get_namespaced_custom_object(
                    "sparkoperator.k8s.io",
                    "v1beta2",
                    "spark-jobs",
                    "sparkapplications",
                    "job-{id}-{user}".format(id=job_id, user=user_id))

            return JobTracker._KubeStatus(
                status['status']['applicationState']['state'],
                status['status']['lastSubmissionAttemptTime'],
                status['status']['terminationTime']
            )

        except Exception as e:
            _log.info(e)

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
            new_status = 'queued'
        elif state == 'RUNNING':
            new_status = 'running'
        else:
            new_status = 'created'

        if final_state == 'KILLED':
            new_status = 'canceled'
        elif final_state == 'SUCCEEDED':
            new_status = 'finished'
        elif final_state == 'FAILED':
            new_status = 'error'

        return new_status

    @staticmethod
    def _kube_status_parser(state: str) -> str:
        if state == 'PENDING':
            new_status = 'queued'
        elif state == 'RUNNING':
            new_status = 'running'
        elif state == 'COMPLETED':
            new_status = 'finished'
        elif state == 'FAILED':
            new_status = 'error'
        else:
            new_status = 'created'

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

    def _delete_batch_process_results(self, job_id: str, subfolders: List[str]):
        s3_service = self._jvm.org.openeo.geotrellissentinelhub.S3Service()
        bucket_name = ConfigParams().sentinel_hub_batch_bucket

        for subfolder in subfolders:
            try:
                s3_service.delete_batch_process_results(bucket_name, subfolder)
            except Py4JJavaError as e:
                java_exception = e.java_exception

                if (java_exception.getClass().getName() ==
                        'org.openeo.geotrellissentinelhub.S3Service$UnknownFolderException'):
                    _log.warning("could not delete unknown result folder s3://{b}/{f}"
                                 .format(b=bucket_name, f=subfolder), extra={'job_id': job_id})
                else:
                    raise e

        _log.info("deleted result folders {fs} for batch job {j}".format(fs=subfolders, j=job_id),
                  extra={'job_id': job_id})


if __name__ == '__main__':
    import json
    from openeo.util import dict_no_none

    logging.basicConfig(level=logging.INFO)

    class JsonFormatter:
        # TODO: add 'message' and a timestamp that is recognized by Kibana ('asctime'?)
        def format(self, record):
            formatted_record = {}

            for key in ['created', 'levelname', 'msg', 'job_id']:
                formatted_record[key] = getattr(record, key, None)

            return json.dumps(dict_no_none(**formatted_record))

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.formatter = JsonFormatter()

    _log.addHandler(handler)

    try:
        # TODO: make configurable
        java_gateway = JavaGateway.launch_gateway(jarpath="venv36/share/py4j/py4j0.10.9.2.jar",
                                                  classpath="geotrellis-extensions-2.2.0-SNAPSHOT.jar",
                                                  javaopts=["-client"],
                                                  die_on_exit=True)

        JobTracker(JobRegistry, principal="", keytab="", jvm=java_gateway.jvm).update_statuses()
    except:
        _log.error(traceback.format_exc())
