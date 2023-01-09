import abc
import logging
import re
import subprocess
import sys
from datetime import datetime
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from pathlib import Path
from subprocess import CalledProcessError
from typing import Callable, NamedTuple, Optional, Union

import kazoo.client
import kubernetes.client.exceptions
import requests
from openeo.util import rfc3339, url_join
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import JOB_STATUS, ElasticJobRegistry
from openeo_driver.util.logging import JSON_LOGGER_DEFAULT_FORMAT
from pythonjsonlogger.jsonlogger import JsonFormatter

import openeogeotrellis.backend
from openeogeotrellis import async_task
from openeogeotrellis.backend import GpsBatchJobs, get_or_build_elastic_job_registry
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.integrations.kubernetes import (
    K8S_SPARK_APP_STATE,
    k8s_job_name,
    k8s_state_to_openeo_job_status,
    kube_client,
)
from openeogeotrellis.integrations.yarn import yarn_state_to_openeo_job_status
from openeogeotrellis.job_registry import ZkJobRegistry

_log = logging.getLogger(__name__)

# TODO: current implementation mixes YARN and Kubernetes logic. Instead use composition/inheritance for better separation of concerns?
#       Especially because the job registry storage will also get different options: legacy ZooKeeper and ElasticJobRegistry (and maybe even a simple in-memory option)


class _JobMetadata(NamedTuple):
    """Simple container for openEO batch job oriented metadata"""
    # Job status, following the openEO API spec (see `openeo_driver.jobregistry.JOB_STATUS`)
    status: str

    # RFC-3339 formatted UTC datetime (or None if not started yet)
    start_time: Optional[str] = None

    # RFC-3339 formatted UTC datetime (or None if not finished yet)
    finish_time: Optional[str] = None

    # Resource usage stats (if any)
    usage: Optional[dict] = None


class JobMetadataGetterInterface(metaclass=abc.ABCMeta):
    """
    Interface for implementations that interact with some kind of
    task/app orchestration/management component
    to get a job's status metadata (state, start/finish times, resource usage stats, ...)
    For example: YARN ("yarn applications -status ..."), Kubernetes, ...
    """

    @abc.abstractmethod
    def get_job_metadata(self, job_id: str, user_id: str, app_id: str) -> _JobMetadata:
        raise NotImplementedError


class AppNotFound(Exception):
    """Exception to throw when app can not be found in YARN, Kubernetes, ..."""
    pass


class YarnAppReportParseException(Exception):
    pass


class YarnStatusGetter(JobMetadataGetterInterface):
    """YARN app status getter"""

    def get_job_metadata(self, job_id: str, user_id: str, app_id: str) -> _JobMetadata:

        try:
            application_report = subprocess.check_output(
                ["yarn", "application", "-status", app_id]
            ).decode("utf-8")
        except CalledProcessError as e:
            stdout = e.stdout.decode()
            if "doesn't exist in RM or Timeline Server" in stdout:
                raise AppNotFound(stdout) from None
            else:
                raise
        return self.parse_application_report(application_report)

    def parse_application_report(self, report: str) -> _JobMetadata:
        try:
            props = dict(re.findall(r"^\t(.+?) : (.+)$", report, flags=re.MULTILINE))

            job_status = yarn_state_to_openeo_job_status(
                state=props["State"], final_state=props["Final-State"]
            )

            def ms_epoch_to_date(epoch_millis: str) -> Union[str, None]:
                """Parse millisecond timestamp from app report and return as rfc3339 date (or None)"""
                if epoch_millis == "0":
                    return None
                utc_datetime = datetime.utcfromtimestamp(int(epoch_millis) / 1000)
                return rfc3339.datetime(utc_datetime)

            start_time = ms_epoch_to_date(props["Start-Time"])
            finish_time = ms_epoch_to_date(props["Finish-Time"])

            allocation = props["Aggregate Resource Allocation"]
            match = re.fullmatch(r"^(\d+) MB-seconds, (\d+) vcore-seconds$", allocation)
            if match:
                usage = {
                    "cpu": {"value": int(match.group(2)), "unit": "cpu-seconds"},
                    "memory": {"value": int(match.group(1)), "unit": "mb-seconds"},
                }
            else:
                usage = None

            return _JobMetadata(
                status=job_status,
                start_time=start_time,
                finish_time=finish_time,
                usage=usage,
            )
        except Exception as e:
            raise YarnAppReportParseException() from e


class K8sStatusGetter(JobMetadataGetterInterface):
    """Kubernetes app status getter"""

    def __init__(self, kubecost_url: Optional[str] = None):
        self._kubernetes_api = kube_client()
        # TODO: get this url from config?
        self._kubecost_url = kubecost_url or "http://kubecost.kube-dev.vgt.vito.be/"

    def get_job_metadata(self, job_id: str, user_id: str, app_id: str) -> _JobMetadata:
        job_status = self._get_job_status(job_id=job_id, user_id=user_id)
        usage = self._get_usage(job_id=job_id, user_id=user_id)
        return _JobMetadata(
            status=job_status.status,
            start_time=job_status.start_time,
            finish_time=job_status.finish_time,
            usage=usage,
        )

    def _get_job_status(self, job_id: str, user_id: str) -> _JobMetadata:
        try:
            metadata = self._kubernetes_api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace="spark-jobs",
                plural="sparkapplications",
                name=k8s_job_name(job_id=job_id, user_id=user_id),
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                # TODO: more precise checking that it was indeed the app that was not found (instead of k8s api itself).
                raise AppNotFound() from e
            raise

        if "status" in metadata:
            app_state = metadata["status"]["applicationState"]["state"]
            start_time = metadata["status"]["lastSubmissionAttemptTime"]
            finish_time = metadata["status"]["terminationTime"]
        else:
            _log.warning("No K8s app status found, assuming new app")
            app_state = K8S_SPARK_APP_STATE.NEW
            start_time = finish_time = None

        job_status = k8s_state_to_openeo_job_status(app_state)
        return _JobMetadata(
            status=job_status, start_time=start_time, finish_time=finish_time
        )

    def _get_usage(self, job_id: str, user_id: str) -> Union[dict, None]:
        try:
            url = url_join(self._kubecost_url, "/model/allocation")
            namespace = "spark-jobs"
            window = "5d"
            pod = k8s_job_name(job_id=job_id, user_id=user_id) + "*"
            params = (
                ("aggregate", "namespace"),
                ("filterNamespaces", namespace),
                ("filterPods", pod),
                ("window", window),
                ("accumulate", "true"),
            )
            response = requests.get(url, params=params)
            response.raise_for_status()
            total_cost = response.json()
            if total_cost["code"] != 200:
                raise ValueError(total_cost)

            cost = total_cost["data"][0][namespace]
            # TODO: need to iterate through "data" list?
            _log.info(f"Successfully retrieved total cost {cost}")
            usage = {}
            usage["cpu"] = {"value": cost["cpuCoreHours"], "unit": "cpu-hours"}
            usage["memory"] = {
                "value": cost["ramByteHours"] / (1024 * 1024),
                "unit": "mb-hours",
            }
            return usage
        except Exception:
            _log.error(
                f"Failed to retrieve usage stats from kubecost",
                exc_info=True,
                extra={"job_id": job_id},
            )


class JobTracker:
    def __init__(
        self,
        app_state_getter: JobMetadataGetterInterface,
        job_registry: Callable[[], ZkJobRegistry],
        principal: str,
        keytab: str,
        output_root_dir: Optional[Union[str, Path]] = None,
        elastic_job_registry: Optional[ElasticJobRegistry] = None,
    ):
        self._app_state_getter = app_state_getter
        self._job_registry = job_registry
        self._principal = principal
        self._keytab = keytab
        # TODO: inject GpsBatchJobs (instead of constructing it here and requiring all its constructor args to be present)
        #       Also note that only `get_results_metadata` is actually used, so dragging a complete GpsBatchJobs might actuall be overkill in the first place.
        self._batch_jobs = GpsBatchJobs(
            catalog=None,
            jvm=None,
            principal=principal,
            key_tab=keytab,
            vault=None,
            output_root_dir=output_root_dir,
            elastic_job_registry=elastic_job_registry,
        )
        self._elastic_job_registry = get_or_build_elastic_job_registry(
            elastic_job_registry, ref="JobTracker"
        )

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
                    previous_status = job_info["status"]

                    if application_id:
                        # Artificial indentation levels for easier diff review
                        if True:
                            if True:
                                job_metadata = self._app_state_getter.get_job_metadata(
                                    job_id=job_id,
                                    user_id=user_id,
                                    app_id=application_id,
                                )

                                if previous_status != job_metadata.status:
                                    _log.info(
                                        f"job {job_id}: status change from {previous_status} to {job_metadata.status}",
                                        extra={"job_id": job_id},
                                    )

                                extra = {}
                                if job_metadata.usage:
                                    # TODO: is this old-style usage reporting still necessary?
                                    if job_metadata.usage.get("memory", {}).get("unit") == "mb-seconds":
                                        extra["memory_time_megabyte_seconds"] = job_metadata.usage.get("memory", {}).get("value")
                                    if job_metadata.usage.get("cpu", {}).get("unit") == "cpu-seconds":
                                        extra["cpu_time_seconds"] = job_metadata.usage.get("cpu", {}).get("value")

                                registry.patch(
                                    job_id=job_id,
                                    user_id=user_id,
                                    status=job_metadata.status,
                                    started=job_metadata.start_time,
                                    finished=job_metadata.finish_time,
                                    usage=job_metadata.usage,
                                    **extra,
                                )
                                with ElasticJobRegistry.just_log_errors(
                                    f"job_tracker {job_metadata.status=} from {type(self._app_state_getter).__name__}"
                                ):
                                    if self._elastic_job_registry:
                                        self._elastic_job_registry.set_status(
                                            job_id=job_id,
                                            status=job_metadata.status,
                                            started=job_metadata.start_time,
                                            finished=job_metadata.finish_time,
                                            # TODO: also record usage data
                                        )

                                if job_metadata.status in {
                                    JOB_STATUS.FINISHED,
                                    JOB_STATUS.ERROR,
                                    JOB_STATUS.CANCELED,
                                }:
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

                                    # TODO: make this usage report handling/logging more generic?
                                    sentinelhub_processing_units = (result_metadata.get("usage", {})
                                                                    .get("sentinelhub", {}).get("value", 0.0))

                                    sentinelhub_batch_processing_units = (ZkJobRegistry.get_dependency_usage(job_info)
                                                                          or Decimal("0.0"))

                                    _log.info(f"marked {job_id} as done", extra={
                                        'job_id': job_id,
                                        'area': result_metadata.get('area'),
                                        'unique_process_ids': result_metadata.get('unique_process_ids'),
                                        # 'cpu_time_seconds': cpu_time_seconds,  # TODO: necessary to get this working again?
                                        'sentinelhub': float(Decimal(sentinelhub_processing_units) +
                                                             sentinelhub_batch_processing_units)
                                    })
                except AppNotFound:
                    _log.warning(
                        f"App not found for {job_id=}",
                        exc_info=True,
                        extra={"job_id": job_id},
                    )
                    if job_id and user_id:
                        registry.set_status(job_id, user_id, JOB_STATUS.ERROR)
                        registry.mark_done(job_id, user_id)
                    with ElasticJobRegistry.just_log_errors(
                        f"job_tracker app not found"
                    ):
                        if self._elastic_job_registry:
                            # TODO: also set started/finished, exception/error info ...
                            self._elastic_job_registry.set_status(
                                job_id=job_id, status=JOB_STATUS.ERROR
                            )
                except Exception as e:
                    # TODO: option for strict mode (fail fast instead of just warnings)?
                    _log.exception(
                        f"Failed status update of {job_id=}: unexpected {type(e).__name__}: {e}",
                        exc_info=True,
                        extra={"job_id": job_id},
                    )
                    # TODO: this looks risky: an unexpected issue in JobTracker logic
                    #  will cause a job (or possibly all running jobs) to be marked as "done" with status "error"?
                    #       Temporarily disabling this for now. To be reconsidered later?
                    # if job_id and user_id:
                    #     registry.set_status(job_id, user_id, JOB_STATUS.ERROR)
                    #     registry.mark_done(job_id, user_id)



if __name__ == "__main__":
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
        if ConfigParams().is_kube_deploy:
            app_state_getter = K8sStatusGetter()
        else:
            app_state_getter = YarnStatusGetter()
        job_tracker = JobTracker(
            app_state_getter=app_state_getter,
            job_registry=ZkJobRegistry,
            principal=args.principal,
            keytab=args.keytab,
        )
        job_tracker.update_statuses()
    except JobNotFoundException as e:
        # TODO: is this necessary? From where would this exception be thrown?
        _log.error(e, exc_info=True, extra={'job_id': e.job_id})
    except Exception as e:
        _log.error(e, exc_info=True)
        raise e
