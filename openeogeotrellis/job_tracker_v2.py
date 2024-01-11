"""
V2 implementation of JobTracker
"""
# TODO: when stable enough: eliminate original implementation and get rid of "v2"?

import abc
import argparse
import collections
import datetime as dt
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any, List, NamedTuple, Optional, Union

import requests

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.integrations.prometheus import Prometheus

# We only need requests_gssapi for Yarn, which uses Kerberos authentication.
try:
    import requests_gssapi
except ImportError:
    requests_gssapi = None

from openeo.util import TimingLogger, repr_truncate, Rfc3339, rfc3339, url_join, deep_get
from openeo_driver.jobregistry import JOB_STATUS, ElasticJobRegistry
from openeo_driver.util.http import requests_with_retry
from openeo_driver.util.logging import (
    get_logging_config,
    setup_logging,
    LOG_HANDLER_STDERR_BASIC,
    LOG_HANDLER_STDERR_JSON,
)
import openeo_driver.utils

from openeogeotrellis import async_task
from openeogeotrellis.backend import GpsBatchJobs, get_elastic_job_registry
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.integrations.kubernetes import (
    K8S_SPARK_APP_STATE,
    k8s_state_to_openeo_job_status,
    kube_client,
)
from openeogeotrellis.integrations.etl_api import ETL_API_STATE, get_etl_api
from openeogeotrellis.integrations.yarn import yarn_state_to_openeo_job_status, YARN_STATE
from openeogeotrellis.job_costs_calculator import (
    JobCostsCalculator,
    noJobCostsCalculator,
    CostsDetails,
    EtlApiJobCostsCalculator,
)
from openeogeotrellis.job_registry import ZkJobRegistry, get_deletable_dependency_sources
from openeogeotrellis.utils import StatsReporter, dict_merge_recursive


# Note: hardcoded logger name as this script is executed directly which kills the usefulness of `__name__`.
_log = logging.getLogger("openeogeotrellis.job_tracker_v2")


class _Usage(NamedTuple):
    cpu_seconds: Optional[float] = None
    mb_seconds: Optional[float] = None
    network_receive_bytes: Optional[float] = None
    max_executor_gigabytes: Optional[float] = None

    def to_dict(self) -> dict:
        result = {}

        if self.cpu_seconds:
            result["cpu"] = {"value": self.cpu_seconds, "unit": "cpu-seconds"}
        if self.mb_seconds:
            result["memory"] = {"value": self.mb_seconds, "unit": "mb-seconds"}
        if self.network_receive_bytes:
            result["network_received"] = {"value": self.network_receive_bytes, "unit": "b"}
        if self.max_executor_gigabytes:
            result["max_executor_memory"] = {"value": self.max_executor_gigabytes, "unit": "gb"}

        return result


class _JobMetadata(NamedTuple):
    """Simple container for openEO batch job oriented metadata"""

    # Job state, implementation-specific
    app_state: str

    # Job status, following the openEO API spec (see `openeo_driver.jobregistry.JOB_STATUS`)
    status: str

    # Resource usage stats
    usage: _Usage

    # UTC datetime (or None if not started yet)
    start_time: Optional[dt.datetime] = None

    # UTC datetime (or None if not finished yet)
    finish_time: Optional[dt.datetime] = None


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

    @classmethod
    @abc.abstractmethod
    def app_state_to_etl_state(cls, app_state: str) -> str:
        # TODO #610 this is just part of a temporary migration path, to be cleaned up when not necessary anymore
        raise NotImplementedError


class JobTrackerException(Exception):
    pass


class AppNotFound(JobTrackerException):
    """Exception to throw when app can not be found in YARN, Kubernetes, ..."""
    pass


class YarnAppReportParseException(Exception):
    pass


class YarnStatusGetter(JobMetadataGetterInterface):
    """YARN app status getter"""

    def __init__(self, yarn_api_base_url: str, auth: Optional[Any] = None):
        """Constructor for a YarnStatusGetter instance.

        :param yarn_api_base_url:
            The start of the URL for requests to the YARN REST API.
            See also: get_application_url(...)

        :param auth:
            If specified, this is the authentication passed to requests.get(...)
            Defaults to None, and by default no authentication will be used.

            Normally we use Kerberos and then you should pass an instance of
            requests_gssapi.HTTPSPNEGOAuth.

            In general, you could use any authentication that requests.get(auth=auth)
            accepts for its auth parameter, if needed.
            See also: https://requests.readthedocs.io/en/latest/api/#requests.request
        """
        self.yarn_api_base_url = yarn_api_base_url
        self.auth = auth

    def get_application_url(self, application_id: str) -> str:
        """Get the URL to get the application status from the YARN REST API.

        :param application_id: The same app_id expected by get_job_metadata(...)

        :return: The full URL to get the application status from YARN.
        """
        return url_join(
            self.yarn_api_base_url,
            f"/ws/v1/cluster/apps/{application_id}",
        )

    def get_job_metadata(self, job_id: str, user_id: str, app_id: str) -> _JobMetadata:
        url = self.get_application_url(application_id=app_id)
        response = requests.get(url, auth=self.auth)
        if response.status_code == 404:
            raise AppNotFound(response)
        else:
            # Check if there was any other HTTP error status.
            response.raise_for_status()
            # Handle a corrupt response that is not a dict (most likely a string).
            json = response.json()
            if not isinstance(json, dict):
                raise YarnAppReportParseException(
                    "Cannot parse response body: expecting a JSON dict but body contains "
                    + f"a value of type {type(json)}, value={json!r} Response body={response.text!r}"
                )
            return self.parse_application_response(data=json, job_id=job_id, user_id=user_id)

    @staticmethod
    def _ms_epoch_to_date(epoch_millis: int) -> Union[dt.datetime, None]:
        """Parse millisecond timestamp from app report and return as rfc3339 date (or None)"""
        if epoch_millis == 0:
            return None
        return dt.datetime.utcfromtimestamp(epoch_millis / 1000)

    @classmethod
    def parse_application_response(cls, data: dict, job_id: str, user_id: str) -> _JobMetadata:
        """Parse the HTTP response body of the application status request.

        :param data: The data in the HTTP response body, which was in JSON format.
        :param job_id: context for the error log
        :param user_id: context for the error log
        :raises YarnAppReportParseException: When the JSON response can not be parsed properly.
        :return: _JobMetadata containing the info we need about the Job.
        """

        report = data.get("app", {})
        required_keys = ["state", "finalStatus", "startedTime", "finishedTime"]
        missing_keys = [k for k in required_keys if k not in report]
        if missing_keys:
            raise YarnAppReportParseException(
                f"JSON response is missing following required keys: {missing_keys}, json={data}"
            )

        try:
            state = report["state"]
            job_status = yarn_state_to_openeo_job_status(
                state=state, final_state=report["finalStatus"]
            )
            # Log the diagnostics if there was an error.
            # In particular, sometimes YARN can't launch the container and then the
            # field 'diagnostics' provides more info why this failed.
            diagnostics = report.get("diagnostics", None)
            if job_status == JOB_STATUS.ERROR and diagnostics:
                _log.error(
                    f"YARN application status reports error diagnostics: {diagnostics}",
                    extra={"job_id": job_id, "user_id": user_id}
                )

            start_time = cls._ms_epoch_to_date(report["startedTime"])
            finish_time = cls._ms_epoch_to_date(report["finishedTime"])

            memory_seconds = report.get("memorySeconds")
            vcore_seconds = report.get("vcoreSeconds")
            usage = _Usage(cpu_seconds=vcore_seconds, mb_seconds=memory_seconds)

            return _JobMetadata(
                app_state=state,
                status=job_status,
                start_time=start_time,
                finish_time=finish_time,
                usage=usage,
            )
        except Exception as e:
            raise YarnAppReportParseException() from e

    # TODO #610 this is just part of a temporary migration path, to be cleaned up when not necessary anymore
    _yarn_state_to_etl_api_state = {
        YARN_STATE.ACCEPTED: ETL_API_STATE.ACCEPTED,
        YARN_STATE.RUNNING: ETL_API_STATE.RUNNING,
        YARN_STATE.FINISHED: ETL_API_STATE.FINISHED,
        YARN_STATE.KILLED: ETL_API_STATE.KILLED,
        YARN_STATE.FAILED: ETL_API_STATE.FAILED,
    }

    @classmethod
    def app_state_to_etl_state(cls, app_state: str) -> str:
        # TODO #610 this is just part of a temporary migration path, to be cleaned up when not necessary anymore
        if app_state not in cls._yarn_state_to_etl_api_state:
            _log.warning(f"Unhandled YARN app state mapping: {app_state}")
        return cls._yarn_state_to_etl_api_state.get(app_state, ETL_API_STATE.UNDEFINED)


class K8sException(Exception):
    pass


class K8sStatusGetter(JobMetadataGetterInterface):
    """Kubernetes app status getter"""

    def __init__(self, kubernetes_api, prometheus_api):
        self._kubernetes_api = kubernetes_api
        self._prometheus_api = prometheus_api

    def get_job_metadata(self, job_id: str, user_id: str, app_id: str) -> _JobMetadata:
        # Local import to avoid kubernetes dependency when not necessary
        import kubernetes.client.exceptions
        try:
            metadata = self._kubernetes_api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=ConfigParams().pod_namespace,
                plural="sparkapplications",
                name=app_id,
            )
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                # TODO: more precise checking that it was indeed the app that was not found (instead of k8s api itself).
                raise AppNotFound() from e
            raise

        if "status" in metadata:
            app_state = metadata["status"]["applicationState"]["state"]
            datetime_formatter = Rfc3339(propagate_none=True)
            start_time = datetime_formatter.parse_datetime(metadata["status"]["lastSubmissionAttemptTime"])
            finish_time = datetime_formatter.parse_datetime(metadata["status"]["terminationTime"])
        else:
            _log.warning("No K8s app status found, assuming new app", extra={"job_id": job_id, "user_id": user_id})
            app_state = K8S_SPARK_APP_STATE.NEW
            start_time = finish_time = None

        job_status = k8s_state_to_openeo_job_status(app_state)
        return _JobMetadata(
            app_state=app_state, status=job_status, usage=self._get_usage(app_id, start_time, finish_time,
                                                                          job_id, user_id),
            start_time=start_time, finish_time=finish_time
        )

    def _get_usage(self, application_id: str, start_time: Optional[dt.datetime], finish_time: Optional[dt.datetime],
                   job_id: str, user_id: str) -> _Usage:
        try:
            if start_time is None or finish_time is None:
                application_duration_s = None
                byte_seconds = None
            else:
                application_duration_s = (finish_time - start_time).total_seconds()
                byte_seconds = self._prometheus_api.get_memory_usage(application_id, application_duration_s)

            cpu_seconds = self._prometheus_api.get_cpu_usage(application_id)
            network_receive_bytes = self._prometheus_api.get_network_received_usage(application_id)

            max_executor_gigabyte = self._prometheus_api.get_max_executor_memory_usage(application_id)


            _log.info(f"Successfully retrieved usage stats from {self._prometheus_api.endpoint}: "
                      f"{cpu_seconds=}, {byte_seconds=}, {network_receive_bytes=}, {max_executor_gigabyte=}",
                      extra={"job_id": job_id, "user_id": user_id})

            if application_duration_s is not None:
                if not cpu_seconds and not byte_seconds:
                    _log.warning(f"App {application_id} took {application_duration_s}s "
                                 f"but no CPU or memory usage was recorded: {cpu_seconds=} and {byte_seconds=}",
                                 extra={"job_id": job_id, "user_id": user_id})
                if not cpu_seconds:
                    cpu_seconds = get_backend_config().default_usage_cpu_seconds
                if not byte_seconds:
                    byte_seconds = get_backend_config().default_usage_byte_seconds

            return _Usage(cpu_seconds=cpu_seconds,
                          mb_seconds=byte_seconds / (1024 * 1024) if byte_seconds is not None else None,
                          network_receive_bytes=network_receive_bytes, max_executor_gigabytes=max_executor_gigabyte
                          )
        except Exception as e:
            _log.exception(
                f"Failed to retrieve usage stats from {self._prometheus_api.endpoint}: {type(e).__name__}: {e}",
                extra={"job_id": job_id, "user_id": user_id},
            )

        return _Usage()

    # TODO #610 this is just part of a temporary migration path, to be cleaned up when not necessary anymore
    _k8s_state_to_etl_api_state = {
        K8S_SPARK_APP_STATE.NEW: ETL_API_STATE.ACCEPTED,
        K8S_SPARK_APP_STATE.SUBMITTED: ETL_API_STATE.ACCEPTED,
        K8S_SPARK_APP_STATE.RUNNING: ETL_API_STATE.RUNNING,
        K8S_SPARK_APP_STATE.SUCCEEDING: ETL_API_STATE.RUNNING,
        K8S_SPARK_APP_STATE.COMPLETED: ETL_API_STATE.FINISHED,
        K8S_SPARK_APP_STATE.FAILED: ETL_API_STATE.FAILED,
        K8S_SPARK_APP_STATE.SUBMISSION_FAILED: ETL_API_STATE.FAILED,
        K8S_SPARK_APP_STATE.FAILING: ETL_API_STATE.FAILED,
    }

    @classmethod
    def app_state_to_etl_state(cls, app_state: str) -> str:
        # TODO #610 this is just part of a temporary migration path, to be cleaned up when not necessary anymore
        if app_state not in cls._k8s_state_to_etl_api_state:
            _log.warning(f"Unhandled K8s app state mapping: {app_state}")
        return cls._k8s_state_to_etl_api_state.get(app_state, ETL_API_STATE.UNDEFINED)


class JobTracker:
    def __init__(
        self,
        app_state_getter: JobMetadataGetterInterface,
        zk_job_registry: ZkJobRegistry,
        principal: str,
        keytab: str,
        job_costs_calculator: JobCostsCalculator = noJobCostsCalculator,
        output_root_dir: Optional[Union[str, Path]] = None,
        elastic_job_registry: Optional[ElasticJobRegistry] = None
    ):
        self._app_state_getter = app_state_getter
        # TODO #236/#498/#632 make ZkJobRegistry optional
        self._zk_job_registry = zk_job_registry
        self._job_costs_calculator = job_costs_calculator
        # TODO: inject GpsBatchJobs (instead of constructing it here and requiring all its constructor args to be present)
        #       Also note that only `load_results_metadata` is actually used, so dragging a complete GpsBatchJobs might actually be overkill in the first place.
        self._batch_jobs = GpsBatchJobs(
            catalog=None,
            jvm=None,
            principal=principal,
            key_tab=keytab,
            vault=None,
            output_root_dir=output_root_dir,
            elastic_job_registry=elastic_job_registry,
        )
        self._elastic_job_registry = elastic_job_registry

    def update_statuses(self, fail_fast: bool = False) -> None:
        """Iterate through all known (ongoing) jobs and update their status"""
        # TODO #236/#498/#632 make ZkJobRegistry optional
        with self._zk_job_registry as zk_job_registry, StatsReporter(
            name="JobTracker.update_statuses stats", report=_log.info
        ) as stats, TimingLogger("JobTracker.update_statuses", logger=_log.info):

            # TODO: #236/#498/#632 also/instead get jobs_to_track from EJR?
            jobs_to_track = zk_job_registry.get_running_jobs(parse_specification=True)

            for job_info in jobs_to_track:
                stats["collected jobs"] += 1
                if not (
                    isinstance(job_info, dict)
                    and job_info.get("job_id")
                    and job_info.get("user_id")
                ):
                    _log.error(
                        f"Invalid job info: {repr_truncate(job_info, width=200)}"
                    )
                    stats["invalid job_info"] += 1
                    continue

                job_id = job_info["job_id"]
                user_id = job_info["user_id"]

                try:
                    application_id = job_info.get("application_id")
                    status = job_info.get("status")

                    if not application_id:
                        # No application_id typically means that job hasn't been started yet.
                        created = job_info.get("created")
                        if created:
                            age = dt.datetime.utcnow() - rfc3339.parse_datetime(created)
                        else:
                            age = "unknown"
                        # TODO: handle very old, non-started jobs? E.g. mark as error?
                        _log.info(
                            f"Skipping job without application_id: {job_id=}, {created=}, {age=}, {status=}",
                            extra={"job_id": job_id, "user_id": user_id}
                        )
                        stats[f"skip due to no application_id ({status=})"] += 1
                        continue

                    self._sync_job_status(
                        job_id=job_id,
                        user_id=user_id,
                        application_id=application_id,
                        job_info=job_info,
                        zk_job_registry=zk_job_registry,
                        stats=stats,
                    )
                except Exception as e:
                    _log.exception(
                        f"Failed status sync for {job_id=}: unexpected {type(e).__name__}: {e}",
                        extra={"job_id": job_id, "user_id": user_id},
                    )
                    stats["failed sync"] += 1
                    if fail_fast:
                        raise

    def _sync_job_status(
        self,
        job_id: str,
        user_id: str,
        application_id: str,
        job_info: dict,
        # TODO #236/#498/#632 make ZkJobRegistry optional
        zk_job_registry: ZkJobRegistry,
        stats: collections.Counter,
    ):
        """Sync job status for a single job"""
        # Local logger with default `extra`
        log = logging.LoggerAdapter(_log, extra={"job_id": job_id, "user_id": user_id})

        assert application_id == job_info.get("application_id")
        previous_status = job_info.get("status")
        log.debug(
            f"About to sync status for {job_id=} {user_id=} {application_id=} {previous_status=}"
        )
        stats[f"job with {previous_status=}"] += 1

        try:
            stats["get metadata attempt"] += 1
            job_metadata: _JobMetadata = self._app_state_getter.get_job_metadata(
                job_id=job_id, user_id=user_id, app_id=application_id
            )
            stats["new metadata"] += 1
        except AppNotFound:
            log.warning(f"App not found: {job_id=} {application_id=}", exc_info=True)
            # TODO: handle status setting generically with logic below (e.g. dummy job_metadata)?
            zk_job_registry.set_status(job_id, user_id, JOB_STATUS.ERROR)
            with ElasticJobRegistry.just_log_errors("job_tracker app not found"):
                if self._elastic_job_registry:
                    # TODO: also set started/finished, exception/error info ...
                    self._elastic_job_registry.set_status(
                        job_id=job_id, status=JOB_STATUS.ERROR
                    )
            stats["app not found"] += 1
            return

        if previous_status != job_metadata.status:
            log.info(
                f"job {job_id}: status change from {previous_status} to {job_metadata.status}",
            )
            stats["status change"] += 1
            stats[f"status change {previous_status!r} -> {job_metadata.status!r}"] += 1
        else:
            stats["status same"] += 1
            stats[f"status same {job_metadata.status!r}"] += 1

        if job_metadata.status in {
            JOB_STATUS.FINISHED,
            JOB_STATUS.ERROR,
            JOB_STATUS.CANCELED,
        }:
            stats[f"reached final status {job_metadata.status}"] += 1
            result_metadata = self._batch_jobs.load_results_metadata(job_id, user_id)

            zk_job_registry.remove_dependencies(job_id, user_id)

            # there can be duplicates if batch processes are recycled
            dependency_sources = list(set(get_deletable_dependency_sources(job_info)))

            if dependency_sources:
                async_task.schedule_delete_batch_process_dependency_sources(
                    job_id, user_id, dependency_sources
                )

            area = deep_get(result_metadata, 'area', 'value', default=None)

            # TODO: make this usage report handling/logging more generic?
            sentinelhub_processing_units = (
                result_metadata.get("usage", {})
                .get("sentinelhub", {})
                .get("value", 0.0)
            )

            sentinelhub_batch_processing_units = float(ZkJobRegistry.get_dependency_usage(job_info) or Decimal("0.0"))

            costs_details = CostsDetails(
                job_id=job_info["job_id"],
                user_id=job_info["user_id"],
                execution_id=job_info["application_id"],
                # TODO #610 this is just part of a temporary migration path, to be cleaned up when not necessary anymore. Just use `job_status`
                app_state_etl_api_deprecated=self._app_state_getter.app_state_to_etl_state(job_metadata.app_state),
                job_status=job_metadata.status,
                area_square_meters=area,
                job_title=job_info.get("title"),
                start_time=job_metadata.start_time,
                finish_time=job_metadata.finish_time,
                cpu_seconds=job_metadata.usage.cpu_seconds,
                mb_seconds=job_metadata.usage.mb_seconds,
                sentinelhub_processing_units=(sentinelhub_processing_units + sentinelhub_batch_processing_units)
                or None,
                unique_process_ids=result_metadata.get("unique_process_ids", []),
                job_options=job_info.get("job_options"),
            )

            try:
                job_costs = self._job_costs_calculator.calculate_costs(costs_details)
                _log.debug(f"job_costs: calculated {job_costs}")
                stats["job_costs: calculated"] += 1
                stats[f"job_costs: nonzero={isinstance(job_costs, float) and job_costs>0}"] += 1
                # TODO: skip patching the job znode and read from this file directly?
            except Exception as e:
                log.exception(f"Failed to calculate job costs: {e}")
                stats["job_costs: failed"] += 1
                job_costs = None

            usage = dict_merge_recursive(job_metadata.usage.to_dict(), result_metadata.get("usage", {}))
            zk_job_registry.patch(job_id, user_id, **dict(result_metadata, costs=job_costs, usage=usage))

            with ElasticJobRegistry.just_log_errors(
                    f"job_tracker {job_metadata.status=} from {type(self._app_state_getter).__name__}"
            ):
                if self._elastic_job_registry:
                    self._elastic_job_registry.set_usage(job_id, job_costs, dict(usage))

        datetime_formatter = Rfc3339(propagate_none=True)

        zk_job_registry.patch(
            job_id=job_id,
            user_id=user_id,
            status=job_metadata.status,
            started=datetime_formatter.datetime(job_metadata.start_time),
            finished=datetime_formatter.datetime(job_metadata.finish_time),
        )
        with ElasticJobRegistry.just_log_errors(
            f"job_tracker {job_metadata.status=} from {type(self._app_state_getter).__name__}"
        ):
            if self._elastic_job_registry:
                self._elastic_job_registry.set_status(
                    job_id=job_id,
                    status=job_metadata.status,
                    started=datetime_formatter.datetime(job_metadata.start_time),
                    finished=datetime_formatter.datetime(job_metadata.finish_time),
                )


class CliApp:
    def main(self, *, args: Optional[List[str]] = None):

        args = self.parse_cli_args(args=args)

        self.setup_logging(
            basic_logging=args.basic_logging,
            rotating_file=(
                "logs/job_tracker_python.log"
                if not ConfigParams().is_kube_deploy and Path("logs").is_dir()
                else None
            ),
        )

        _log.info(f"job_tracker_v2 cli {args=}")
        _log.info(f"job_tracker_v2 cli {ConfigParams()=!s}")
        package_versions = openeo_driver.utils.get_package_versions(
            ["openeo", "openeo_driver", "openeo-geopyspark", "kubernetes"]
        )
        _log.info(f"job_tracker_v2 cli {package_versions=}")

        with TimingLogger(logger=_log.info, title=f"job_tracker_v2 cli"):
            try:
                # ZooKeeper Job Registry
                zk_root_path = args.zk_job_registry_root_path
                _log.info(f"Using {zk_root_path=}")
                # TODO #236/#498/#632 make ZkJobRegistry optional
                zk_job_registry = ZkJobRegistry(root_path=zk_root_path)

                requests_session = requests_with_retry(total=3, backoff_factor=2)

                # Elastic Job Registry (EJR)
                elastic_job_registry = get_elastic_job_registry(requests_session)

                # YARN or Kubernetes?
                app_cluster = args.app_cluster
                if app_cluster == "auto":
                    # TODO: eliminate (need for) auto-detection.
                    app_cluster = "k8s" if ConfigParams().is_kube_deploy else "yarn"
                if app_cluster == "yarn":
                    app_state_getter = YarnStatusGetter(
                        yarn_api_base_url=ConfigParams().yarn_rest_api_base_url,
                        auth=requests_gssapi.HTTPSPNEGOAuth(
                            mutual_authentication=requests_gssapi.REQUIRED
                        ),
                    )
                elif app_cluster == "k8s":
                    app_state_getter = K8sStatusGetter(kube_client(),
                                                       Prometheus(get_backend_config().prometheus_api))
                else:
                    raise ValueError(app_cluster)

                etl_api = get_etl_api(requests_session=requests_session)
                job_costs_calculator = EtlApiJobCostsCalculator(etl_api=etl_api)

                job_tracker = JobTracker(
                    app_state_getter=app_state_getter,
                    zk_job_registry=zk_job_registry,
                    principal=args.principal,
                    keytab=args.keytab,
                    elastic_job_registry=elastic_job_registry,
                    job_costs_calculator=job_costs_calculator
                )
                job_tracker.update_statuses(fail_fast=args.fail_fast)
            except Exception as e:
                _log.exception(f"Failed job tracker run: {type(e).__name__}: {e}")
                raise

    def parse_cli_args(self, args: Optional[List[str]] = None) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="JobTracker from openeo-geopyspark-driver.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--principal",
            default="openeo@VGT.VITO.BE",
            help="Principal to be used to login to KDC",
        )
        parser.add_argument(
            "--keytab",
            default="openeo-deploy/mep/openeo.keytab",
            help="The full path to the file that contains the keytab for the principal",
        )

        parser.add_argument(
            "--fail-fast",
            action="store_true",
            default=False,
            help="Stop immediately on unexpected errors while tracking a certain job, instead of skipping to next job.",
        )
        parser.add_argument(
            "--app-cluster",
            choices=["yarn", "k8s", "auto"],
            default="auto",
            help="Application cluster to get job/app status from.",
        )
        parser.add_argument(
            "--basic-logging",
            action="store_true",
            help="Use basic logging on stderr instead of JSON formatted logs.",
        )
        parser.add_argument(
            "--zk-job-registry-root-path",
            default=ConfigParams().batch_jobs_zookeeper_root_path,
            help="ZooKeeper root path for the job registry",
        )
        # TODO: also allow setting zk_root_path through "env" setting (prod, dev, integration tests, ...)?

        return parser.parse_args(args=args)

    def setup_logging(
        self,
        *,
        basic_logging: bool = False,
        rotating_file: Optional[Union[str, Path]] = None,
    ):
        logging_config = get_logging_config(
            root_handlers=[LOG_HANDLER_STDERR_BASIC] if basic_logging else [LOG_HANDLER_STDERR_JSON],
            loggers={
                "openeo": {"level": "DEBUG"},
                "openeo_driver": {"level": "DEBUG"},
                "openeogeotrellis": {"level": "DEBUG"},
                _log.name: {"level": "DEBUG"},
            },
            context="job_tracker"
        )

        if rotating_file:
            # TODO: support this appending directly in get_logging_config
            logging_config["handlers"]["rotating_file"] = {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": str(rotating_file),
                "maxBytes": 10 * 1024 * 1024,
                "backupCount": 1,
                "formatter": "json",
            }
            logging_config["root"]["handlers"].append("rotating_file")

        setup_logging(logging_config, force=True)


if __name__ == "__main__":
    CliApp().main()
