from pathlib import Path
import logging
from decimal import Decimal
from logging.handlers import RotatingFileHandler
import subprocess
import sys
from subprocess import CalledProcessError
from typing import Callable, Union, Optional
from collections import namedtuple
from datetime import datetime
import re
import requests

import kazoo.client

import openeogeotrellis.backend
from openeo.util import date_to_rfc3339, deep_get, url_join
from openeo.rest.auth.oidc import OidcClientCredentialsAuthenticator, OidcClientInfo, OidcProviderInfo
from pythonjsonlogger.jsonlogger import JsonFormatter

from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import JOB_STATUS, ElasticJobRegistry
from openeo_driver.util.logging import JSON_LOGGER_DEFAULT_FORMAT
from openeogeotrellis.integrations.etl_api import EtlApi
from openeogeotrellis.integrations.kubernetes import (
    kube_client,
    k8s_job_name,
    k8s_state_to_openeo_job_status, K8S_SPARK_APP_STATE,
)
from openeogeotrellis.integrations.yarn import yarn_state_to_openeo_job_status
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.backend import GpsBatchJobs, get_or_build_elastic_job_registry
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.vault import Vault
from openeogeotrellis import async_task


# Note: hardcoded logger name as this script is executed directly which kills the usefulness of `__name__`.
_log = logging.getLogger("openeogeotrellis.job_tracker")


# TODO: current implementation mixes YARN and Kubernetes logic. Instead use composition/inheritance for better separation of concerns?
#       Especially because the job registry storage will also get different options: legacy ZooKeeper and ElasticJobRegistry (and maybe even a simple in-memory option)


class UnknownYarnApplicationException(ValueError):
    pass


class JobTracker:

    _YarnStatus = namedtuple('YarnStatus', ['state', 'final_state', 'start_time', 'finish_time',
                                            'aggregate_resource_allocation'])
    _KubeStatus = namedtuple('KubeStatus', ['state', 'start_time', 'finish_time'])

    # TODO: get this url from config and make this an instance attribute.
    _KUBECOST_URL = "http://kubecost.kube-dev.vgt.vito.be/"

    def __init__(
        self,
        job_registry: Callable[[], ZkJobRegistry],
        principal: str,
        keytab: str,
        output_root_dir: Optional[Union[str, Path]] = None,
        elastic_job_registry: Optional[ElasticJobRegistry] = None,
        etl_api: Optional[EtlApi] = None,
        etl_api_access_token: Optional[str] = None
    ):
        # TODO: avoid is_kube_deploy anti-pattern
        self._kube_mode = ConfigParams().is_kube_deploy

        if not self._kube_mode:
            if etl_api is None:
                raise ValueError(etl_api)
            if etl_api_access_token is None:
                raise ValueError(etl_api_access_token)

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
        self._etl_api = etl_api
        self._etl_api_access_token = etl_api_access_token

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

                    # TODO: application_id is not used/necessary for Kube
                    if application_id:
                        try:
                            if self._kube_mode:
                                from openeogeotrellis.utils import s3_client, download_s3_dir
                                state, start_time, finish_time = JobTracker._kube_status(job_id, user_id)

                                new_status = k8s_state_to_openeo_job_status(state)

                                registry.patch(job_id, user_id,
                                               status=new_status,
                                               started=start_time,
                                               finished=finish_time)
                                with ElasticJobRegistry.just_log_errors(f"job_tracker {new_status=} from K8s"):
                                    if self._elastic_job_registry:
                                        self._elastic_job_registry.set_status(
                                            job_id=job_id,
                                            status=new_status,
                                            started=start_time,
                                            finished=finish_time,
                                        )

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

                                new_status = yarn_state_to_openeo_job_status(
                                    state, final_state
                                )

                                registry.patch(job_id, user_id,
                                               status=new_status,
                                               started=JobTracker._to_serializable_datetime(start_time),
                                               finished=JobTracker._to_serializable_datetime(finish_time),
                                               memory_time_megabyte_seconds=memory_time_megabyte_seconds,
                                               cpu_time_seconds=cpu_time_seconds)
                                with ElasticJobRegistry.just_log_errors(f"job_tracker {new_status=} from YARN"):
                                    if self._elastic_job_registry:
                                        self._elastic_job_registry.set_status(
                                            job_id=job_id,
                                            status=new_status,
                                            started=JobTracker._to_serializable_datetime(
                                                start_time
                                            ),
                                            finished=JobTracker._to_serializable_datetime(
                                                finish_time
                                            ),
                                    )

                                if current_status != new_status:
                                    _log.info("changed job %s status from %s to %s" %
                                              (job_id, current_status, new_status), extra={'job_id': job_id})

                                if final_state != "UNDEFINED":
                                    result_metadata = self._batch_jobs.get_results_metadata(job_id, user_id)
                                    # TODO: skip patching the job znode and read from this file directly?

                                    sentinelhub_processing_units = (result_metadata.get("usage", {})
                                                                    .get("sentinelhub", {}).get("value", 0.0))

                                    sentinelhub_batch_processing_units = (ZkJobRegistry.get_dependency_usage(job_info)
                                                                          or Decimal("0.0"))

                                    job_title = job_info.get("title")

                                    resource_costs_in_credits = self._etl_api.log_resource_usage(
                                        batch_job_id=job_id,
                                        title=job_title,
                                        application_id=application_id,
                                        user_id=user_id,
                                        started_ms=float(start_time),
                                        finished_ms=float(finish_time),
                                        state=state,
                                        status=final_state,
                                        cpu_seconds=cpu_time_seconds,
                                        mb_seconds=memory_time_megabyte_seconds,
                                        duration_ms=float(finish_time) - float(start_time),
                                        sentinel_hub_processing_units=float(Decimal(sentinelhub_processing_units) +
                                                                            sentinelhub_batch_processing_units),
                                        access_token=self._etl_api_access_token)

                                    area = deep_get(result_metadata, 'area', 'value', default=None)

                                    added_value_costs_in_credits = sum(self._etl_api.log_added_value(
                                        batch_job_id=job_id,
                                        title=job_title,
                                        application_id=application_id,
                                        user_id=user_id,
                                        started_ms=float(start_time),
                                        finished_ms=float(finish_time),
                                        process_id=process_id,
                                        square_meters=float(area) if area is not None else 0.0,
                                        access_token=self._etl_api_access_token) for process_id in
                                                                       result_metadata.get('unique_process_ids',
                                                                                           []))

                                    registry.patch(job_id, user_id, **dict(
                                        result_metadata,
                                        costs=resource_costs_in_credits + added_value_costs_in_credits))

                                    registry.remove_dependencies(job_id, user_id)

                                    # there can be duplicates if batch processes are recycled
                                    dependency_sources = list(set(ZkJobRegistry.get_dependency_sources(job_info)))

                                    if dependency_sources:
                                        async_task.schedule_delete_batch_process_dependency_sources(
                                            job_id, user_id, dependency_sources)

                                    registry.mark_done(job_id, user_id)

                                    _log.info("marked %s as done" % job_id, extra={
                                        'job_id': job_id,
                                        'area': result_metadata.get('area'),
                                        'unique_process_ids': result_metadata.get('unique_process_ids'),
                                        'cpu_time_seconds': cpu_time_seconds,
                                        'sentinelhub': float(Decimal(sentinelhub_processing_units) +
                                                             sentinelhub_batch_processing_units)
                                    })
                        except UnknownYarnApplicationException:
                            # TODO eliminate this whole try-except (but not now to keep diff simple)
                            raise
                except Exception as e:
                    # TODO: option for strict mode (fail fast instead of just warnings)?
                    _log.warning(
                        f"Failed status update of {job_id=}: {type(e).__name__}: {e}",
                        exc_info=True,
                        extra={"job_id": job_id},
                    )
                    # TODO: this looks risky: an unexpected issue in JobTracker logic
                    #  will cause a job (or possibly all running jobs) to be marked as "done" with status "error"?
                    if job_id and user_id:
                        registry.set_status(job_id, user_id, JOB_STATUS.ERROR)

                        with ElasticJobRegistry.just_log_errors(f"job_tracker flag error"):
                            if self._elastic_job_registry:
                                # TODO: also set started/finished, exception/error info ...
                                self._elastic_job_registry.set_status(
                                    job_id=job_id, status=JOB_STATUS.ERROR
                                )

    def get_kube_usage(self, job_id, user_id) -> Union[dict, None]:
        try:
            url = url_join(self._KUBECOST_URL, "/model/allocation")
            namespace = "spark-jobs"
            window = "5d"
            pod = k8s_job_name(job_id=job_id, user_id=user_id) + "*"
            params = (
                ('aggregate', 'namespace'),
                ('filterNamespaces', namespace),
                ('filterPods', pod),
                ('window', window),
                ('accumulate', 'true'),
            )

            total_cost = requests.get(url, params=params).json()
            if total_cost['code'] == 200:
                if namespace in total_cost['data'][0]:
                    cost = total_cost['data'][0][namespace]
                    # TODO: need to iterate through "data" list?
                    _log.info(f"Successfully retrieved total cost {cost}")
                    usage = {}
                    usage["cpu"] = {"value": cost["cpuCoreHours"], "unit": "cpu-hours"}
                    usage["memory"] = {
                        "value": cost["ramByteHours"] / (1024 * 1024),
                        "unit": "mb-hours",
                    }

                    return usage
                else:
                    _log.error(f"Unable to retrieve job cost {total_cost}")
            else:
                # TODO: better error logging?
                _log.error(f"Unable to retrieve job cost {total_cost}")
        except Exception:
            _log.error(f"error while handling creo usage", exc_info=True, extra={'job_id': job_id})

    @staticmethod
    def _kube_status(job_id: str, user_id: str) -> '_KubeStatus':
        api_instance = kube_client()
        status = api_instance.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace="spark-jobs",
            plural="sparkapplications",
            name=k8s_job_name(job_id=job_id, user_id=user_id),
        )
        if 'status' in status:
            return JobTracker._KubeStatus(
                status['status']['applicationState']['state'],
                status['status']['lastSubmissionAttemptTime'],
                status['status']['terminationTime']
            )
        else:
            return JobTracker._KubeStatus(K8S_SPARK_APP_STATE.NEW, None, None)

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
                raise UnknownYarnApplicationException(stdout)
            else:
                raise

    @staticmethod
    def _parse_resource_allocation(aggregate_resource_allocation) -> (int, int):
        match = re.fullmatch(r"^(\d+) MB-seconds, (\d+) vcore-seconds$", aggregate_resource_allocation)

        return int(match.group(1)), int(match.group(2)) if match else (None, None)


    @staticmethod
    def _to_serializable_datetime(epoch_millis: str) -> Union[str, None]:
        # TODO: move this parsing to `_yarn_status`
        if epoch_millis == "0":
            return None

        utc_datetime = datetime.utcfromtimestamp(int(epoch_millis) / 1000)
        return date_to_rfc3339(utc_datetime)


def get_etl_api_access_token(principal: str, keytab: str):
    vault = Vault(ConfigParams().vault_addr)
    vault_token = vault.login_kerberos(principal, keytab)

    etl_api_credentials = vault.get_etl_api_credentials(vault_token)
    oidc_provider = OidcProviderInfo(issuer=ConfigParams().etl_api_oidc_issuer)

    client_info = OidcClientInfo(
        provider=oidc_provider,
        client_id=etl_api_credentials.client_id,
        client_secret=etl_api_credentials.client_secret,
    )

    authenticator = OidcClientCredentialsAuthenticator(client_info)
    return authenticator.get_tokens().access_token


def main():
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
    parser.add_argument("--keytab", default="openeo.keytab",
                        help="The full path to the file that contains the keytab for the principal")
    args = parser.parse_args()

    try:
        # the assumption here is that a token lifetime of 5 minutes is long enough for a JobTracker run
        etl_api_access_token = None if ConfigParams().is_kube_deploy else get_etl_api_access_token(args.principal,
                                                                                                   args.keytab)

        with EtlApi(ConfigParams().etl_api) as etl_api:
            job_tracker = JobTracker(ZkJobRegistry, args.principal, args.keytab,
                                     etl_api=etl_api, etl_api_access_token=etl_api_access_token)
            job_tracker.update_statuses()
    except JobNotFoundException as e:
        _log.error(e, exc_info=True, extra={'job_id': e.job_id})
    except Exception as e:
        _log.error(e, exc_info=True)
        raise e


if __name__ == '__main__':
    main()
