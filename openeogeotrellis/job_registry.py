from __future__ import annotations
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, List, Dict, Callable, Union, Optional, Iterator, Tuple

from openeo.util import rfc3339, TimingLogger
from openeo_driver.backend import BatchJobMetadata, JobListing
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import (
    JOB_STATUS,
    JobRegistryInterface,
    JobDict,
    ejr_job_info_to_metadata,
)

# TODO: eliminate coupling of job_registry with openeogeotrellis.sentinel_hub?
#       It's actually only used for OG_BATCH_RESULTS_BUCKET, which could be a config?
from openeogeotrellis import sentinel_hub
from openeogeotrellis.configparams import ConfigParams

_log = logging.getLogger(__name__)


def get_deletable_dependency_sources(job_info: dict) -> List[str]:
    """Returns source locations (as URIs) of dependencies that we created ourselves and are therefore able to delete."""

    def sources(dependency: dict) -> Iterator[str]:
        if "partial_job_results_url" in dependency:
            return

        if "results_location" in dependency:
            yield dependency["results_location"]
        else:
            subfolder = dependency.get("subfolder") or dependency["batch_request_id"]
            yield f"s3://{sentinel_hub.OG_BATCH_RESULTS_BUCKET}/{subfolder}"

        if "assembled_location" in dependency:
            yield dependency["assembled_location"]

    return [source for dependency in (job_info.get("dependencies") or []) for source in sources(dependency)]


class InMemoryJobRegistry(JobRegistryInterface):
    """
    Simple in-memory implementation of JobRegistryInterface for testing/dummy purposes
    """
    # TODO move this implementation to openeo_python_driver

    def __init__(self):
        self.db: Dict[str, JobDict] = {}

    def create_job(
        self,
        *,
        process: dict,
        user_id: str,
        job_id: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        parent_id: Optional[str] = None,
        api_version: Optional[str] = None,
        job_options: Optional[dict] = None,
    ) -> JobDict:
        assert job_id not in self.db
        created = rfc3339.now_utc()
        self.db[job_id] = {
            "job_id": job_id,
            "user_id": user_id,
            "process": process,
            "title": title,
            "description": description,
            "application_id": None,
            "parent_id": parent_id,
            "status": JOB_STATUS.CREATED,
            "created": created,
            "updated": created,
            "api_version": api_version,
            "job_options": job_options,
        }
        return self.db[job_id]

    def get_job(self, job_id: str, *, user_id: Optional[str] = None) -> JobDict:
        job = self.db.get(job_id)

        if not job or (user_id is not None and job['user_id'] != user_id):
            raise JobNotFoundException(job_id=job_id)

        return job

    def delete_job(self, job_id: str, *, user_id: Optional[str] = None, verify_deletion: bool = True) -> None:
        self.get_job(job_id=job_id, user_id=user_id)  # will raise on job not found
        del self.db[job_id]

    def _update(self, job_id: str, **kwargs) -> JobDict:
        assert job_id in self.db
        if "usage" in kwargs:
            kwargs["input_pixel"] = kwargs["usage"].get("input_pixel", {}).get("value", 0.0)
        self.db[job_id].update(**kwargs)
        return self.db[job_id]

    def set_status(
        self,
        job_id: str,
        *,
        user_id: Optional[str] = None,
        status: str,
        updated: Optional[str] = None,
        started: Optional[str] = None,
        finished: Optional[str] = None,
    ) -> None:
        self._update(
            job_id=job_id,
            status=status,
            updated=rfc3339.datetime(updated) if updated else rfc3339.now_utc(),
        )
        if started:
            self._update(job_id=job_id, started=rfc3339.datetime(started))
        if finished:
            self._update(job_id=job_id, finished=rfc3339.datetime(finished))

    def set_dependencies(
        self, job_id: str, *, user_id: Optional[str] = None, dependencies: List[Dict[str, str]]
    ) -> None:
        self._update(job_id=job_id, dependencies=dependencies)

    def remove_dependencies(self, job_id: str, *, user_id: Optional[str] = None) -> None:
        self._update(job_id=job_id, dependencies=None, dependency_status=None)

    def set_dependency_status(self, job_id: str, *, user_id: Optional[str] = None, dependency_status: str) -> None:
        self._update(job_id=job_id, dependency_status=dependency_status)

    def set_dependency_usage(self, job_id: str, *, user_id: Optional[str] = None, dependency_usage: Decimal) -> None:
        self._update(job_id, dependency_usage=str(dependency_usage))

    def set_proxy_user(self, job_id: str, *, user_id: Optional[str] = None, proxy_user: str) -> None:
        self._update(job_id=job_id, proxy_user=proxy_user)

    def set_application_id(self, job_id: str, *, user_id: Optional[str] = None, application_id: str) -> None:
        self._update(job_id=job_id, application_id=application_id)

    def set_results_metadata(
        self,
        job_id: str,
        *,
        user_id: Optional[str] = None,
        costs: Optional[float],
        usage: dict,
        results_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._update(job_id=job_id, costs=costs, usage=usage, results_metadata=results_metadata)

    def set_results_metadata_uri(
        self, job_id: str, *, user_id: Optional[str] = None, results_metadata_uri: str
    ) -> None:
        self._update(job_id=job_id, results_metadata_uri=results_metadata_uri)

    def list_user_jobs(
        self,
        user_id: str,
        *,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        request_parameters: Optional[dict] = None,
        # TODO #959 settle on returning just `JobListing` and eliminate other options/code paths.
    ) -> Union[JobListing, List[JobDict]]:
        jobs = [job for job in self.db.values() if job["user_id"] == user_id]
        if limit:
            pagination_param = "page"
            page_number = int((request_parameters or {}).get(pagination_param, 0))
            assert page_number >= 0 and limit >= 1
            if len(jobs) > (page_number + 1) * limit:
                next_parameters = {"limit": limit, pagination_param: page_number + 1}
            else:
                next_parameters = None
            jobs = jobs[page_number * limit : (page_number + 1) * limit]
            jobs = JobListing(
                jobs=[ejr_job_info_to_metadata(j, full=False) for j in jobs],
                next_parameters=next_parameters,
            )
        return jobs

    def list_active_jobs(
        self,
        *,
        fields: Optional[List[str]] = None,
        max_age: Optional[int] = None,
        max_updated_ago: Optional[int] = None,
        require_application_id: bool = False,
    ) -> List[JobDict]:
        active = [JOB_STATUS.CREATED, JOB_STATUS.QUEUED, JOB_STATUS.RUNNING]
        # TODO: implement support for max_age, max_updated_ago, fields
        return [
            job
            for job in self.db.values()
            if job["status"] in active and (not require_application_id or job.get("application_id") is not None)
        ]


class DoubleJobRegistryException(Exception):
    pass


class DoubleJobRegistry:  # TODO: extend JobRegistryInterface?
    """
    Adapter to delegate job registry operations to an ElasticJobRegistry.

    Previously also supported a ZkJobRegistry as a secondary store; that has been removed.
    Kept as a thin wrapper for API compatibility.
    """

    _log = logging.getLogger(f"{__name__}.double")

    def __init__(
        self,
        *,
        zk_job_registry_factory=None,  # Kept for backwards compatibility, ignored
        elastic_job_registry: Optional[JobRegistryInterface] = None,
    ):
        self.elastic_job_registry = elastic_job_registry

    def __repr__(self):
        return f"<{type(self).__name__} {type(self.elastic_job_registry).__name__}>"

    def __enter__(self):
        self._log.debug(f"Context enter {self!r}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._log.debug(f"Context exit {self!r} ({exc_type=})")

    def create_job(
        self,
        *,
        process: dict,
        user_id: str,
        job_id: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        parent_id: Optional[str] = None,
        api_version: Optional[str] = None,
        job_options: Optional[dict] = None,
    ) -> JobDict:
        if not job_id:
            job_id = JobRegistryInterface.generate_job_id()

        if self.elastic_job_registry:
            return self.elastic_job_registry.create_job(
                process=process,
                user_id=user_id,
                job_id=job_id,
                title=title,
                description=description,
                api_version=api_version,
                job_options=job_options,
                parent_id=parent_id,
            )
        raise DoubleJobRegistryException(f"No job registry configured to register {job_id=}")

    def get_job(self, job_id: str, *, user_id: Optional[str] = None) -> JobDict:
        if self.elastic_job_registry:
            return self.elastic_job_registry.get_job(job_id=job_id, user_id=user_id)
        raise JobNotFoundException(job_id=job_id)

    def get_job_metadata(self, job_id: str, user_id: str) -> BatchJobMetadata:
        job_info = self.get_job(job_id=job_id, user_id=user_id)
        return ejr_job_info_to_metadata(job_info)

    def set_status(
        self,
        job_id: str,
        *,
        user_id: Optional[str] = None,
        status: str,
        updated: Optional[str] = None,
        started: Optional[str] = None,
        finished: Optional[str] = None,
    ) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_status(
                job_id=job_id, user_id=user_id, status=status, updated=updated, started=started, finished=finished
            )

    def delete_job(self, job_id: str, *, user_id: Optional[str] = None, verify_deletion: bool = True) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.delete_job(job_id=job_id, user_id=user_id, verify_deletion=verify_deletion)

    def set_dependencies(
        self, job_id: str, *, user_id: Optional[str] = None, dependencies: List[Dict[str, str]]
    ) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_dependencies(job_id=job_id, user_id=user_id, dependencies=dependencies)

    def remove_dependencies(self, job_id: str, *, user_id: Optional[str] = None) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.remove_dependencies(job_id=job_id, user_id=user_id)

    def set_dependency_status(self, job_id: str, *, user_id: Optional[str] = None, dependency_status: str) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_dependency_status(
                job_id=job_id, user_id=user_id, dependency_status=dependency_status
            )

    def set_dependency_usage(self, job_id: str, *, user_id: Optional[str] = None, dependency_usage: Decimal) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_dependency_usage(
                job_id=job_id, user_id=user_id, dependency_usage=dependency_usage
            )

    def set_proxy_user(self, job_id: str, *, user_id: Optional[str] = None, proxy_user: str) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_proxy_user(job_id=job_id, user_id=user_id, proxy_user=proxy_user)

    def set_application_id(self, job_id: str, *, user_id: Optional[str] = None, application_id: str) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_application_id(job_id=job_id, user_id=user_id, application_id=application_id)

    def set_results_metadata_uri(self, job_id: str, *, user_id: Optional[str] = None, results_metadata_uri: str):
        if self.elastic_job_registry:
            self.elastic_job_registry.set_results_metadata_uri(
                job_id=job_id, user_id=user_id, results_metadata_uri=results_metadata_uri
            )

    def get_user_jobs(
        self,
        user_id: str,
        *,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        request_parameters: Optional[dict] = None,
        # TODO  # 959 settle on returning just `JobListing` and eliminate other options/code paths.
    ) -> Union[List[BatchJobMetadata], JobListing]:
        if self.elastic_job_registry:
            ejr_jobs = self.elastic_job_registry.list_user_jobs(
                user_id=user_id,
                fields=fields,
                limit=limit,
                request_parameters=request_parameters,
            )
            # TODO #959 Settle on just handling JobListing and drop other legacy code path
            if isinstance(ejr_jobs, list):
                ejr_jobs = [ejr_job_info_to_metadata(j, full=False) for j in ejr_jobs]
            self._log.info(
                f"DoubleJobRegistry.get_user_jobs({user_id=}) ejr_jobs={len(ejr_jobs) if ejr_jobs is not None else None}"
            )
            return ejr_jobs or []
        return []

    def list_active_jobs(
        self,
        *,
        fields: Optional[List[str]] = None,
        max_age: Optional[int] = None,
        max_updated_ago: Optional[int] = None,
        require_application_id: bool = False,
    ) -> List[JobDict]:
        if self.elastic_job_registry:
            return self.elastic_job_registry.list_active_jobs(
                fields=fields,
                max_age=max_age,
                max_updated_ago=max_updated_ago,
                require_application_id=require_application_id,
            )
        return []

    def set_results_metadata(
        self,
        job_id: str,
        *,
        user_id: Optional[str] = None,
        costs: Optional[float],
        usage: dict,
        results_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.elastic_job_registry:
            self.elastic_job_registry.set_results_metadata(
                job_id=job_id, user_id=user_id, costs=costs, usage=usage, results_metadata=results_metadata
            )


class EagerlyK8sTrackingInMemoryJobRegistry(InMemoryJobRegistry):
    """
    Calls k8s API for application status eagerly, avoiding the need for a separate job_tracker process.
    """

    STATUS_ONGOING = {JOB_STATUS.CREATED, JOB_STATUS.QUEUED, JOB_STATUS.RUNNING}

    def __init__(self, kubernetes_api):
        super().__init__()
        self._kubernetes_api = kubernetes_api

    def get_job(self, job_id: str, *, user_id: Optional[str] = None) -> JobDict:
        import kubernetes.client.exceptions

        job = super().get_job(job_id=job_id, user_id=user_id)

        if job["status"] not in self.STATUS_ONGOING:
            _log.debug(f"Job is done with status {job['status']}, skipping k8s status check")
            return job

        application_id = job.get("application_id")
        if application_id:
            try:
                new_status = self._get_openeo_status(application_id)
                _log.debug(f"App {application_id} status: {new_status}")

                self.set_status(job_id, user_id=user_id, status=new_status)
                job["status"] = new_status
            except kubernetes.client.exceptions.ApiException as e:
                if e.status == 404:  # app is gone
                    if job["status"] in self.STATUS_ONGOING:
                        # mark as done to avoid endless polling
                        _log.warning(f"App {application_id} not found, marking job as done", exc_info=True)
                        new_status = JOB_STATUS.ERROR
                        self.set_status(job_id, user_id=user_id, status=new_status)
                        job["status"] = new_status
                    else:
                        # retain old (done) status
                        _log.warning(f"App {application_id} not found, retaining status {job['status']}", exc_info=True)
                else:
                    raise

        return job

    def _get_openeo_status(self, application_id: str) -> str:
        # TODO: reduce code duplication with openeogeotrellis.job_tracker_v2.K8sStatusGetter?
        from openeogeotrellis.integrations.kubernetes import K8S_SPARK_APP_STATE, k8s_state_to_openeo_job_status

        metadata = self._kubernetes_api.get_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            # TODO: this namespace should come from job metadata, not config
            namespace=ConfigParams().pod_namespace,
            plural="sparkapplications",
            name=application_id,
        )

        app_state = metadata["status"]["applicationState"]["state"] if "status" in metadata else K8S_SPARK_APP_STATE.NEW
        return k8s_state_to_openeo_job_status(app_state)
