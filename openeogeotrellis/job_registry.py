import datetime as dt
import json
import logging
import random
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Callable, Union, Optional, Iterator

import kazoo
import kazoo.exceptions
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import ZnodeStat

from openeo.util import rfc3339, dict_no_none, TimingLogger
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import (
    JOB_STATUS,
    JobRegistryInterface,
    JobDict,
)
from openeo_driver.util.logging import just_log_exceptions
from openeogeotrellis import sentinel_hub
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.testing import KazooClientMock
from openeogeotrellis.utils import StatsReporter

_log = logging.getLogger(__name__)


class ZkJobRegistry:
    # TODO: improve encapsulation
    def __init__(
        self,
        root_path: Optional[str] = None,
        zk_client: Union[str, KazooClient, KazooClientMock, None] = None,
    ):
        self._root = (
            root_path or ConfigParams().batch_jobs_zookeeper_root_path
        ).rstrip("/")
        _log.debug(f"Using batch job zk root path {self._root}")
        if zk_client is None:
            zk_client = KazooClient(hosts=",".join(ConfigParams().zookeepernodes))
        elif isinstance(zk_client, str):
            zk_client = KazooClient(hosts=zk_client)
        self._zk = zk_client

    def ensure_paths(self):
        # TODO: just do this automatically in __init__?
        #       Only worthwhile if we first can eliminate ad-hoc ZkJobRegistry() instantiation
        #       and reuse/pass around a single instance. See #313.
        #       Or do this only automatically before first write operation from an instance?
        self._zk.ensure_path(self._ongoing())
        self._zk.ensure_path(self._done())

    def register(
            self, job_id: str, user_id: str, api_version: str, specification: dict,
            title: str = None, description: str = None
    ) -> dict:
        """Registers a to-be-run batch job."""
        # TODO: use `BatchJobMetadata` instead of free form dict here?
        job_info = {
            "job_id": job_id,
            "user_id": user_id,
            "status": JOB_STATUS.CREATED,
            # TODO: move api_Version into specification?
            'api_version': api_version,
            # TODO: why json-encoding `specification` when the whole job_info dict will be json-encoded anyway?
            "specification": json.dumps(specification),
            "application_id": None,
            "created": rfc3339.utcnow(),
            "updated": rfc3339.utcnow(),
            "title": title,
            "description": description,
        }
        self._create(job_info)
        return job_info

    def set_application_id(self, job_id: str, user_id: str, application_id: str) -> None:
        """Updates a registered batch job with its Spark application ID."""

        self.patch(job_id, user_id, application_id=application_id)

    def set_status(
        self, job_id: str, user_id: str, status: str, auto_mark_done: bool = True
    ) -> None:
        """Updates a registered batch job with its status. Additionally, updates its "updated" property."""
        self.patch(
            job_id,
            user_id,
            status=status,
            updated=rfc3339.utcnow(),
            auto_mark_done=auto_mark_done,
        )
        _log.debug("batch job {j} -> {s}".format(j=job_id, s=status))

    def set_dependency_status(self, job_id: str, user_id: str, dependency_status: str) -> None:
        self.patch(job_id, user_id, dependency_status=dependency_status)
        _log.debug("batch job {j} dependency -> {s}".format(j=job_id, s=dependency_status))

    def set_dependency_usage(self, job_id: str, user_id: str, processing_units: Decimal):
        self.patch(job_id, user_id, dependency_usage=str(processing_units))

    @staticmethod
    def get_dependency_usage(job_info: dict) -> Optional[Decimal]:
        usage = job_info.get('dependency_usage')
        return Decimal(usage) if usage is not None else None

    def set_dependencies(self, job_id: str, user_id: str, dependencies: List[Dict[str, str]]):
        self.patch(job_id, user_id, dependencies=dependencies)

    def remove_dependencies(self, job_id: str, user_id: str):
        self.patch(job_id, user_id, dependencies=None, dependency_status=None)

    def patch(
        self, job_id: str, user_id: str, auto_mark_done: bool = True, **kwargs
    ) -> None:
        """Partially updates a registered batch job."""
        # TODO make this a private method to have cleaner API
        # TODO: is there still need to have `auto_mark_done` as public argument?
        job_info, version = self._read(job_id, user_id)
        self._update({**job_info, **kwargs}, version)

        if auto_mark_done and kwargs.get("status") in {
            JOB_STATUS.FINISHED,
            JOB_STATUS.ERROR,
            JOB_STATUS.CANCELED,
        }:
            self._mark_done(job_id=job_id, user_id=user_id)

    def _mark_done(self, job_id: str, user_id: str) -> None:
        """Marks a job as done (not to be tracked anymore)."""
        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id, user_id)

        source = self._ongoing(user_id, job_id)

        try:
            self._create(job_info, done=True)
        except NodeExistsError:
            pass

        self._zk.delete(source, version)

        _log.info(f"Marked {job_id} as done", extra={"job_id": job_id})

    def mark_ongoing(self, job_id: str, user_id: str) -> None:
        """Marks as job as ongoing (to be tracked)."""
        # TODO: can this method be made private or removed completely?

        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id, user_id, include_done=True)

        source = self._done(user_id, job_id)

        try:
            self._create(job_info, done=False)
        except NodeExistsError:
            pass

        self._zk.delete(source, version)

    def get_running_jobs(self, user_limit: Optional[int] = 1000) -> List[Dict]:
        """Returns a list of jobs that are currently not finished (should still be tracked)."""

        jobs = []

        with StatsReporter(name="get_running_jobs", report=_log) as stats, TimingLogger(
            title="get_running_jobs", logger=_log
        ):
            user_ids = self._zk.get_children(self._ongoing())

            for user_id in user_ids:
                stats["user_id"] += 1
                job_ids = self._zk.get_children(self._ongoing(user_id))

                if user_limit and len(job_ids) > user_limit:
                    _log.warning(
                        f"Extreme number of jobs found for {user_id=}: {len(job_ids)} > {user_limit}. "
                        f"Taking random sample of {user_limit} items."
                    )
                    stats["user_limit_exceeded"] += 1
                    stats["jobs_skipped"] += len(job_ids) - user_limit
                    job_ids = random.sample(job_ids, user_limit)

                if job_ids:
                    jobs.extend([self.get_job(job_id, user_id) for job_id in job_ids])
                    stats["user_id with jobs"] += 1
                    stats["job_ids"] += len(job_ids)
                else:
                    stats["user_id without jobs"] += 1

        return jobs

    def get_job(self, job_id: str, user_id: str) -> Dict:
        """Returns details of a job."""
        job_info, _ = self._read(job_id, user_id, include_done=True)
        return job_info

    def get_user_jobs(self, user_id: str) -> List[Dict]:
        """Returns details of all jobs for a specific user."""

        jobs = []

        try:
            done_job_ids = self._zk.get_children(self._done(user_id))
            jobs.extend([self.get_job(job_id, user_id) for job_id in done_job_ids])
        except NoNodeError:
            pass

        try:
            ongoing_job_ids = self._zk.get_children(self._ongoing(user_id))
            jobs.extend([self.get_job(job_id, user_id) for job_id in ongoing_job_ids])
        except NoNodeError:
            pass

        return jobs

    def __enter__(self) -> 'ZkJobRegistry':
        self._zk.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._zk.stop()
        self._zk.close()

    def delete(self, job_id: str, user_id: str) -> None:
        path = self._ongoing(user_id, job_id)

        try:
            self._zk.delete(path)
        except NoNodeError as e:
            e.args += (path,)
            path = self._done(user_id, job_id)

            try:
                self._zk.delete(path)
            except NoNodeError as e:
                e.args += (path,)
                raise JobNotFoundException(job_id) from e

    def get_all_jobs_before(
        self,
        upper: datetime,
        *,
        user_ids: Optional[List[str]] = None,
        include_ongoing: bool = True,
        include_done: bool = True,
        user_limit: Optional[int] = 1000,
    ) -> List[Dict]:
        def get_jobs_in(
            get_path: Callable[[Union[str, None], Union[str, None]], str],
            user_ids: Optional[List[str]] = None,
        ) -> List[Dict]:
            if user_ids is None:
                user_ids = self._zk.get_children(get_path(None, None))

            jobs_before = []

            for user_id in user_ids:
                path = get_path(user_id, None)
                try:
                    user_job_ids = self._zk.get_children(path)
                except NoNodeError:
                    _log.warning(
                        f"Not found (and no children) for {user_id=} ({path=})"
                    )
                    continue

                if user_limit and len(user_job_ids) > user_limit:
                    _log.warning(
                        f"User {user_id} has excessive number of jobs: {len(user_job_ids)}. "
                        f"Sampling down to {user_limit}."
                    )
                    user_job_ids = random.sample(user_job_ids, k=user_limit)

                for job_id in user_job_ids:
                    path = get_path(user_id, job_id)
                    data, stat = self._zk.get(path)
                    job_info = json.loads(data.decode())

                    updated = job_info.get('updated')
                    job_date = (rfc3339.parse_datetime(updated) if updated
                                else datetime.utcfromtimestamp(stat.last_modified))

                    if job_date < upper:
                        _log.debug("job {j}'s job_date {d} is before {u}".format(j=job_id, d=job_date, u=upper))
                        # TODO: not all job_info data is used, just pass the necessary bits?
                        jobs_before.append(job_info)

            return jobs_before

        # note: By default consider ongoing as well because that's where abandoned (never started) jobs are
        jobs = []
        if include_ongoing:
            jobs.extend(get_jobs_in(self._ongoing, user_ids=user_ids))
        if include_done:
            jobs.extend(get_jobs_in(self._done, user_ids=user_ids))
        return jobs

    def _create(self, job_info: Dict, done: bool = False) -> None:
        job_id = job_info['job_id']
        user_id = job_info['user_id']

        path = self._done(user_id, job_id) if done else self._ongoing(user_id, job_id)
        data = json.dumps(job_info).encode("utf-8")

        self._zk.create(path, data, makepath=True)

    def _read(self, job_id: str, user_id: str, include_done=False) -> (Dict, int):
        assert job_id, "Shouldn't be empty: job_id"
        assert user_id, "Shouldn't be empty: user_id"

        path = self._ongoing(user_id, job_id)

        try:
            data, stat = self._zk.get(path)
        except NoNodeError as e:
            e.args += (path,)
            if include_done:
                path = self._done(user_id, job_id)

                try:
                    data, stat = self._zk.get(path)
                except NoNodeError as e:
                    e.args += (path,)
                    raise JobNotFoundException(job_id) from e
            else:
                raise JobNotFoundException(job_id) from e

        return json.loads(data.decode()), stat.version

    def _update(self, job_info: Dict, version: int) -> None:
        job_id = job_info['job_id']
        user_id = job_info['user_id']

        path = self._ongoing(user_id, job_id)
        data = json.dumps(job_info).encode("utf-8")

        self._zk.set(path, data, version)

    def _ongoing(self, user_id: str = None, job_id: str = None) -> str:
        if job_id:
            return "{r}/ongoing/{u}/{j}".format(r=self._root, u=user_id, j=job_id)
        elif user_id:
            return "{r}/ongoing/{u}".format(r=self._root, u=user_id)

        return "{r}/ongoing".format(r=self._root)

    def _done(self, user_id: str = None, job_id: str = None) -> str:
        if job_id:
            return "{r}/done/{u}/{j}".format(r=self._root, u=user_id, j=job_id)
        elif user_id:
            return "{r}/done/{u}".format(r=self._root, u=user_id)

        return "{r}/done".format(r=self._root)

    def prune_empty_users(self, dry_run: bool = True):
        """
        Warning: this is a maintenance functionality that should be called manually in an ad-hoc way,
        not intended to be called/triggered automatically.
        """

        with TimingLogger(title="prune_empty_users", logger=_log), StatsReporter(
            report=_log
        ) as stats:
            ongoing_users = set(self._zk.get_children(self._ongoing()))
            done_users = set(self._zk.get_children(self._done()))
            common_users = ongoing_users.intersection(done_users)
            all_users = ongoing_users.union(done_users)
            _log.info(
                f"{len(ongoing_users)=} {len(done_users)=} {len(common_users)=} {len(all_users)=}"
            )

            for user_id in all_users:
                stats["user"] += 1
                ongoing_path = self._ongoing(user_id=user_id)
                done_path = self._done(user_id=user_id)
                try:
                    ongoing_stat: Optional[ZnodeStat] = self._zk.get(ongoing_path)[1]
                    ongoing_count = ongoing_stat.children_count
                except kazoo.exceptions.NoNodeError:
                    ongoing_stat = None
                    ongoing_count = 0
                try:
                    done_stat: Optional[ZnodeStat] = self._zk.get(done_path)[1]
                    done_count = done_stat.children_count
                except kazoo.exceptions.NoNodeError:
                    done_stat = None
                    done_count = 0

                stats[f"user with {bool(ongoing_count)=} {bool(done_count)=}"] += 1
                _log.debug(f"{user_id=} {ongoing_count=} {done_count=}")

                if ongoing_count == 0 and done_count == 0:
                    # For now, only prune users where both `ongoing` and `done` are empty
                    if ongoing_stat:
                        _log.info(f"Deleting {ongoing_path}")
                        stats["delete"] += 1
                        stats["delete ongoing"] += 1
                        if not dry_run:
                            self._zk.delete(
                                ongoing_path,
                                version=ongoing_stat.version,
                                recursive=False,
                            )
                    if done_stat:
                        _log.info(f"Deleting {done_path}")
                        stats["delete"] += 1
                        stats["delete done"] += 1
                        if not dry_run:
                            self._zk.delete(
                                done_path, version=done_stat.version, recursive=False
                            )


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


def zk_job_info_to_metadata(job_info: dict) -> BatchJobMetadata:
    """Convert job info dict (from ZkJobRegistry) to BatchJobMetadata"""
    # TODO: eliminate zk_job_info_to_metadata/ejr_job_info_to_metadata duplication?
    status = job_info.get("status")
    if status == "submitted":
        # TODO: is conversion of "submitted" still necessary?
        status = JOB_STATUS.CREATED
    specification = job_info["specification"]
    if isinstance(specification, str):
        specification = json.loads(specification)
    job_options = specification.pop("job_options", None)

    def map_safe(prop: str, f):
        value = job_info.get(prop)
        return f(value) if value else None

    return BatchJobMetadata(
        id=job_info["job_id"],
        process=specification,
        title=job_info.get("title"),
        description=job_info.get("description"),
        status=status,
        created=map_safe("created", rfc3339.parse_datetime),
        updated=map_safe("updated", rfc3339.parse_datetime),
        job_options=job_options,
        started=map_safe("started", rfc3339.parse_datetime),
        finished=map_safe("finished", rfc3339.parse_datetime),
        memory_time_megabyte=map_safe(
            "memory_time_megabyte_seconds", lambda seconds: timedelta(seconds=seconds)
        ),
        cpu_time=map_safe(
            "cpu_time_seconds", lambda seconds: timedelta(seconds=seconds)
        ),
        geometry=job_info.get("geometry"),
        bbox=job_info.get("bbox"),
        start_datetime=map_safe("start_datetime", rfc3339.parse_datetime),
        end_datetime=map_safe("end_datetime", rfc3339.parse_datetime),
        instruments=job_info.get("instruments"),
        epsg=job_info.get("epsg"),
        links=job_info.get("links"),
        usage=job_info.get("usage"),
        costs=job_info.get("costs"),
        proj_shape=job_info.get("proj:shape"),
        proj_bbox=job_info.get("proj:bbox"),
    )


def ejr_job_info_to_metadata(job_info: JobDict) -> BatchJobMetadata:
    """Convert job info dict (from JobRegistryInterface) to BatchJobMetadata"""
    # TODO: eliminate zk_job_info_to_metadata/ejr_job_info_to_metadata duplication?

    def map_safe(prop: str, f):
        value = job_info.get(prop)
        return f(value) if value else None

    return BatchJobMetadata(
        id=job_info["job_id"],
        status=job_info["status"],
        created=map_safe("created", rfc3339.parse_datetime),
        process=job_info.get("process"),
        job_options=job_info.get("job_options"),
        title=job_info.get("title"),
        description=job_info.get("description"),
        updated=map_safe("updated", rfc3339.parse_datetime),
        started=map_safe("started", rfc3339.parse_datetime),
        finished=map_safe("finished", rfc3339.parse_datetime),
        memory_time_megabyte=map_safe("memory_time_megabyte_seconds", lambda seconds: timedelta(seconds=seconds)),
        cpu_time=map_safe("cpu_time_seconds", lambda seconds: timedelta(seconds=seconds)),
        geometry=job_info.get("geometry"),
        bbox=job_info.get("bbox"),
        start_datetime=map_safe("start_datetime", rfc3339.parse_datetime),
        end_datetime=map_safe("end_datetime", rfc3339.parse_datetime),
        instruments=job_info.get("instruments"),
        epsg=job_info.get("epsg"),
        links=job_info.get("links"),
        usage=job_info.get("usage"),
        costs=job_info.get("costs"),
        proj_shape=job_info.get("proj:shape"),
        proj_bbox=job_info.get("proj:bbox"),
    )


class InMemoryJobRegistry(JobRegistryInterface):
    """
    Simple in-memory implementation of JobRegistryInterface for testing/dummy purposes
    """
    # TODO move this implementation to openeo_python_driver

    def __init__(self):
        self.db: Dict[str, JobDict] = {}

    def create_job(
        self,
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
        created = rfc3339.utcnow()
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

    def get_job(self, job_id: str) -> JobDict:
        if job_id not in self.db:
            raise JobNotFoundException(job_id=job_id)
        return self.db[job_id]

    def _update(self, job_id: str, **kwargs) -> JobDict:
        assert job_id in self.db
        self.db[job_id].update(**kwargs)
        return self.db[job_id]

    def set_status(
        self,
        job_id: str,
        status: str,
        *,
        updated: Optional[str] = None,
        started: Optional[str] = None,
        finished: Optional[str] = None,
    ) -> JobDict:
        self._update(
            job_id=job_id,
            status=status,
            updated=rfc3339.datetime(updated) if updated else rfc3339.utcnow(),
        )
        if started:
            self._update(job_id=job_id, started=rfc3339.datetime(started))
        if finished:
            self._update(job_id=job_id, finished=rfc3339.datetime(finished))
        return self.db[job_id]

    def set_dependencies(
        self, job_id: str, dependencies: List[Dict[str, str]]
    ) -> JobDict:
        return self._update(job_id=job_id, dependencies=dependencies)

    def remove_dependencies(self, job_id: str) -> JobDict:
        return self._update(job_id=job_id, dependencies=None, dependency_status=None)

    def set_dependency_status(self, job_id: str, dependency_status: str) -> JobDict:
        return self._update(job_id=job_id, dependency_status=dependency_status)

    def set_dependency_usage(self, job_id: str, dependency_usage: Decimal) -> JobDict:
        return self._update(job_id, dependency_usage=str(dependency_usage))

    def set_proxy_user(self, job_id: str, proxy_user: str) -> JobDict:
        return self._update(job_id=job_id, proxy_user=proxy_user)

    def set_application_id(self, job_id: str, application_id: str) -> JobDict:
        return self._update(job_id=job_id, application_id=application_id)

    def list_user_jobs(
        self, user_id: str, fields: Optional[List[str]] = None
    ) -> List[JobDict]:
        return [job for job in self.db.values() if job["user_id"] == user_id]

    def list_active_jobs(
        self, max_age: Optional[int] = None, fields: Optional[List[str]] = None
    ) -> List[JobDict]:
        active = [JOB_STATUS.CREATED, JOB_STATUS.QUEUED, JOB_STATUS.RUNNING]
        # TODO: implement max_age support
        return [job for job in self.db.values() if job["status"] in active]


class DoubleJobRegistryException(Exception):
    pass


class DoubleJobRegistry:
    """
    Adapter to simultaneously keep track of jobs in two job registries:
    a legacy ZkJobRegistry and a new ElasticJobRegistry.

    Meant as temporary stop gap to ease step-by-step migration from one system to the other.
    """

    _log = logging.getLogger(f"{__name__}.double")

    def __init__(
        self,
        zk_job_registry_factory: Optional[Callable[[], ZkJobRegistry]] = ZkJobRegistry,
        elastic_job_registry: Optional[JobRegistryInterface] = None,
    ):
        # Note: we use a factory here because current implementation (and test coverage) heavily depends on
        # just-in-time instantiation of `ZkJobRegistry` in various places (`with ZkJobRegistry(): ...`)
        self._zk_job_registry_factory = zk_job_registry_factory
        self.zk_job_registry: Optional[ZkJobRegistry] = None
        self.elastic_job_registry = elastic_job_registry
        # Synchronisation lock to make sure that only one thread at a time can use this as a context manager.
        self._lock = threading.RLock()

    def __repr__(self):
        return (
            f"<{type(self).__name__} {type(self.zk_job_registry).__name__}+{type(self.elastic_job_registry).__name__}>"
        )

    def __enter__(self):
        self._lock.acquire()
        self.zk_job_registry = self._zk_job_registry_factory() if self._zk_job_registry_factory else None
        self._log.debug(f"Context enter {self!r}")
        if self.zk_job_registry:
            try:
                self.zk_job_registry.__enter__()
            except KazooTimeoutError as e:
                _log.warning(f"Failed to enter {type(self.zk_job_registry).__name__}: {e!r}")
                self.zk_job_registry = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._log.debug(f"Context exit {self!r} ({exc_type=})")
        try:
            if self.zk_job_registry:
                self.zk_job_registry.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self.zk_job_registry = None
            self._lock.release()

    def _just_log_errors(
        self, name: str, job_id: Optional[str] = None, extra: Optional[dict] = None
    ):
        """Context manager to just log exceptions"""
        if job_id:
            extra = dict(extra or {}, job_id=job_id)
        return just_log_exceptions(
            log=self._log.warning, name=f"DoubleJobRegistry.{name}", extra=extra
        )

    def create_job(
        self,
        job_id: str,
        user_id: str,
        process: dict,
        api_version: Optional[str] = None,
        job_options: Optional[dict] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> dict:
        zk_job_info = None
        ejr_job_info = None
        if self.zk_job_registry:
            zk_job_info = self.zk_job_registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version=api_version,
                specification=dict_no_none(
                    process_graph=process["process_graph"],
                    job_options=job_options,
                ),
                title=title,
                description=description,
            )
        if self.elastic_job_registry:
            with self._just_log_errors("create_job", job_id=job_id):
                ejr_job_info = self.elastic_job_registry.create_job(
                    process=process,
                    user_id=user_id,
                    job_id=job_id,
                    title=title,
                    description=description,
                    api_version=api_version,
                    job_options=job_options,
                )
        if zk_job_info is None and ejr_job_info is None:
            raise DoubleJobRegistryException(f"None of ZK/EJR registered {job_id=}")
        return zk_job_info or ejr_job_info

    def get_job(self, job_id: str, user_id: str) -> dict:
        # TODO: eliminate get_job/get_job_metadata duplication?
        zk_job = ejr_job = None
        if self.zk_job_registry:
            zk_job = self.zk_job_registry.get_job(job_id=job_id, user_id=user_id)
        if self.elastic_job_registry:
            with self._just_log_errors("get_job", job_id=job_id):
                ejr_job = self.elastic_job_registry.get_job(job_id=job_id)

        self._check_zk_ejr_job_info(job_id=job_id, zk_job_info=zk_job, ejr_job_info=ejr_job)
        return zk_job or ejr_job

    def get_job_metadata(self, job_id: str, user_id: str) -> BatchJobMetadata:
        # TODO: eliminate get_job/get_job_metadata duplication?
        zk_job_info = ejr_job_info = None
        if self.zk_job_registry:
            zk_job_info = self.zk_job_registry.get_job(job_id=job_id, user_id=user_id)
        if self.elastic_job_registry:
            with self._just_log_errors("get_job_metadata", job_id=job_id):
                ejr_job_info = self.elastic_job_registry.get_job(job_id=job_id)

        self._check_zk_ejr_job_info(job_id=job_id, zk_job_info=zk_job_info, ejr_job_info=ejr_job_info)
        job_metadata = zk_job_info_to_metadata(zk_job_info) if zk_job_info else ejr_job_info_to_metadata(ejr_job_info)
        return job_metadata

    def _check_zk_ejr_job_info(self, job_id: str, zk_job_info: Union[dict, None], ejr_job_info: Union[dict, None]):
        # TODO #236 For now: compare job metadata between Zk and EJR
        fields = ["job_id", "status", "created"]
        if zk_job_info is not None and ejr_job_info is not None:
            zk_job_info = {k: v for (k, v) in zk_job_info.items() if k in fields}
            ejr_job_info = {k: v for (k, v) in ejr_job_info.items() if k in fields}
            if zk_job_info != ejr_job_info:
                self._log.warning(f"DoubleJobRegistry mismatch {zk_job_info=} {ejr_job_info=}")
        elif zk_job_info is None and ejr_job_info is None:
            raise DoubleJobRegistryException(f"None of ZK/EJR have {job_id=}")

    def set_status(self, job_id: str, user_id: str, status: str) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.set_status(job_id=job_id, user_id=user_id, status=status)
        if self.elastic_job_registry:
            with self._just_log_errors("set_status", job_id=job_id):
                self.elastic_job_registry.set_status(job_id=job_id, status=status)

    def delete(self, job_id: str, user_id: str) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.delete(job_id=job_id, user_id=user_id)
        if self.elastic_job_registry:
            # TODO support for deletion in EJR (https://github.com/Open-EO/openeo-python-driver/issues/163)
            self._log.warning(f"EJR TODO: support job deletion ({job_id=})")

    def set_dependencies(
        self, job_id: str, user_id: str, dependencies: List[Dict[str, str]]
    ):
        if self.zk_job_registry:
            self.zk_job_registry.set_dependencies(job_id=job_id, user_id=user_id, dependencies=dependencies)
        if self.elastic_job_registry:
            with self._just_log_errors("set_dependencies", job_id=job_id):
                self.elastic_job_registry.set_dependencies(
                    job_id=job_id, dependencies=dependencies
                )

    def remove_dependencies(self, job_id: str, user_id: str):
        if self.zk_job_registry:
            self.zk_job_registry.remove_dependencies(job_id=job_id, user_id=user_id)
        if self.elastic_job_registry:
            with self._just_log_errors("remove_dependencies", job_id=job_id):
                self.elastic_job_registry.remove_dependencies(job_id=job_id)

    def set_dependency_status(
        self, job_id: str, user_id: str, dependency_status: str
    ) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.set_dependency_status(
                job_id=job_id, user_id=user_id, dependency_status=dependency_status
            )
        if self.elastic_job_registry:
            with self._just_log_errors("set_dependency_status", job_id=job_id):
                self.elastic_job_registry.set_dependency_status(
                    job_id=job_id, dependency_status=dependency_status
                )

    def set_dependency_usage(
        self, job_id: str, user_id: str, dependency_usage: Decimal
    ):
        if self.zk_job_registry:
            self.zk_job_registry.set_dependency_usage(job_id=job_id, user_id=user_id, processing_units=dependency_usage)
        if self.elastic_job_registry:
            with self._just_log_errors("set_dependency_usage", job_id=job_id):
                self.elastic_job_registry.set_dependency_usage(
                    job_id=job_id, dependency_usage=dependency_usage
                )

    def set_proxy_user(self, job_id: str, user_id: str, proxy_user: str):
        # TODO: add dedicated method
        if self.zk_job_registry:
            self.zk_job_registry.patch(job_id=job_id, user_id=user_id, proxy_user=proxy_user)
        if self.elastic_job_registry:
            with self._just_log_errors("set_proxy_user", job_id=job_id):
                self.elastic_job_registry.set_proxy_user(
                    job_id=job_id, proxy_user=proxy_user
                )

    def set_application_id(
        self, job_id: str, user_id: str, application_id: str
    ) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.set_application_id(job_id=job_id, user_id=user_id, application_id=application_id)
        if self.elastic_job_registry:
            with self._just_log_errors("set_application_id", job_id=job_id):
                self.elastic_job_registry.set_application_id(
                    job_id=job_id, application_id=application_id
                )

    def mark_ongoing(self, job_id: str, user_id: str) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.mark_ongoing(job_id=job_id, user_id=user_id)

    def get_user_jobs(
        self, user_id: str, fields: Optional[List[str]] = None
    ) -> List[BatchJobMetadata]:
        zk_jobs = None
        ejr_jobs = None
        if self.zk_job_registry:
            zk_jobs = [zk_job_info_to_metadata(j) for j in self.zk_job_registry.get_user_jobs(user_id)]
        if self.elastic_job_registry:
            with self._just_log_errors("get_user_jobs"):
                ejr_jobs = [
                    ejr_job_info_to_metadata(j)
                    for j in self.elastic_job_registry.list_user_jobs(user_id=user_id, fields=fields)
                ]

        # TODO: more insightful comparison? (e.g. only consider recent jobs)
        self._log.log(
            level=logging.WARNING if (zk_jobs is None or ejr_jobs is None) else logging.INFO,
            msg=f"DoubleJobRegistry.get_user_jobs({user_id=}) zk_jobs={zk_jobs and len(zk_jobs)} ejr_jobs={ejr_jobs and len(ejr_jobs)}",
        )
        # TODO #236: eror if both sources failed?

        return zk_jobs or ejr_jobs or []

    def get_all_jobs_before(
        self,
        upper: dt.datetime,
        *,
        user_ids: Optional[List[str]] = None,
        include_ongoing: bool = True,
        include_done: bool = True,
        user_limit: Optional[int] = 1000,
    ) -> List[dict]:
        # TODO #236/#498 Need to have EJR implementation for this? This is only necessary for ZK cleaner script anyway.
        jobs = self.zk_job_registry.get_all_jobs_before(
            upper=upper,
            user_ids=user_ids,
            include_ongoing=include_ongoing,
            include_done=include_done,
            user_limit=user_limit,
        )
        return jobs


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    with ZkJobRegistry(
        root_path="/openeo/dev/jobs"
        # root_path="/openeo/jobs"
    ) as zk_registry:
        zk_registry.get_running_jobs(
            # user_limit=1000,
            # user_limit=None,
        )

        # zk_registry.prune_empty_users(
        #     dry_run=True
        #     # dry_run=False
        # )
