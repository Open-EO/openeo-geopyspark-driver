import contextlib
import datetime as dt
import json
import logging
import random
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, List, Dict, Callable, Union, Optional, Iterator, Tuple

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
from openeogeotrellis import sentinel_hub
from openeogeotrellis.config import get_backend_config
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

    @staticmethod
    def build_specification_dict(process_graph: dict, job_options: Optional[dict] = None) -> dict:
        """
        Helper to build a specification dict for ZK job registry from process graph and job options in a consistent way.
        """
        assert "process_graph" not in process_graph
        return dict_no_none(
            process_graph=process_graph,
            job_options=job_options,
        )

    def register(
            self, job_id: str, user_id: str, api_version: str, specification: dict,
            title: str = None, description: str = None
    ) -> dict:
        """Registers a to-be-run batch job."""
        # TODO: why json-encoding `specification` when the whole job_info dict will be json-encoded anyway?
        # TODO: use more efficient JSON separators (e.g. `separators=(',', ':')`)?
        specification_blob = json.dumps(specification)
        specification_size = len(specification_blob)
        _log.debug(f"ZkJobRegistry.register: {specification_size=}")
        max_specification_size = get_backend_config().zk_job_registry_max_specification_size
        if max_specification_size is not None and specification_size > max_specification_size:
            _log.warning(
                f"Stripping 'specification' from ZK payload for {job_id=}: {specification_size=} > {max_specification_size=}"
            )
            # Note this is intentionally invalid JSON to cause failure when naively attempting to decode it
            specification_blob = (
                f"{ZkStrippedSpecification.PAYLOAD_MARKER} {specification_size=} > {max_specification_size=}"
            )
        # TODO: use `BatchJobMetadata` instead of free form dict here?
        job_info = {
            "job_id": job_id,
            "user_id": user_id,
            "status": JOB_STATUS.CREATED,
            # TODO: move api_Version into specification?
            'api_version': api_version,
            "specification": specification_blob,
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
        self, job_id: str, user_id: str, status: str, started: Optional[str] = None, finished: Optional[str] = None,
            auto_mark_done: bool = True
    ) -> None:
        """Updates a registered batch job with its status. Additionally, updates its "updated" property."""
        kwargs = {
            "status": status,
            "updated": rfc3339.utcnow(),
        }

        if started:
            kwargs["started"] = started
        if finished:
            kwargs["finished"] = finished

        self.patch(
            job_id,
            user_id,
            auto_mark_done=auto_mark_done,
            **kwargs
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

    def get_running_jobs(self, *, user_limit: Optional[int] = 1000, parse_specification: bool = True) -> Iterator[Dict]:
        """Returns an iterator of job info dicts that are currently not finished (should still be tracked)."""

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
                    stats["user_id with jobs"] += 1
                    for job_id in job_ids:
                        try:
                            job_info = self.get_job(job_id, user_id, parse_specification=parse_specification)
                            if job_info.get("application_id"):
                                yield job_info
                                stats["job_ids"] += 1
                        except JobNotFoundException:
                            _log.warning(f"Job {job_id} of user {user_id} disappeared from the list of running"
                                         f" jobs; this can happen if it was deleted in the meanwhile.", exc_info=True)

                else:
                    stats["user_id without jobs"] += 1

    def get_job(
        self,
        job_id: str,
        user_id: str,
        *,
        parse_specification: bool = False,
        omit_raw_specification: bool = False,
    ) -> dict:
        """Returns details of a job."""
        job_info, _ = self._read(
            job_id=job_id,
            user_id=user_id,
            include_done=True,
            parse_specification=parse_specification,
            omit_raw_specification=omit_raw_specification,
        )
        return job_info

    def get_user_jobs(self, user_id: str) -> List[Dict]:
        """Returns details of all jobs for a specific user."""

        jobs = []

        try:
            done_job_ids = self._zk.get_children(self._done(user_id))
            jobs.extend(
                self.get_job(job_id=job_id, user_id=user_id, parse_specification=False, omit_raw_specification=True)
                for job_id in done_job_ids
            )
        except NoNodeError:
            pass

        try:
            ongoing_job_ids = self._zk.get_children(self._ongoing(user_id))
            jobs.extend(
                self.get_job(job_id=job_id, user_id=user_id, parse_specification=False, omit_raw_specification=True)
                for job_id in ongoing_job_ids
            )
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
        per_user_limit: Optional[int] = 1000,
        field_whitelist: Optional[List[str]] = None,
    ) -> List[Dict]:
        def get_jobs_in(
            get_path: Callable[[Union[str, None], Union[str, None]], str],
            user_ids: Optional[List[str]] = None,
        ) -> List[Dict]:
            if user_ids is None:
                user_ids = self._zk.get_children(get_path(None, None))
                _log.info(f"Collected {len(user_ids)=} in {get_path(None, None)!r}")
            else:
                _log.info(f"Using provided user_id list {user_ids=}")

            jobs_before = []

            for user_id in sorted(user_ids):
                path = get_path(user_id, None)
                try:
                    user_job_ids = self._zk.get_children(path)
                except NoNodeError:
                    _log.warning(
                        f"Not found (and no children) for {user_id=} ({path=})"
                    )
                    continue

                if per_user_limit and len(user_job_ids) > per_user_limit:
                    _log.warning(
                        f"User {user_id} has excessive number of jobs: {len(user_job_ids)}. "
                        f"Sampling down to {per_user_limit}."
                    )
                    user_job_ids = random.sample(user_job_ids, k=per_user_limit)

                for job_id in user_job_ids:
                    path = get_path(user_id, job_id)
                    data, stat = self._zk.get(path)
                    job_info = json.loads(data.decode())

                    updated = job_info.get('updated')
                    job_date = (rfc3339.parse_datetime(updated) if updated
                                else datetime.utcfromtimestamp(stat.last_modified))

                    if job_date < upper:
                        _log.debug("job {j}'s job_date {d} is before {u}".format(j=job_id, d=job_date, u=upper))
                        if field_whitelist:
                            job_info = {k: job_info[k] for k in field_whitelist if k in job_info}
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

    def _read(
        self,
        job_id: str,
        user_id: str,
        *,
        include_done: bool = False,
        parse_specification: bool = False,
        omit_raw_specification: bool = False,
    ) -> Tuple[Dict, int]:
        """
        :param parse_specification: parse the (JSON encoded) "specification" field
            and inject the process_graph (as `"process": {"process_graph": ...}`) and job_options as additional fields
        :param omit_raw_specification: remove the original (raw) "specification" field from the result
        """
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

        job_info = json.loads(data.decode())
        if parse_specification:
            process_graph, job_options = parse_zk_job_specification(job_info)
            if "process" not in job_info:
                job_info["process"] = {"process_graph": process_graph}
            if "job_options" not in job_info:
                job_info["job_options"] = job_options

        if omit_raw_specification:
            del job_info["specification"]

        return job_info, stat.version

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


class ZkStrippedSpecification(Exception):
    PAYLOAD_MARKER = "<ZkStrippedSpecification>"


def parse_zk_job_specification(job_info: dict, default_job_options=None) -> Tuple[dict, Union[dict, None]]:
    """Parse the JSON-encoded 'specification' field from ZK job info dict"""
    specification_json = job_info["specification"]
    if specification_json.startswith(ZkStrippedSpecification.PAYLOAD_MARKER):
        raise ZkStrippedSpecification()
    specification = json.loads(specification_json)
    process_graph = specification["process_graph"]
    job_options = specification.get("job_options", default_job_options)
    return process_graph, job_options


def zk_job_info_to_metadata(job_info: dict) -> BatchJobMetadata:
    """Convert job info dict (from ZkJobRegistry) to BatchJobMetadata"""
    # TODO: eliminate zk_job_info_to_metadata/ejr_job_info_to_metadata duplication?
    status = job_info.get("status")
    if status == "submitted":
        # TODO: is conversion of "submitted" still necessary?
        status = JOB_STATUS.CREATED

    def map_safe(prop: str, f):
        value = job_info.get(prop)
        return f(value) if value else None

    return BatchJobMetadata(
        id=job_info["job_id"],
        process=job_info.get("process"),
        title=job_info.get("title"),
        description=job_info.get("description"),
        status=status,
        created=map_safe("created", rfc3339.parse_datetime),
        updated=map_safe("updated", rfc3339.parse_datetime),
        job_options=job_info.get("job_options"),
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


def ejr_job_info_to_metadata(job_info: JobDict, full: bool = True) -> BatchJobMetadata:
    """Convert job info dict (from JobRegistryInterface) to BatchJobMetadata"""
    # TODO: eliminate zk_job_info_to_metadata/ejr_job_info_to_metadata duplication?

    def map_safe(prop: str, f):
        value = job_info.get(prop)
        return f(value) if value else None

    def get_results_metadata(result_metadata_prop: str):
        return job_info.get("results_metadata", {}).get(result_metadata_prop)

    def map_results_metadata_safe(result_metadata_prop: str, f):
        value = get_results_metadata(result_metadata_prop)
        return f(value) if value is not None else None

    return BatchJobMetadata(
        id=job_info["job_id"],
        status=job_info["status"],
        created=map_safe("created", rfc3339.parse_datetime),
        process=job_info.get("process") if full else None,
        job_options=job_info.get("job_options") if full else None,
        title=job_info.get("title"),
        description=job_info.get("description"),
        updated=map_safe("updated", rfc3339.parse_datetime),
        started=map_safe("started", rfc3339.parse_datetime),
        finished=map_safe("finished", rfc3339.parse_datetime),
        memory_time_megabyte=map_safe("memory_time_megabyte_seconds", lambda seconds: timedelta(seconds=seconds)),
        cpu_time=map_safe("cpu_time_seconds", lambda seconds: timedelta(seconds=seconds)),
        geometry=get_results_metadata("geometry"),
        bbox=get_results_metadata("bbox"),
        start_datetime=map_results_metadata_safe("start_datetime", rfc3339.parse_datetime),
        end_datetime=map_results_metadata_safe("end_datetime", rfc3339.parse_datetime),
        instruments=get_results_metadata("instruments"),
        epsg=get_results_metadata("epsg"),
        links=get_results_metadata("links"),
        usage=job_info.get("usage"),
        costs=job_info.get("costs"),
        proj_shape=get_results_metadata("proj:shape"),
        proj_bbox=get_results_metadata("proj:bbox"),
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

    def get_job(self, job_id: str, user_id: Optional[str] = None) -> JobDict:
        job = self.db.get(job_id)

        if not job or (user_id is not None and job['user_id'] != user_id):
            raise JobNotFoundException(job_id=job_id)

        return job

    def delete_job(self, job_id: str, user_id: Optional[str] = None) -> None:
        self.get_job(job_id=job_id, user_id=user_id)  # will raise on job not found
        del self.db[job_id]

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

    def set_usage(self, job_id: str, costs: float, usage: dict) -> JobDict:
        return self._update(job_id=job_id, costs=costs, usage=usage)

    def set_results_metadata(self, job_id: str, costs: Optional[float], usage: dict,
                             results_metadata: Dict[str, Any]) -> JobDict:
        return self._update(job_id=job_id, costs=costs, usage=usage, results_metadata=results_metadata)

    def list_user_jobs(
        self, user_id: str, fields: Optional[List[str]] = None
    ) -> List[JobDict]:
        return [job for job in self.db.values() if job["user_id"] == user_id]

    def list_active_jobs(
        self,
        *,
        max_age: Optional[int] = None,
        fields: Optional[List[str]] = None,
        has_application_id: bool = False,
    ) -> List[JobDict]:
        active = [JOB_STATUS.CREATED, JOB_STATUS.QUEUED, JOB_STATUS.RUNNING]
        # TODO: implement support for max_age, has_application_id, fields
        return [job for job in self.db.values() if job["status"] in active]



class DoubleJobRegistryException(Exception):
    pass


class DoubleJobRegistry:  # TODO: extend JobRegistryInterface?
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
        # self._lock = threading.RLock()

    def __repr__(self):
        return (
            f"<{type(self).__name__} {type(self.zk_job_registry).__name__}+{type(self.elastic_job_registry).__name__}>"
        )

    def __enter__(self):
        # self._lock.acquire()
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
            # self._lock.release()

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
                specification=ZkJobRegistry.build_specification_dict(
                    process_graph=process["process_graph"],
                    job_options=job_options,
                ),
                title=title,
                description=description,
            )
        if self.elastic_job_registry:
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
            with contextlib.suppress(JobNotFoundException, ZkStrippedSpecification):
                zk_job = self.zk_job_registry.get_job(
                    job_id=job_id, user_id=user_id, parse_specification=True, omit_raw_specification=True
                )
        if self.elastic_job_registry:
            with contextlib.suppress(JobNotFoundException):
                ejr_job = self.elastic_job_registry.get_job(job_id=job_id, user_id=user_id)

        self._check_zk_ejr_job_info(job_id=job_id, zk_job_info=zk_job, ejr_job_info=ejr_job)
        return zk_job or ejr_job

    def get_job_metadata(self, job_id: str, user_id: str) -> BatchJobMetadata:
        # TODO: eliminate get_job/get_job_metadata duplication?
        zk_job_info = ejr_job_info = None
        if self.zk_job_registry:
            with TimingLogger(f"self.zk_job_registry.get_job({job_id=}, {user_id=})", logger=_log.debug):
                with contextlib.suppress(JobNotFoundException, ZkStrippedSpecification):
                    zk_job_info = self.zk_job_registry.get_job(
                        job_id=job_id, user_id=user_id, parse_specification=True, omit_raw_specification=True
                    )
        if self.elastic_job_registry:
            with TimingLogger(f"self.elastic_job_registry.get_job({job_id=})", logger=_log.debug):
                with contextlib.suppress(JobNotFoundException):
                    ejr_job_info = self.elastic_job_registry.get_job(job_id=job_id, user_id=user_id)

        self._check_zk_ejr_job_info(job_id=job_id, zk_job_info=zk_job_info, ejr_job_info=ejr_job_info)
        job_metadata = zk_job_info_to_metadata(zk_job_info) if zk_job_info else ejr_job_info_to_metadata(ejr_job_info)
        return job_metadata

    def _check_zk_ejr_job_info(self, job_id: str, zk_job_info: Union[dict, None], ejr_job_info: Union[dict, None]):
        # TODO #236/#498 For now: compare job metadata between Zk and EJR
        fields = ["job_id", "status", "created"]
        if zk_job_info is not None and ejr_job_info is not None:
            zk_job_info = {k: v for (k, v) in zk_job_info.items() if k in fields}
            ejr_job_info = {k: v for (k, v) in ejr_job_info.items() if k in fields}
            if zk_job_info != ejr_job_info:
                self._log.warning(f"DoubleJobRegistry mismatch {zk_job_info=} {ejr_job_info=}")
        elif zk_job_info is None and ejr_job_info is None:
            raise JobNotFoundException(job_id=job_id)

    def set_status(self, job_id: str, user_id: str, status: str,
                   started: Optional[str] = None, finished: Optional[str] = None,
                   ) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.set_status(job_id=job_id, user_id=user_id, status=status, started=started,
                                            finished=finished)
        if self.elastic_job_registry:
            self.elastic_job_registry.set_status(job_id=job_id, status=status, started=started, finished=finished)

    def delete_job(self, job_id: str, user_id: str) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.delete(job_id=job_id, user_id=user_id)
        if self.elastic_job_registry:
            self.elastic_job_registry.delete_job(job_id=job_id, user_id=user_id)

    # Legacy alias
    delete = delete_job

    def set_dependencies(
        self, job_id: str, user_id: str, dependencies: List[Dict[str, str]]
    ):
        if self.zk_job_registry:
            self.zk_job_registry.set_dependencies(job_id=job_id, user_id=user_id, dependencies=dependencies)
        if self.elastic_job_registry:
            self.elastic_job_registry.set_dependencies(
                job_id=job_id, dependencies=dependencies
            )

    def remove_dependencies(self, job_id: str, user_id: str):
        if self.zk_job_registry:
            self.zk_job_registry.remove_dependencies(job_id=job_id, user_id=user_id)
        if self.elastic_job_registry:
            self.elastic_job_registry.remove_dependencies(job_id=job_id)

    def set_dependency_status(
        self, job_id: str, user_id: str, dependency_status: str
    ) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.set_dependency_status(
                job_id=job_id, user_id=user_id, dependency_status=dependency_status
            )
        if self.elastic_job_registry:
            self.elastic_job_registry.set_dependency_status(
                job_id=job_id, dependency_status=dependency_status
            )

    def set_dependency_usage(
        self, job_id: str, user_id: str, dependency_usage: Decimal
    ):
        if self.zk_job_registry:
            self.zk_job_registry.set_dependency_usage(job_id=job_id, user_id=user_id, processing_units=dependency_usage)
        if self.elastic_job_registry:
            self.elastic_job_registry.set_dependency_usage(
                job_id=job_id, dependency_usage=dependency_usage
            )

    def set_proxy_user(self, job_id: str, user_id: str, proxy_user: str):
        # TODO: add dedicated method
        if self.zk_job_registry:
            self.zk_job_registry.patch(job_id=job_id, user_id=user_id, proxy_user=proxy_user)
        if self.elastic_job_registry:
            self.elastic_job_registry.set_proxy_user(
                job_id=job_id, proxy_user=proxy_user
            )

    def set_application_id(
        self, job_id: str, user_id: str, application_id: str
    ) -> None:
        if self.zk_job_registry:
            self.zk_job_registry.set_application_id(job_id=job_id, user_id=user_id, application_id=application_id)
        if self.elastic_job_registry:
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
            ejr_jobs = [
                ejr_job_info_to_metadata(j, full=False)
                for j in self.elastic_job_registry.list_user_jobs(user_id=user_id, fields=fields)
            ]

        # TODO: more insightful comparison? (e.g. only consider recent jobs)
        self._log.log(
            level=logging.WARNING if (zk_jobs is None or ejr_jobs is None) else logging.INFO,
            msg=f"DoubleJobRegistry.get_user_jobs({user_id=}) zk_jobs={zk_jobs and len(zk_jobs)} ejr_jobs={ejr_jobs and len(ejr_jobs)}",
        )
        # TODO #236/#498: error if both sources failed?

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
        if not self.zk_job_registry:
            raise NotImplementedError("only necessary for ZK cleaner script")

        jobs = self.zk_job_registry.get_all_jobs_before(
            upper=upper,
            user_ids=user_ids,
            include_ongoing=include_ongoing,
            include_done=include_done,
            per_user_limit=user_limit,
        )
        return jobs

    def get_active_jobs(self, max_age: Optional[int] = None) -> Iterator[Dict]:
        if self.zk_job_registry:
            # Note: `parse_specification` is enabled here because the jobtracker needs job_options (e.g. to determine target ETL)
            yield from self.zk_job_registry.get_running_jobs(parse_specification=True)
        elif self.elastic_job_registry:
            yield from self.elastic_job_registry.list_active_jobs(
                fields=[
                    "job_id",
                    "user_id",
                    "application_id",
                    "status",
                    "created",
                    "title",
                    "job_options",
                    "dependencies",
                    "dependency_usage",
                ],
                max_age=max_age,
                has_application_id=True,
            )

    def set_results_metadata(self, job_id, user_id, costs: Optional[float], usage: dict,
                             results_metadata: Dict[str, Any]):
        if self.zk_job_registry:
            self.zk_job_registry.patch(job_id=job_id, user_id=user_id,
                                       **dict(results_metadata, costs=costs, usage=usage))

        if self.elastic_job_registry:
            self.elastic_job_registry.set_results_metadata(job_id=job_id, costs=costs, usage=usage,
                                                           results_metadata=results_metadata)
