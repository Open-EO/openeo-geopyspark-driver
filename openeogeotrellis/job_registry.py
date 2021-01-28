import json
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Union
import logging

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError

from openeo.util import rfc3339
from openeo_driver.backend import BatchJobMetadata
from openeogeotrellis.configparams import ConfigParams
from openeo_driver.errors import JobNotFoundException


_log = logging.getLogger(__name__)


class JobRegistry:
    # TODO: improve encapsulation
    def __init__(self, zookeeper_hosts: str=','.join(ConfigParams().zookeepernodes)):
        self._root = '/openeo/jobs'
        self._zk = KazooClient(hosts=zookeeper_hosts)

    def ensure_paths(self):
        self._zk.ensure_path(self._ongoing())
        self._zk.ensure_path(self._done())

    def register(
            self, job_id: str, user_id: str, api_version: str, specification: dict,
            title:str=None, description:str=None
    ) -> dict:
        """Registers a to-be-run batch job."""
        # TODO: use `BatchJobMetadata` instead of free form dict here?
        job_info = {
            'job_id': job_id,
            'user_id': user_id,
            'status': 'created',
            # TODO: move api_Version into specification?
            'api_version': api_version,
            # TODO: why json-encoding `specification` when the whole job_info dict will be json-encoded anyway?
            'specification': json.dumps(specification),
            'application_id': None,
            'created': rfc3339.datetime(datetime.utcnow()),
            'title': title,
            'description': description,
        }
        self._create(job_info)
        return job_info

    @staticmethod
    def job_info_to_metadata(job_info: dict) -> BatchJobMetadata:
        """Convert job info dict to BatchJobMetadata"""
        status = job_info.get("status")
        if status == "submitted":
            status = "created"
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
            memory_time_megabyte=map_safe("memory_time_megabyte_seconds", lambda seconds: timedelta(seconds=seconds)),
            cpu_time=map_safe("cpu_time_seconds", lambda seconds: timedelta(seconds=seconds)),
            geometry=job_info.get("geometry"),
            bbox=job_info.get("bbox"),
            start_datetime=map_safe("start_datetime", rfc3339.parse_datetime),
            end_datetime=map_safe("end_datetime", rfc3339.parse_datetime),
            processing_facility="VITO"
        )

    def set_application_id(self, job_id: str, user_id: str, application_id: str) -> None:
        """Updates a registered batch job with its Spark application ID."""

        self.patch(job_id, user_id, application_id=application_id)

    def set_status(self, job_id: str, user_id: str, status: str) -> None:
        """Updates a registered batch job with its status. Additionally, updates its "updated" property."""

        self.patch(job_id, user_id, status=status, updated=rfc3339.datetime(datetime.utcnow()))
        _log.debug("batch job {j} -> {s}".format(j=job_id, s=status))

    def set_dependency_status(self, job_id: str, user_id: str, dependency_status: str) -> None:
        self.patch(job_id, user_id, dependency_status=dependency_status)
        _log.debug("batch job {j} dependency -> {s}".format(j=job_id, s=dependency_status))

    def add_dependencies(self, job_id: str, user_id: str, dependencies: List[Dict[str, str]]):
        self.patch(job_id, user_id, dependencies=dependencies)

    def remove_dependencies(self, job_id: str, user_id: str):
        self.patch(job_id, user_id, dependencies=None, dependency_status=None)

    def patch(self, job_id: str, user_id: str, **kwargs) -> None:
        """Partially updates a registered batch job."""

        job_info, version = self._read(job_id, user_id)

        self._update({**job_info, **kwargs}, version)

    def mark_done(self, job_id: str, user_id: str) -> None:
        """Marks a job as done (not to be tracked anymore)."""

        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id, user_id)

        source = self._ongoing(user_id, job_id)

        try:
            self._create(job_info, done=True)
        except NodeExistsError:
            pass

        self._zk.delete(source, version)

    def mark_ongoing(self, job_id: str, user_id: str) -> None:
        """Marks as job as ongoing (to be tracked)."""

        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id, user_id, include_done=True)

        source = self._done(user_id, job_id)

        try:
            self._create(job_info, done=False)
        except NodeExistsError:
            pass

        self._zk.delete(source, version)

    def get_running_jobs(self) -> List[Dict]:
        """Returns a list of jobs that are currently not finished (should still be tracked)."""

        jobs = []

        user_ids = self._zk.get_children(self._ongoing())

        for user_id in user_ids:
            job_ids = self._zk.get_children(self._ongoing(user_id))
            jobs.extend([self.get_job(job_id, user_id) for job_id in job_ids])

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

    def __enter__(self) -> 'JobRegistry':
        self._zk.start()
        return self

    def __exit__(self, *_):
        self._zk.stop()
        self._zk.close()

    def delete(self, job_id: str, user_id: str) -> None:
        try:
            path = self._ongoing(user_id, job_id)
            self._zk.delete(path)
        except NoNodeError:
            path = self._done(user_id, job_id)

            try:
                self._zk.delete(path)
            except NoNodeError:
                raise JobNotFoundException(job_id)

    def get_all_jobs_before(self, upper: datetime) -> List[Dict]:
        def get_jobs_in(get_path: Callable[[Union[str, None], Union[str, None]], str]) -> List[Dict]:
            user_ids = self._zk.get_children(get_path(None, None))

            jobs_before = []

            for user_id in user_ids:
                user_job_ids = self._zk.get_children(get_path(user_id, None))

                for job_id in user_job_ids:
                    path = get_path(user_id, job_id)
                    data, stat = self._zk.get(path)
                    job_info = json.loads(data.decode())

                    updated = job_info.get('updated')
                    job_date = rfc3339.parse_datetime(updated) if updated else datetime.utcfromtimestamp(stat.last_modified)

                    if job_date < upper:
                        _log.debug("job {j}'s job_date {d} is before {u}".format(j=job_id, d=job_date, u=upper))
                        jobs_before.append(job_info)

            return jobs_before

        # note: consider ongoing as well because that's where abandoned (never started) jobs are
        return get_jobs_in(self._ongoing) + get_jobs_in(self._done)

    def _create(self, job_info: Dict, done: bool=False) -> None:
        job_id = job_info['job_id']
        user_id = job_info['user_id']

        path = self._done(user_id, job_id) if done else self._ongoing(user_id, job_id)
        data = json.dumps(job_info).encode()

        self._zk.create(path, data, makepath=True)

    def _read(self, job_id: str, user_id: str, include_done=False) -> (Dict, int):
        try:
            path = self._ongoing(user_id, job_id)
            data, stat = self._zk.get(path)
        except NoNodeError:
            if include_done:
                path = self._done(user_id, job_id)

                try:
                    data, stat = self._zk.get(path)
                except NoNodeError:
                    raise JobNotFoundException(job_id)
            else:
                raise JobNotFoundException(job_id)

        return json.loads(data.decode()), stat.version

    def _update(self, job_info: Dict, version: int) -> None:
        job_id = job_info['job_id']
        user_id = job_info['user_id']

        path = self._ongoing(user_id, job_id)
        data = json.dumps(job_info).encode()

        self._zk.set(path, data, version)

    def _ongoing(self, user_id: str=None, job_id: str=None) -> str:
        if job_id:
            return "{r}/ongoing/{u}/{j}".format(r=self._root, u=user_id, j=job_id)
        elif user_id:
            return "{r}/ongoing/{u}".format(r=self._root, u=user_id)

        return "{r}/ongoing".format(r=self._root)

    def _done(self, user_id: str=None, job_id: str=None) -> str:
        if job_id:
            return "{r}/done/{u}/{j}".format(r=self._root, u=user_id, j=job_id)
        elif user_id:
            return "{r}/done/{u}".format(r=self._root, u=user_id)

        return "{r}/done".format(r=self._root)
