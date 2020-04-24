from datetime import datetime
from typing import List, Dict
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
import json

from openeo.util import date_to_rfc3339
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.utils import parse_rfc3339
from openeogeotrellis.configparams import ConfigParams
from openeo_driver.errors import JobNotFoundException


class JobRegistry:
    def __init__(self, zookeeper_hosts: str=','.join(ConfigParams().zookeepernodes)):
        self._root = '/openeo/jobs'
        self._zk = KazooClient(hosts=zookeeper_hosts)

    def ensure_paths(self):
        self._zk.ensure_path(self._ongoing())
        self._zk.ensure_path(self._done())

    def register(self, job_id: str, user_id: str, api_version: str, specification: dict) -> dict:
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
            'created': date_to_rfc3339(datetime.utcnow()),
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
        return BatchJobMetadata(
            id=job_info["job_id"],
            process=specification,
            status=status,
            created=parse_rfc3339(job_info["created"]) if "created" in job_info else None,
            job_options=job_options
        )

    def set_application_id(self, job_id: str, user_id: str, application_id: str) -> None:
        """Updates a registered batch job with its Spark application ID."""

        job_info, version = self._read(job_id, user_id)
        job_info['application_id'] = application_id

        self._update(job_info, version)

    def set_status(self, job_id: str, user_id: str, status: str) -> None:
        """Updates an registered batch job with its status."""

        job_info, version = self._read(job_id, user_id)
        job_info['status'] = status

        self._update(job_info, version)

    def mark_done(self, job_id: str, user_id: str) -> None:
        """Marks a job as done (not to be tracked anymore)."""

        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id, user_id)

        source = self._ongoing(user_id, job_id)

        self._create(job_info, done=True)
        self._zk.delete(source, version)

    def mark_ongoing(self, job_id: str, user_id: str) -> None:
        """Marks as job as ongoing (to be tracked)."""

        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id, user_id, include_done=True)

        source = self._done(user_id, job_id)

        self._create(job_info, done=False)
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
