from typing import List, Dict
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
import json

from openeogeotrellis.configparams import ConfigParams

class JobRegistry:
    def __init__(self, zookeeper_hosts: str=','.join(ConfigParams().zookeepernodes)):
        self._root = '/openeo/jobs'
        self._zk = KazooClient(hosts=zookeeper_hosts)

    def register(self, job_id: str, api_version: str, specification: Dict) -> None:
        """Registers a to-be-run batch job."""

        job_info = {
            'job_id': job_id,
            'status': 'submitted',
            'api_version': api_version,
            'specification': json.dumps(specification),
            'application_id': None,
        }

        self._create(job_info)

    def set_application_id(self, job_id: str, application_id: str) -> None:
        """Updates a registered batch job with its Spark application ID."""

        job_info, version = self._read(job_id)
        job_info['application_id'] = application_id

        self._update(job_info, version)

    def set_status(self, job_id: str, status: str) -> None:
        """Updates an registered batch job with its status."""

        job_info, version = self._read(job_id)
        job_info['status'] = status

        self._update(job_info, version)

    def update(self, job_id: str, **kwargs: str) -> None:
        """Updates a registered batch job."""

        job_info, version = self._read(job_id)
        job_info.update(kwargs)

        self._update(job_info, version)

    def mark_done(self, job_id):
        # FIXME: can be done in a transaction
        job_info, version = self._read(job_id)

        source = self._ongoing(job_id)

        self._create(job_info, done=True)
        self._zk.delete(source, version)

    def get_running_jobs(self) -> List[str]:
        """Returns a list of jobs that are currently not finished (should still be tracked)."""

        ongoing_job_ids = self._zk.get_children(self._ongoing())

        return [self.get_job(job_id) for job_id in ongoing_job_ids]

    def get_job(self, job_id: str) -> Dict:
        """Returns details of a job."""

        job_info, _ = self._read(job_id, include_done=True)
        return job_info

    def __enter__(self) -> 'JobRegistry':
        self._zk.start()
        return self

    def __exit__(self, *_):
        self._zk.stop()

    def _create(self, job_info: Dict, done: bool=False) -> None:
        path = self._done(job_info['job_id']) if done else self._ongoing(job_info['job_id'])
        data = json.dumps(job_info).encode()

        self._zk.create(path, data)

    def _read(self, job_id: str, include_done=False) -> (Dict, int):
        try:
            path = self._ongoing(job_id)
            data, stat = self._zk.get(path)
        except NoNodeError:
            if include_done:
                path = self._done(job_id)
                data, stat = self._zk.get(path)
            else:
                raise

        return json.loads(data.decode()), stat.version

    def _update(self, job_info: Dict, version: int) -> None:
        path = self._ongoing(job_info['job_id'])
        data = json.dumps(job_info).encode()

        self._zk.set(path, data, version)

    def _ongoing(self, job_id: str=None) -> str:
        return self._root + "/ongoing/" + job_id if job_id else self._root + "/ongoing"

    def _done(self, job_id: str) -> str:
        return self._root + "/done/" + job_id
