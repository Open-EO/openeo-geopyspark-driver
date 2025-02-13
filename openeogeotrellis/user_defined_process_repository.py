import contextlib
import json
import logging
from typing import List, Dict
from typing import Union

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.retry import KazooRetry

from openeo_driver.backend import UserDefinedProcessMetadata, UserDefinedProcesses
from openeo_driver.errors import ProcessGraphNotFoundException
from openeogeotrellis.configparams import ConfigParams


class ZooKeeperUserDefinedProcessRepository(UserDefinedProcesses):
    # TODO: encode user id before using in zookeeper path (it could contain characters that don't play nice)
    # TODO: include version number in payload to allow schema updates?

    _log = logging.getLogger(__name__)

    def __init__(self, hosts: List[str], root: str = "/openeo/udps", zk_client_reuse: bool = False):
        self._hosts = ",".join(hosts)
        self._root = root
        self._zk_client_reuse = zk_client_reuse
        self._zk_client_cache = None

    @staticmethod
    def _serialize(spec: dict) -> bytes:
        return json.dumps({
            'specification': spec
        }).encode()

    @staticmethod
    def _deserialize(data: bytes) -> dict:
        return json.loads(data.decode())

    def save(self, user_id: str, process_id: str, spec: dict) -> None:
        with self._zk_client() as zk:
            udp_path = "{r}/{u}/{p}".format(r=self._root, u=user_id, p=spec['id'])
            data = self._serialize(spec)

            try:
                zk.create(udp_path, data, makepath=True)
            except NodeExistsError:
                _, stat = zk.get(udp_path)
                zk.set(udp_path, data, version=stat.version)

    def get(self, user_id: str, process_id: str) -> Union[UserDefinedProcessMetadata, None]:
        with self._zk_client() as zk:
            udp_path = "{r}/{u}/{p}".format(r=self._root, u=user_id, p=process_id)
            try:
                data, _ = zk.get(udp_path)
                return UserDefinedProcessMetadata.from_dict(self._deserialize(data)['specification'])
            except NoNodeError:
                return None
            except KazooTimeoutError:
                self._log.error(f"Timeout while checking for user defined process {process_id} for user {user_id}")
                return None

    def get_for_user(self, user_id: str) -> List[UserDefinedProcessMetadata]:
        with self._zk_client() as zk:
            user_path = "{r}/{u}".format(r=self._root, u=user_id)
            try:
                process_graph_ids = zk.get_children(user_path)

                udps = (self.get(user_id, process_graph_id) for process_graph_id in process_graph_ids)
                return sorted(udps, key=lambda udp: udp.id.lower())
            except NoNodeError:
                return []
            except KazooTimeoutError:
                self._log.error(f"Timeout while looking up user defined processes for user {user_id}")
                return []

    def delete(self, user_id: str, process_id: str) -> None:
        with self._zk_client() as zk:
            udp_path = "{r}/{u}/{p}".format(r=self._root, u=user_id, p=process_id)

            try:
                zk.delete(udp_path)
            except NoNodeError:
                raise ProcessGraphNotFoundException(process_graph_id=process_id)

    @contextlib.contextmanager
    def _zk_client(self):
        create_new_client = (not self._zk_client_reuse) or (self._zk_client_cache is None)
        if create_new_client:
            kz_retry = KazooRetry(max_tries=10, delay=0.5, backoff=2)
            zk = KazooClient(
                hosts=self._hosts,
                connection_retry=kz_retry,
                command_retry=kz_retry,
                timeout=3.0,
            )
            zk.start(timeout=15.0)
            if self._zk_client_reuse:
                self._zk_client_cache = zk
        else:
            zk = self._zk_client_cache

        try:
            yield zk
        finally:
            if not self._zk_client_reuse:
                zk.stop()
                zk.close()

    def __del__(self):
        if self._zk_client_cache:
            self._zk_client_cache.stop()
            self._zk_client_cache.close()
            self._zk_client_cache = None


class InMemoryUserDefinedProcessRepository(UserDefinedProcesses):
    def __init__(self):
        self._store: Dict[str, Dict[str, UserDefinedProcessMetadata]] = {}

    def save(self, user_id: str, process_id: str, spec: dict) -> None:
        user_udps = self._store.get(user_id, {})
        new_udp = UserDefinedProcessMetadata.from_dict(spec)
        user_udps[new_udp.id] = new_udp
        self._store[user_id] = user_udps

    def get(self, user_id: str, process_id: str) -> Union[UserDefinedProcessMetadata, None]:
        user_udps = self._store.get(user_id, {})
        return user_udps.get(process_id)

    def get_for_user(self, user_id: str) -> List[UserDefinedProcessMetadata]:
        user_udps = self._store.get(user_id, {})
        return list(user_udps.values())

    def delete(self, user_id: str, process_id: str) -> None:
        user_udps = self._store.get(user_id, {})

        try:
            user_udps.pop(process_id)
        except KeyError:
            raise ProcessGraphNotFoundException(process_id)


def main():
    repo = ZooKeeperUserDefinedProcessRepository(hosts=ConfigParams().zookeepernodes)

    user_id = 'vdboschj'
    process_graph_id = 'evi'
    udp_spec = {
        'id': process_graph_id,
        'process_graph': {
            'loadcollection1': {
                'process_id': 'load_collection',
                'arguments': {
                    'id': 'PROBAV_L3_S10_TOC_NDVI_333M'
                }
            }
        }
    }

    repo.save(user_id=user_id, process_id=process_graph_id, spec=udp_spec)

    udps = repo.get_for_user(user_id)

    for udp in udps:
        print(udp)

    repo.delete(user_id, process_graph_id)

    print(repo.get(user_id, process_graph_id))


if __name__ == '__main__':
    main()
