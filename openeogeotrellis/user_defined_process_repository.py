import contextlib
import json
from typing import List, Dict
from typing import Union

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

from openeo_driver.backend import UserDefinedProcessMetadata, UserDefinedProcesses
from openeo_driver.errors import ProcessGraphNotFoundException
from openeogeotrellis.configparams import ConfigParams


class ZooKeeperUserDefinedProcessRepository(UserDefinedProcesses):
    def __init__(self):
        self._hosts = ','.join(ConfigParams().zookeepernodes)
        self._root = "/openeo/udps"

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

    def get_for_user(self, user_id: str) -> List[UserDefinedProcessMetadata]:
        with self._zk_client() as zk:
            user_path = "{r}/{u}".format(r=self._root, u=user_id)
            try:
                process_graph_ids = zk.get_children(user_path)

                udps = (self.get(user_id, process_graph_id) for process_graph_id in process_graph_ids)
                return sorted(udps, key=lambda udp: udp.id.lower())
            except NoNodeError:
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
        zk = KazooClient(hosts=self._hosts)
        zk.start()

        try:
            yield zk
        finally:
            zk.stop()
            zk.close()


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
    repo = ZooKeeperUserDefinedProcessRepository()

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
