from typing import Dict
from kazoo.client import KazooClient, NoNodeError

import json

from openeo_driver.errors import SecondaryServiceNotFound


class InMemoryServiceRegistry:
    """Keeps the mapping only in memory; Traefik will not be able to expose the service to the outside
    world."""

    def __init__(self):
        self._mapping = {}

    def register(self, service_id: str, specification: Dict, host: str, port: int):
        service_info = {
            'specification': specification
        }

        self._mapping[service_id] = service_info

    def get(self, service_id) -> Dict:
        return self._mapping[service_id]

    def get_all(self) -> Dict[str, Dict]:
        return self._mapping


class ZooKeeperServiceRegistry:
    """The idea is that 1) Traefik will use this to map an url to a port and 2) this application will use it
    to map ID's to service details (exposed in the API)."""

    def __init__(self):
        self._root = '/openeo/services'
        # TODO: move these hosts to config, argument or constant?
        self._hosts = 'epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181'
        with self._zk_client() as zk:
            zk.ensure_path(self._root)

    def register(self, service_id: str, specification: Dict, host: str, port: int):
        with self._zk_client() as zk:
            self._persist_details(zk, service_id, specification),
            Traefik(zk).route(service_id, host, port)

    def _persist_details(self, zk, service_id, specification):
        service_info = {
            'specification': specification
        }

        data = json.dumps(service_info).encode()
        zk.create(self._path(service_id), data)

    def _path(self, service_id):
        return self._root + "/" + service_id

    def get(self, service_id) -> Dict:
        with self._zk_client() as zk:
            return self._load_details(zk, service_id)

    def _load_details(self, zk: KazooClient, service_id):
        try:
            data, _ = zk.get(self._path(service_id))
        except NoNodeError:
            raise SecondaryServiceNotFound(service_id)
        return json.loads(data.decode())

    def get_all(self) -> Dict[str, Dict]:
        with self._zk_client() as zk:
            service_ids = zk.get_children(self._root)
            return {service_id: self._load_details(zk, service_id) for service_id in service_ids}

    @contextlib.contextmanager
    def _zk_client(self):
        zk = KazooClient(hosts=self._hosts)
        zk.start()
        yield zk
        zk.stop()


class Traefik:
    def __init__(self, zk):
        self._zk = zk

    def route(self, service_id, host, port):
        backend_id = self._create_backend_server(service_id, host, port)
        self._create_frontend_rule(service_id, backend_id)
        self._trigger_configuration_update()

    def _create_backend_server(self, service_id, host, port):
        backend_id = "backend%s" % service_id
        server_key = "/traefik/backends/%s/servers/server1" % backend_id
        self._zk.ensure_path(server_key)

        url = "http://%s:%d" % (host, port)
        self._zk.create(server_key + "/url", url.encode())

        return backend_id

    def _create_frontend_rule(self, service_id, backend_id):
        frontend_key = "/traefik/frontends/frontend%s" % service_id
        test_key = frontend_key + "/routes/test"
        self._zk.ensure_path(test_key)

        self._zk.create(frontend_key + "/entrypoints", b"web")
        self._zk.create(frontend_key + "/backend", backend_id.encode())

        match_path = "PathPrefixStripRegex: /openeo/services/%s,/openeo/{version}/services/%s" % (service_id, service_id)
        self._zk.create(test_key + "/rule", match_path.encode())

    def _trigger_configuration_update(self):
        # https://github.com/containous/traefik/issues/2068
        self._zk.delete("/traefik/leader", recursive=True)
