from typing import Dict
from kazoo.client import KazooClient


class InMemoryServiceRegistry:
    """Keeps the mapping only in memory; Traefik will not be able to expose the service to the outside
    world."""

    def __init__(self):
        self._mapping = {}

    def register(self, service_id: str, specification: Dict, host: str, port: int):
        details = {
            'host': host,
            'port': port,
            'specification': specification
        }

        self._mapping[service_id] = details

    def get(self, service_id) -> Dict:
        return self._mapping[service_id]

    def get_all(self) -> Dict[str, Dict]:
        return self._mapping


class ZooKeeperServiceRegistry:
    """The idea is that 1) Traefik will use this to map an url to a port and 2) this application will use it
    to map ID's to service details (exposed in the API)."""

    def register(self, service_id: str, specification: Dict, host: str, port: int):
        ZooKeeperServiceRegistry._with_zk(lambda zk: (
            self._persist_details(service_id, specification),
            Traefik(zk).route(service_id, host, port)
        ))

    def _persist_details(self, service_id, specification):
        # TODO: persist specification in ZooKeeper
        pass

    @staticmethod
    def _with_zk(callback):
        zk = KazooClient(hosts='epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181')
        zk.start()

        try:
            callback(zk)
        finally:
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

        match_path = "Path:/%s" % service_id
        self._zk.create(test_key + "/rule", match_path.encode())

    def _trigger_configuration_update(self):
        # https://github.com/containous/traefik/issues/2068
        self._zk.delete("/traefik/leader", recursive=True)
