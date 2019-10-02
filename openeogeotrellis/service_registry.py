import contextlib
import json
import logging
from typing import Dict

from kazoo.client import KazooClient, NoNodeError

from openeo_driver.errors import ServiceNotFoundException

from openeogeotrellis.configparams import ConfigParams

_log = logging.getLogger(__name__)


class WMTSService:
    """Container with information about running WMTS service."""

    # TODO create an abstract base service class and provide other kind of services too?
    # TODO move the whole `WMTSServer.createServer` creation part also into this class?
    def __init__(self, service_id: str, specification: dict, host: str, port: int, server):
        self.service_id = service_id
        self.specification = specification
        self.host = host
        self.port = port
        self.server = server

    def stop(self):
        self.server.stop()
        # TODO check if `.stop()` is enough (e.g. are all Spark RDDs and caches also released properly?)

    def __str__(self):
        return '{c}[{i}]@{h}:{p}({s})'.format(
            c=self.__class__.__name__, i=self.service_id, h=self.host, p=self.port, s=self.server
        )


class InMemoryServiceRegistry:
    """
    Basic Service Registry that only keeps services in memory.
    Traefik will not be able to expose the service to the outside world.
    """

    # TODO support other services apart from WMTSService?
    # TODO InMemoryServiceRegistry is used as base class for ZooKeeperServiceRegistry, which is not ideal naming-wise.
    #   It is done that way to easily reuse the `stop_service` functionality
    #   without too much overengineering at the moment.
    #   This whole ServiceRegistry needs more refactoring anyway in the longer term,
    #   e.g. to support platforms without Zookeeper or Traefik, or to have full lifecycle management like
    #   restarting secondary services (from persisted metadata) after restart of OpenEO Backend.

    def __init__(self, services: Dict[str, WMTSService] = None):
        _log.info('Creating new {c}: {s}'.format(c=self.__class__.__name__, s=self))
        self._services = services or {}

    def register(self, service: WMTSService):
        _log.info('Registering service {s}'.format(s=service))
        self._services[service.service_id] = service

    def get_specification(self, service_id: str) -> dict:
        if service_id not in self._services:
            raise ServiceNotFoundException(service_id)
        return self._services[service_id].specification

    def get_all_specifications(self) -> Dict[str, dict]:
        return {sid: self.get_specification(sid) for sid in self._services.keys()}

    def stop_service(self, service_id: str):
        if service_id not in self._services:
            raise ServiceNotFoundException(service_id)
        service = self._services.pop(service_id)
        _log.info('Stopping service {s}'.format(s=service))
        service.stop()


class ZooKeeperServiceRegistry(InMemoryServiceRegistry):
    """The idea is that 1) Traefik will use this to map an url to a port and 2) this application will use it
    to map ID's to service details (exposed in the API)."""

    def __init__(self):
        super().__init__()
        self._root = '/openeo/services'
        # TODO: move these hosts to config, argument or constant?
        self._hosts = ','.join(ConfigParams().zookeepernodes)
        with self._zk_client() as zk:
            zk.ensure_path(self._root)

    def register(self, service: WMTSService):
        super().register(service)
        with self._zk_client() as zk:
            self._persist_details(zk, service.service_id, service.specification),
            Traefik(zk).route(service.service_id, service.host, service.port)

    def _persist_details(self, zk: KazooClient, service_id: str, specification: dict):
        # TODO: add more metadata: date, user, ...
        service_info = {
            'specification': specification
        }
        data = json.dumps(service_info).encode()
        zk.create(self._path(service_id), data)

    def _path(self, service_id):
        return self._root + "/" + service_id

    def get_specification(self, service_id: str) -> dict:
        with self._zk_client() as zk:
            return self._load_details(zk, service_id)

    def _load_details(self, zk: KazooClient, service_id: str):
        try:
            data, _ = zk.get(self._path(service_id))
        except NoNodeError:
            raise ServiceNotFoundException(service_id)
        return json.loads(data.decode())

    def get_all_specifications(self) -> Dict[str, dict]:
        with self._zk_client() as zk:
            service_ids = zk.get_children(self._root)
            return {service_id: self._load_details(zk, service_id) for service_id in service_ids}

    @contextlib.contextmanager
    def _zk_client(self):
        zk = KazooClient(hosts=self._hosts)
        zk.start()
        yield zk
        zk.stop()

    def stop_service(self, service_id: str):
        super().stop_service(service_id)
        with self._zk_client() as zk:
            zk.delete(self._path(service_id))
            Traefik(zk).remove(service_id)


class Traefik:
    def __init__(self, zk):
        self._zk = zk

    def route(self, service_id, host, port):
        backend_id = self._create_backend_server(service_id, host, port)
        self._create_frontend_rule(service_id, backend_id)
        self._trigger_configuration_update()

    def remove(self, service_id):
        # TODO
        pass

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
