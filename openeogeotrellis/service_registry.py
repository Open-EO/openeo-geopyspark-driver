import abc
import contextlib
import json
import logging
from typing import Dict

from kazoo.client import KazooClient, NoNodeError

from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import ServiceNotFoundException
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.traefik import Traefik

_log = logging.getLogger(__name__)


class SecondaryService:
    """Container with information about running secondary service."""
    def __init__(self, service_metadata: ServiceMetadata, host: str, port: int, server):
        self.service_metadata = service_metadata
        self.host = host
        self.port = port
        self.server = server

    @property
    def service_id(self):
        return self.service_metadata.id

    def stop(self):
        self.server.stop()
        # TODO check if `.stop()` is enough (e.g. are all Spark RDDs and caches also released properly?)

    def __str__(self):
        return '{c}[{i}]@{h}:{p}({s})'.format(
            c=self.__class__.__name__, i=self.service_id, h=self.host, p=self.port, s=self.server
        )


class AbstractServiceRegistry(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def register(self, service: SecondaryService):
        pass

    @abc.abstractmethod
    def get_metadata(self, service_id: str) -> ServiceMetadata:
        pass

    def get_metadata_all(self) -> Dict[str, ServiceMetadata]:
        pass

    @abc.abstractmethod
    def stop_service(self, service_id: str):
        pass


class InMemoryServiceRegistry(AbstractServiceRegistry):
    """
    Simple Service Registry that only keeps services in memory (for testing/debugging).
    """

    def __init__(self, services: Dict[str, SecondaryService] = None):
        _log.info('Creating new {c}: {s}'.format(c=self.__class__.__name__, s=self))
        self._services = services or {}

    def register(self, service: SecondaryService):
        _log.info('Registering service {s}'.format(s=service))
        self._services[service.service_id] = service

    def get_metadata(self, service_id: str) -> ServiceMetadata:
        if service_id not in self._services:
            raise ServiceNotFoundException(service_id)
        return self._services[service_id].service_metadata

    def get_metadata_all(self) -> Dict[str, ServiceMetadata]:
        return {sid: self.get_metadata(sid) for sid in self._services.keys()}

    def stop_service(self, service_id: str):
        if service_id not in self._services:
            raise ServiceNotFoundException(service_id)
        service = self._services.pop(service_id)
        _log.info('Stopping service {s}'.format(s=service))
        service.stop()


class ZooKeeperServiceRegistry(AbstractServiceRegistry):
    """The idea is that 1) Traefik will use this to map an url to a port and 2) this application will use it
    to map ID's to service details (exposed in the API)."""

    def __init__(self):
        self._root = '/openeo/services'
        self._hosts = ','.join(ConfigParams().zookeepernodes)
        with self._zk_client() as zk:
            zk.ensure_path(self._root)
        # Additional in memory storage of server instances that were registered in current process.
        self._services = {}

    def register(self, service: SecondaryService):
        with self._zk_client() as zk:
            self._services[service.service_id] = service
            self._persist_metadata(zk, service.service_metadata),
            Traefik(zk).proxy_service(service.service_id, service.host, service.port)

    def _path(self, service_id):
        return self._root + "/" + service_id

    def get_metadata(self, service_id: str) -> ServiceMetadata:
        with self._zk_client() as zk:
            return self._load_metadata(zk, service_id)

    def _persist_metadata(self, zk: KazooClient, metadata: ServiceMetadata):
        data = {"metadata": metadata.prepare_for_json()}
        raw = json.dumps(data).encode("utf-8")
        zk.create(self._path(service_id=metadata.id), raw)

    def _load_metadata(self, zk: KazooClient, service_id: str) -> ServiceMetadata:
        try:
            raw, _ = zk.get(self._path(service_id))
        except NoNodeError:
            raise ServiceNotFoundException(service_id)
        data = json.loads(raw.decode('utf-8'))
        if "metadata" in data:
            return ServiceMetadata.from_dict(data["metadata"])
        elif "specification" in data:
            # Old style metadata
            return ServiceMetadata(
                id=service_id,
                process={"process_graph": data["specification"]["process_graph"]},
                type=data["specification"]["type"],
                url="n/a", enabled=True, attributes={}
            )
        else:
            raise ValueError("Failed to load metadata (keys: {k!r})".format(k=data.keys()))

    def get_metadata_all(self) -> Dict[str, ServiceMetadata]:
        with self._zk_client() as zk:
            service_ids = zk.get_children(self._root)
            return {service_id: self._load_metadata(zk, service_id) for service_id in service_ids}

    @contextlib.contextmanager
    def _zk_client(self):
        zk = KazooClient(hosts=self._hosts)
        zk.start()

        try:
            yield zk
        finally:
            zk.stop()
            zk.close()

    def stop_service(self, service_id: str):
        with self._zk_client() as zk:
            zk.delete(self._path(service_id))
            Traefik(zk).remove(service_id)
        if service_id in self._services:
            self._services.pop(service_id).stop()
