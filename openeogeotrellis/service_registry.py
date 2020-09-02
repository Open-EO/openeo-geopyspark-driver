import abc
import contextlib
import json
import logging
from typing import Dict, List, Tuple
from datetime import datetime

from kazoo.client import KazooClient, NoNodeError

from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import ServiceNotFoundException
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.traefik import Traefik

_log = logging.getLogger(__name__)


class SecondaryService:
    """Container with information about running secondary service."""
    def __init__(self, user_id: str, service_metadata: ServiceMetadata, host: str, port: int, server):
        self.user_id = user_id
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
    def get_metadata(self, user_id: str, service_id: str) -> ServiceMetadata:
        pass

    @abc.abstractmethod
    def get_metadata_all(self, user_id: str) -> Dict[str, ServiceMetadata]:  # TODO: what's the purpose of the service_id key?
        pass

    @abc.abstractmethod
    def get_metadata_all_before(self, upper: datetime) -> List[Tuple[str, ServiceMetadata]]:  # (user_id, metadata)
        pass

    @abc.abstractmethod
    def stop_service(self, user_id: str, service_id: str):
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

    def get_metadata(self, user_id: str, service_id: str) -> ServiceMetadata:
        service = self._services.get(service_id)

        if not service or service.user_id != user_id:
            raise ServiceNotFoundException(service_id)

        return service.service_metadata

    def get_metadata_all(self, user_id: str) -> Dict[str, ServiceMetadata]:
        return {sid: service.service_metadata for sid, service in self._services.items() if service.user_id == user_id}

    def get_metadata_all_before(self, upper: datetime) -> List[Tuple[str, ServiceMetadata]]:
        return [(s.user_id, s.service_metadata) for s in self._services.values()
                if not s.service_metadata.created or s.service_metadata.created < upper]

    def stop_service(self, user_id: str, service_id: str):
        service = self._services.get(service_id)

        if not service or service.user_id != user_id:
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
            self._persist_metadata(zk, service.user_id, service.service_metadata),
            Traefik(zk).proxy_service(service.service_id, service.host, service.port)

    def _path(self, user_id: str, service_id: str = None):
        return self._root + "/" + user_id + "/" + service_id if service_id else self._root + "/" + user_id

    def get_metadata(self, user_id: str, service_id: str) -> ServiceMetadata:
        with self._zk_client() as zk:
            try:
                return self._load_metadata(zk, user_id=user_id, service_id=service_id)
            except NoNodeError:
                try:
                    return self._load_metadata(zk, user_id='_anonymous', service_id=service_id)
                except NoNodeError:
                    raise ServiceNotFoundException(service_id)

    def _persist_metadata(self, zk: KazooClient, user_id: str, metadata: ServiceMetadata):
        data = {"metadata": metadata.prepare_for_json()}
        raw = json.dumps(data).encode("utf-8")
        zk.create(self._path(user_id=user_id, service_id=metadata.id), raw, makepath=True)

    def _load_metadata(self, zk: KazooClient, user_id: str, service_id: str) -> ServiceMetadata:
        raw, stat = zk.get(self._path(user_id=user_id, service_id=service_id))
        data = json.loads(raw.decode('utf-8'))
        if "metadata" in data:
            return ServiceMetadata.from_dict(data["metadata"])
        elif "specification" in data:
            # Old style metadata
            return ServiceMetadata(
                id=service_id,
                process={"process_graph": data["specification"]["process_graph"]},
                type=data["specification"]["type"],
                configuration={},
                url="n/a", enabled=True, attributes={}, created=datetime.utcfromtimestamp(stat.created)
            )
        else:
            raise ValueError("Failed to load metadata (keys: {k!r})".format(k=data.keys()))

    def get_metadata_all(self, user_id: str) -> Dict[str, ServiceMetadata]:
        with self._zk_client() as zk:
            def get(user_id: str) -> Dict[str, ServiceMetadata]:
                try:
                    service_ids = zk.get_children(self._path(user_id=user_id))
                    return {service_id: self._load_metadata(zk, user_id=user_id, service_id=service_id) for service_id in service_ids}
                except NoNodeError:  # no services for this user yet
                    return {}

            return {**get(user_id), **get('_anonymous')}

    def get_metadata_all_before(self, upper: datetime) -> List[Tuple[str, ServiceMetadata]]:
        services_before = []

        with self._zk_client() as zk:
            for user_id in zk.get_children(self._root):
                for service_id in zk.get_children(self._path(user_id=user_id)):
                    service_metadata = self._load_metadata(zk, user_id=user_id, service_id=service_id)
                    service_date = service_metadata.created

                    if not service_date or service_date < upper:
                        _log.debug("service {s}'s service_date {d} is before {u}".format(s=service_id, d=service_date, u=upper))
                        services_before.append((user_id, service_metadata))

        return services_before

    @contextlib.contextmanager
    def _zk_client(self):
        zk = KazooClient(hosts=self._hosts)
        zk.start()

        try:
            yield zk
        finally:
            zk.stop()
            zk.close()

    def stop_service(self, user_id: str, service_id: str):
        with self._zk_client() as zk:
            try:
                zk.delete(self._path(user_id=user_id, service_id=service_id))
                Traefik(zk).unproxy_service(service_id)
                _log.info("Deleted secondary service {u}/{s}".format(u=user_id, s=service_id))
            except NoNodeError:
                raise ServiceNotFoundException(service_id)
        if service_id in self._services:
            service = self._services.pop(service_id)
            _log.info('Stopping service {s}'.format(s=service))
            service.stop()
