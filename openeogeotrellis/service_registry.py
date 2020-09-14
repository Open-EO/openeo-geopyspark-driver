import abc
import contextlib
import json
import logging
from typing import Dict, List, Tuple, NamedTuple
from datetime import datetime

from kazoo.client import KazooClient, NoNodeError

from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import ServiceNotFoundException
from openeogeotrellis.configparams import ConfigParams

_log = logging.getLogger(__name__)


class SecondaryService:
    """Container with information about running secondary service."""
    def __init__(self, host: str, port: int, server):
        self.host = host
        self.port = port
        self.server = server

    def stop(self):
        self.server.stop()
        # TODO check if `.stop()` is enough (e.g. are all Spark RDDs and caches also released properly?)

    def __str__(self):
        return '{c}@{h}:{p}({s})'.format(
            c=self.__class__.__name__, h=self.host, p=self.port, s=self.server
        )


class ServiceEntity(NamedTuple):
    """A secondary service as it is persisted in Zookeeper."""
    metadata: ServiceMetadata
    api_version: str


class AbstractServiceRegistry(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def register(self, service_id: str, service: SecondaryService):
        pass

    @abc.abstractmethod
    def persist(self, user_id: str, metadata: ServiceMetadata, api_version: str):
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

    @abc.abstractmethod
    def get(self, user_id: str, service_id: str) -> ServiceEntity:
        pass


class InMemoryServiceRegistry(AbstractServiceRegistry):
    """
    Simple Service Registry that only keeps services in memory (for testing/debugging).
    """

    def __init__(self, metadatas: Dict[str, Tuple[str, ServiceMetadata]] = None):
        _log.info('Creating new {c}: {s}'.format(c=self.__class__.__name__, s=self))
        self._metadatas = metadatas or {}
        self._services = {}

    def register(self, service_id: str, service: SecondaryService):
        _log.info('Registering service {s}'.format(s=service))
        self._services[service_id] = service

    def persist(self, user_id: str, metadata: ServiceMetadata, api_version: str):
        self._metadatas[metadata.id] = (user_id, metadata)

    def get_metadata(self, user_id: str, service_id: str) -> ServiceMetadata:
        return self.get(user_id=user_id, service_id=service_id).metadata

    def get_metadata_all(self, user_id: str) -> Dict[str, ServiceMetadata]:
        return {sid: metadata for sid, (uid, metadata) in self._metadatas.items() if uid == user_id}

    def get_metadata_all_before(self, upper: datetime) -> List[Tuple[str, ServiceMetadata]]:
        return [(uid, metadata) for (uid, metadata) in self._metadatas.values()
                if not metadata.created or metadata.created < upper]

    def stop_service(self, user_id: str, service_id: str):
        uid, metadata = self._metadatas.get(service_id, (None, None))

        if not metadata or uid != user_id:
            raise ServiceNotFoundException(service_id)

        self._metadatas.pop(service_id)
        service = self._services.pop(service_id)
        _log.info('Stopping service {s}'.format(s=service))
        service.stop()

    def get(self, user_id: str, service_id: str) -> ServiceEntity:
        uid, metadata = self._metadatas.get(service_id, (None, None))

        if not metadata or uid != user_id:
            raise ServiceNotFoundException(service_id)

        return ServiceEntity(metadata, api_version="0.4.0")  # TODO: incorporate api_version in persist()


class ZooKeeperServiceRegistry(AbstractServiceRegistry):  # currently a combination of registry (runtime) and persistent store
    def __init__(self):
        self._root = '/openeo/services'
        self._hosts = ','.join(ConfigParams().zookeepernodes)
        with self._zk_client() as zk:
            zk.ensure_path(self._root)
        # Additional in memory storage of server instances that were registered in current process.
        self._services = {}

    def register(self, service_id: str, service: SecondaryService):
        self._services[service_id] = service

    def persist(self, user_id: str, metadata: ServiceMetadata, api_version: str):
        with self._zk_client() as zk:
            self._persist(zk, user_id, metadata, api_version),

    def _path(self, user_id: str, service_id: str = None):
        return self._root + "/" + user_id + "/" + service_id if service_id else self._root + "/" + user_id

    def get_metadata(self, user_id: str, service_id: str) -> ServiceMetadata:
        with self._zk_client() as zk:
            try:
                return self._load(zk, user_id=user_id, service_id=service_id).metadata
            except NoNodeError:
                try:
                    return self._load(zk, user_id='_anonymous', service_id=service_id).metadata
                except NoNodeError:
                    raise ServiceNotFoundException(service_id)

    def _persist(self, zk: KazooClient, user_id: str, metadata: ServiceMetadata, api_version: str):
        data = {"metadata": metadata.prepare_for_json(), "api_version": api_version}
        raw = json.dumps(data).encode("utf-8")
        zk.create(self._path(user_id=user_id, service_id=metadata.id), raw, makepath=True)

    def _load(self, zk: KazooClient, user_id: str, service_id: str) -> ServiceEntity:
        raw, stat = zk.get(self._path(user_id=user_id, service_id=service_id))
        data = json.loads(raw.decode('utf-8'))
        if "metadata" in data:
            metadata = ServiceMetadata.from_dict(data["metadata"])
            api_version = "0.4.0"
        elif "specification" in data:
            # Old style metadata
            metadata = ServiceMetadata(
                id=service_id,
                process={"process_graph": data["specification"]["process_graph"]},
                type=data["specification"]["type"],
                configuration={},
                url="n/a", enabled=True, attributes={}, created=datetime.utcfromtimestamp(stat.created)
            )
            api_version = data["api_version"]
        else:
            raise ValueError("Failed to load metadata (keys: {k!r})".format(k=data.keys()))

        return ServiceEntity(metadata, api_version)

    def get_metadata_all(self, user_id: str) -> Dict[str, ServiceMetadata]:
        with self._zk_client() as zk:
            def get(user_id: str) -> Dict[str, ServiceMetadata]:
                try:
                    service_ids = zk.get_children(self._path(user_id=user_id))
                    return {service_id: self._load(zk, user_id=user_id, service_id=service_id).metadata for service_id in service_ids}
                except NoNodeError:  # no services for this user yet
                    return {}

            return {**get(user_id), **get('_anonymous')}

    def get_metadata_all_before(self, upper: datetime) -> List[Tuple[str, ServiceMetadata]]:
        services_before = []

        with self._zk_client() as zk:
            for user_id in zk.get_children(self._root):
                for service_id in zk.get_children(self._path(user_id=user_id)):
                    service_metadata = self._load(zk, user_id=user_id, service_id=service_id).metadata
                    service_date = service_metadata.created

                    if not service_date or service_date < upper:
                        _log.debug("service {s}'s service_date {d} is before {u}".format(s=service_id, d=service_date, u=upper))
                        services_before.append((user_id, service_metadata))

        return services_before

    @contextlib.contextmanager
    def _zk_client(self):  # TODO: replace with openeogeotrellis.utils.zk_client()
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
                _log.info("Deleted secondary service {u}/{s}".format(u=user_id, s=service_id))
            except NoNodeError:
                raise ServiceNotFoundException(service_id)
        if service_id in self._services:
            service = self._services.pop(service_id)
            _log.info('Stopping service {i} {s}'.format(i=service_id, s=service))
            service.stop()

    def get(self, user_id: str, service_id: str) -> ServiceEntity:
        with self._zk_client() as zk:
            return self._load(zk, user_id, service_id)
