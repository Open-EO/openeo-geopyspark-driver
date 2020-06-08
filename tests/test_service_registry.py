import json
from unittest import mock

from kazoo.exceptions import NoNodeError
import pytest

from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import ServiceNotFoundException
import openeogeotrellis.service_registry
from openeogeotrellis.service_registry import InMemoryServiceRegistry, SecondaryService, ZooKeeperServiceRegistry

dummy_process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
dummy_service_metadata = ServiceMetadata(
    id='s1234', process={"process_graph": dummy_process_graph},
    url="http://oeo.net/s/1234", type="WMTS", enabled=True, attributes={}
)


class TestInMemoryServiceRegistry:

    def test_get_metadata(self):
        reg = InMemoryServiceRegistry()
        server = mock.Mock()
        reg.register(SecondaryService(service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=server))
        assert reg.get_metadata('s1234') == dummy_service_metadata
        assert reg.get_metadata_all() == {'s1234': dummy_service_metadata}

    def test_stop_service(self):
        reg = InMemoryServiceRegistry()
        server = mock.Mock()
        reg.register(SecondaryService(service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=server))
        # Stop service
        reg.stop_service('s1234')
        server.stop.assert_called_with()
        # service should be gone now
        with pytest.raises(ServiceNotFoundException):
            reg.get_metadata('s1234')
        with pytest.raises(ServiceNotFoundException):
            reg.stop_service('s1234')


class TestZooKeeperServiceRegistry:

    def test_register(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.register(
                SecondaryService(service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=mock.Mock())
            )

        client = KazooClient.return_value
        # print(client.mock_calls)
        client.ensure_path.assert_any_call('/openeo/services')

        assert client.create.call_count == 1
        path, raw = client.create.call_args_list[0][0]
        assert path == '/openeo/services/s1234'
        data = json.loads(raw.decode('utf-8'))
        metadata = data["metadata"]
        assert metadata["id"] == "s1234"
        assert metadata["process"] == {"process_graph": dummy_process_graph}
        assert metadata["url"] == dummy_service_metadata.url
        # TODO: check zookeeper operations for Traefic

    def test_get_metadata(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.register(
                SecondaryService(service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=mock.Mock)
            )
            # Extract "created" data
            client = KazooClient.return_value
            assert client.create.call_count == 1
            path, raw = client.create.call_args_list[0][0]
            # Set up return value for zookeeper "get"
            assert isinstance(raw, bytes)
            storage = {path: raw}
            client.get.side_effect = lambda p: (storage[p], "dummy")
            metadata = reg.get_metadata('s1234')
            assert metadata == dummy_service_metadata

    def test_get_metadata_invalid_service_id(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            client = KazooClient.return_value
            client.get.side_effect = NoNodeError
            with pytest.raises(ServiceNotFoundException, match="Service 'foobar' does not exist."):
                reg.get_metadata('foobar')

    def test_get_metadata_all(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.register(
                SecondaryService(service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=mock.Mock)
            )
            # Extract "created" data
            client = KazooClient.return_value
            assert client.create.call_count == 1
            path, raw = client.create.call_args_list[0][0]
            # Set up return value for zookeeper "get"
            assert isinstance(raw, bytes)
            storage = {path: raw}
            client.get_children.side_effect = lambda r: [path.split('/')[-1]]
            client.get.side_effect = lambda p: (storage[p], "dummy")
            metadata_all = reg.get_metadata_all()
            assert metadata_all == {'s1234': dummy_service_metadata}

    def test_stop_service(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            server = mock.Mock()
            reg.register(
                SecondaryService(service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=server)
            )
            reg.stop_service(dummy_service_metadata.id)

        client = KazooClient.return_value
        # print(client.mock_calls)
        client.delete.assert_called_with('/openeo/services/s1234')
        server.stop.assert_called_once()
        # TODO: check zookeeper operations for Traefic
