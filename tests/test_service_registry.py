import json
from datetime import datetime
from unittest import mock

import pytest
from kazoo.exceptions import NoNodeError
from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import ServiceNotFoundException

import openeogeotrellis.service_registry
from openeogeotrellis.service_registry import (
    InMemoryServiceRegistry,
    SecondaryService,
    ZooKeeperServiceRegistry,
)

dummy_process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
dummy_service_metadata = ServiceMetadata(
    id='s1234', process={"process_graph": dummy_process_graph},
    configuration={"version": "0.1.5"},
    url="http://oeo.net/s/1234", type="WMTS", enabled=True, attributes={}, created=datetime(1981, 4, 24, 3, 0, 0)
)


class TestInMemoryServiceRegistry:

    def test_get_metadata(self):
        reg = InMemoryServiceRegistry()
        reg.persist(user_id='u9876', metadata=dummy_service_metadata, api_version="0.4.0")
        assert reg.get_metadata('u9876', 's1234') == dummy_service_metadata
        assert reg.get_metadata_all('u9876') == {'s1234': dummy_service_metadata}

    def test_stop_service(self):
        reg = InMemoryServiceRegistry()
        server = mock.Mock()
        reg.register(dummy_service_metadata.id, SecondaryService(host='oeo.net', port=5678, server=server))
        reg.persist(user_id='u9876', metadata=dummy_service_metadata, api_version="0.4.0")
        # Stop service
        reg.stop_service('u9876', 's1234')
        server.stop.assert_called_with()
        # service should be gone now
        with pytest.raises(ServiceNotFoundException):
            reg.get_metadata('u9876', 's1234')
        with pytest.raises(ServiceNotFoundException):
            reg.stop_service('u9876', 's1234')


class TestZooKeeperServiceRegistry:

    def test_register(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.persist(user_id='u9876', metadata=dummy_service_metadata, api_version="0.4.0")

        client = KazooClient.return_value
        # print(client.mock_calls)
        client.ensure_path.assert_any_call('/openeo/services')

        assert client.create.call_count == 1
        path, raw = client.create.call_args_list[0][0]
        assert path == '/openeo/services/u9876/s1234'
        data = json.loads(raw.decode('utf-8'))
        metadata = data["metadata"]
        assert metadata["id"] == "s1234"
        assert metadata["process"] == {"process_graph": dummy_process_graph}
        assert metadata["url"] == dummy_service_metadata.url

    def test_get_metadata(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.persist(user_id='u9876', metadata=dummy_service_metadata, api_version="0.4.0")

            # Extract "created" data
            client = KazooClient.return_value
            assert client.create.call_count == 1
            path, raw = client.create.call_args_list[0][0]
            # Set up return value for zookeeper "get"
            assert isinstance(raw, bytes)
            storage = {path: raw}
            client.get.side_effect = lambda p: (storage[p], "dummy")
            metadata = reg.get_metadata('u9876', 's1234')
            assert metadata == dummy_service_metadata

    def test_get_metadata_invalid_service_id(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            client = KazooClient.return_value
            client.get.side_effect = NoNodeError
            with pytest.raises(ServiceNotFoundException, match="Service 'foobar' does not exist."):
                reg.get_metadata('u9876', 'foobar')

    def test_get_metadata_all(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.persist(user_id='u9876', metadata=dummy_service_metadata, api_version="0.4.0")

            # Extract "created" data
            client = KazooClient.return_value
            assert client.create.call_count == 1
            path, raw = client.create.call_args_list[0][0]
            # Set up return value for zookeeper "get"
            assert isinstance(raw, bytes)
            storage = {path: raw}
            client.get_children.side_effect = lambda up: [] if up.endswith("/_anonymous") else [path.split('/')[-1]]
            client.get.side_effect = lambda p: (storage[p], "dummy")
            metadata_all = reg.get_metadata_all('u9876')
            assert metadata_all == {'s1234': dummy_service_metadata}

    def test_stop_service(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            server = mock.Mock()
            reg.register(dummy_service_metadata.id, SecondaryService(host='oeo.net', port=5678, server=server))
            reg.persist(user_id='u9876', metadata=dummy_service_metadata, api_version="0.4.0")

            reg.stop_service('u9876', dummy_service_metadata.id)

        client = KazooClient.return_value
        # print(client.mock_calls)
        client.delete.assert_any_call('/openeo/services/u9876/s1234')
        server.stop.assert_called_once()

    def test_get_metadata_all_before(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()

            older_metadata = dummy_service_metadata._replace(created=datetime(1976, 7, 19, 0, 0, 0))
            newer_metadata = dummy_service_metadata._replace(id='s1235', created=datetime(1987, 7, 11, 0, 0, 0))

            reg.persist(user_id='u9876', metadata=older_metadata, api_version="0.4.0")
            reg.persist(user_id='u9876', metadata=newer_metadata, api_version="0.4.0")

            # Extract "created" data
            client = KazooClient.return_value
            assert client.create.call_count == 2

            storage = {call[0][0]: call[0][1] for call in client.create.call_args_list}

            client.get_children.side_effect = lambda p: [older_metadata.id, newer_metadata.id] if p.endswith("/u9876") else ['u9876']
            client.get.side_effect = lambda p: (storage[p], "dummy")

            expired_services = reg.get_metadata_all_before(upper=datetime(1981, 4, 24, 3, 0, 0))

        assert expired_services == [('u9876', older_metadata)]
