import json
from unittest import mock

from kazoo.exceptions import NoNodeError
import pytest
from datetime import datetime

from openeo_driver.backend import ServiceMetadata
from openeo_driver.errors import ServiceNotFoundException
import openeogeotrellis.service_registry
from openeogeotrellis.service_registry import InMemoryServiceRegistry, SecondaryService, ZooKeeperServiceRegistry

dummy_process_graph = {"foo": {"process_id": "foo", "arguments": {}}}
dummy_service_metadata = ServiceMetadata(
    id='s1234', process={"process_graph": dummy_process_graph},
    configuration={"version": "0.1.5"},
    url="http://oeo.net/s/1234", type="WMTS", enabled=True, attributes={}
)


class TestInMemoryServiceRegistry:

    def test_get_metadata(self):
        reg = InMemoryServiceRegistry()
        server = mock.Mock()
        reg.register(SecondaryService(user_id='u9876', service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=server))
        assert reg.get_metadata('u9876', 's1234') == dummy_service_metadata
        assert reg.get_metadata_all('u9876') == {'s1234': dummy_service_metadata}

    def test_stop_service(self):
        reg = InMemoryServiceRegistry()
        server = mock.Mock()
        reg.register(SecondaryService(user_id='u9876', service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=server))
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
            reg.register(
                SecondaryService(user_id='u9876',
                                 service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=mock.Mock())
            )

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

        client.set.assert_any_call('/traefik/backends/backends1234/servers/server1/url', b'http://oeo.net:5678')
        client.set.assert_any_call('/traefik/frontends/frontends1234/backend', b'backends1234')
        client.set.assert_any_call('/traefik/frontends/frontends1234/entrypoints', b'web')
        client.set.assert_any_call('/traefik/frontends/frontends1234/priority', b'200')
        client.set.assert_any_call('/traefik/frontends/frontends1234/routes/test/rule',
                                   b'PathPrefixStripRegex: /openeo/services/s1234,/openeo/{version}/services/s1234')

    def test_get_metadata(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()
            reg.register(
                SecondaryService(
                    user_id='u9876',
                    service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=mock.Mock)
            )
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
            reg.register(
                SecondaryService(
                    user_id='u9876',
                    service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=mock.Mock)
            )
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
            reg.register(
                SecondaryService(
                    user_id='u9876',
                    service_metadata=dummy_service_metadata, host='oeo.net', port=5678, server=server)
            )
            reg.stop_service('u9876', dummy_service_metadata.id)

        client = KazooClient.return_value
        # print(client.mock_calls)
        client.delete.assert_any_call('/openeo/services/u9876/s1234')
        client.delete.assert_any_call('/traefik/backends/backends1234', recursive=True)
        client.delete.assert_any_call('/traefik/frontends/frontends1234', recursive=True)
        server.stop.assert_called_once()

    def test_get_metadata_all_before(self):
        with mock.patch.object(openeogeotrellis.service_registry, 'KazooClient') as KazooClient:
            reg = ZooKeeperServiceRegistry()

            older_metadata = dummy_service_metadata._replace(created=datetime(1976, 7, 19, 0, 0, 0))
            newer_metadata = dummy_service_metadata._replace(id='s1235', created=datetime(1987, 7, 11, 0, 0, 0))

            reg.register(
                SecondaryService(
                    user_id='u9876',
                    service_metadata=older_metadata, host='oeo.net', port=5678, server=mock.Mock)
            )

            reg.register(
                SecondaryService(
                    user_id='u9876',
                    service_metadata=newer_metadata, host='oeo.net', port=5678, server=mock.Mock)
            )

            # Extract "created" data
            client = KazooClient.return_value
            assert client.create.call_count == 2

            storage = {call[0][0]: call[0][1] for call in client.create.call_args_list}

            client.get_children.side_effect = lambda p: [older_metadata.id, newer_metadata.id] if p.endswith("/u9876") else ['u9876']
            client.get.side_effect = lambda p: (storage[p], "dummy")

            expired_services = reg.get_metadata_all_before(upper=datetime(1981, 4, 24, 3, 0, 0))

        assert expired_services == [('u9876', older_metadata)]
