from unittest import mock

import pytest

from openeo_driver.errors import SecondaryServiceNotFound
from openeogeotrellis import InMemoryServiceRegistry, WMTSService


class TestInMemoryServiceRegistry:

    def test_get_specification(self):
        reg = InMemoryServiceRegistry()
        service_id = 'sid-1234'
        spec = {'process_graph': []}
        server = mock.Mock()
        reg.register(WMTSService(service_id, specification=spec, host='example.com', port=5678, server=server))
        assert reg.get_specification(service_id) == spec
        assert reg.get_all_specifications() == {service_id: spec}

    def test_stop_service(self):
        reg = InMemoryServiceRegistry()
        service_id = 'sid-1234'
        spec = {'process_graph': []}
        server = mock.Mock()
        reg.register(WMTSService(service_id, specification=spec, host='example.com', port=5678, server=server))
        # Stop service
        reg.stop_service(service_id)
        server.stop.assert_called_with()
        # service should be gone now
        with pytest.raises(SecondaryServiceNotFound):
            reg.get_specification(service_id)
        with pytest.raises(SecondaryServiceNotFound):
            reg.stop_service(service_id)
