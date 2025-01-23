import pystac
import unittest
from unittest.mock import patch, Mock

import pystac_client
from openeogeotrellis.integrations.stac import StacApiIO

class TestStacClient(unittest.TestCase):

    @patch('requests.Session')
    def test_stac_client_timeout(self, mock_session):
        timeout_value = 42
        mock_session.send.return_value.status_code = 400
        mock_request = Mock()
        mock_session.prepare_request.return_value = mock_request

        stac_io = StacApiIO(timeout=timeout_value)
        stac_io.session = mock_session

        stac_object = pystac.read_file(href="collection.json", stac_io=stac_io)
        with self.assertRaises(pystac_client.exceptions.APIError):
            print(stac_object.get_root().id)

        mock_session.send.assert_called_with(mock_request, timeout=timeout_value)
