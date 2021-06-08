from openeo_driver.errors import OpenEOApiException, ProcessGraphMissingException
import pytest


def test_valid_udp(udps):
    udps.save(user_id='user_id', process_id='abc_123', spec={'process_graph': {}})


def test_process_graph_missing(udps):
    with pytest.raises(ProcessGraphMissingException):
        udps.save(user_id='user_id', process_id='abc_123', spec={})


def test_invalid_process_graph_id(udps):
    with pytest.raises(OpenEOApiException) as exc_info:
        udps.save(user_id='user_id', process_id='user/abc_123', spec={'process_graph': {}})

    assert exc_info.value.status_code == 400
