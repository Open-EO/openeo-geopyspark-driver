import pytest

from openeo_driver.testing import DictSubSet
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.testing import KazooClientMock


@pytest.fixture
def zk_client() -> KazooClientMock:
    return KazooClientMock()


def test_basic(zk_client):
    zjr = ZkJobRegistry(zk_client=zk_client)
    zjr.register(
        job_id="j123", user_id="u456", api_version="1.2.3", specification={"foo": "bar"}
    )

    data = zk_client.get_json_decoded("/openeo.test/jobs/ongoing/u456/j123")
    assert data == DictSubSet(
        user_id="u456", job_id="j123", specification='{"foo": "bar"}', status="created"
    )


@pytest.mark.parametrize(
    ["root_path", "path"],
    [
        ("/oeo.test/jobs", "/oeo.test/jobs/ongoing/u456/j123"),
        ("/oeo/test/jobs/", "/oeo/test/jobs/ongoing/u456/j123"),
    ],
)
def test_root_path(zk_client, root_path, path):
    zjr = ZkJobRegistry(zk_client=zk_client, root_path=root_path)
    zjr.register(
        job_id="j123", user_id="u456", api_version="1.2.3", specification={"foo": "bar"}
    )

    assert zk_client.get_json_decoded(path) == DictSubSet(user_id="u456", job_id="j123")
