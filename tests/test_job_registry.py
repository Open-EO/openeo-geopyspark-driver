import pytest

from openeo_driver.errors import JobNotFoundException
from openeo_driver.testing import DictSubSet
from openeogeotrellis.job_registry import ZkJobRegistry, InMemoryJobRegistry
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


class TestInMemoryJobRegistry:
    @pytest.fixture(autouse=True)
    def _default_time(self, time_machine):
        time_machine.move_to("2023-02-15T17:17:17Z")

    def test_create_get(self):
        jr = InMemoryJobRegistry()
        jr.create_job(process={"foo": "bar"}, user_id="john", job_id="j-123")
        assert jr.get_job("j-123") == DictSubSet(
            {
                "job_id": "j-123",
                "user_id": "john",
                "created": "2023-02-15T17:17:17Z",
                "process": {"foo": "bar"},
                "status": "created",
                "title": None,
                "updated": "2023-02-15T17:17:17Z",
            }
        )

    def test_get_job_not_found(self):
        jr = InMemoryJobRegistry()
        jr.create_job(process={"foo": "bar"}, user_id="john", job_id="j-123")
        with pytest.raises(JobNotFoundException):
            jr.get_job("j-456")

    def test_list_user_jobs(self):
        jr = InMemoryJobRegistry()
        jr.create_job(process={"foo": 1}, user_id="alice", job_id="j-123")
        jr.create_job(process={"foo": 2}, user_id="bob", job_id="j-456")
        jr.create_job(process={"foo": 3}, user_id="alice", job_id="j-789")
        assert jr.list_user_jobs(user_id="alice") == [
            DictSubSet({"job_id": "j-123", "user_id": "alice", "process": {"foo": 1}}),
            DictSubSet({"job_id": "j-789", "user_id": "alice", "process": {"foo": 3}}),
        ]
        assert jr.list_user_jobs(user_id="bob") == [
            DictSubSet({"job_id": "j-456", "user_id": "bob", "process": {"foo": 2}}),
        ]
        assert jr.list_user_jobs(user_id="charlie") == []
