import logging
import datetime
import pytest
from unittest import mock

from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet
from openeogeotrellis.job_registry import ZkJobRegistry, InMemoryJobRegistry, DoubleJobRegistry, get_dependency_sources
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
    ["job_info", "expected"],
    [
        ({}, []),
        (
            {
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ]
            },
            ["s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83"],
        ),
        (
            {"dependencies": [{"batch_request_id": "98029-652-3423"}]},
            ["s3://openeo-sentinelhub/98029-652-3423"],
        ),
        (
            {"dependencies": [{"subfolder": "foo", "batch_request_id": "98029-652-3423"}, {"subfolder": "bar"}]},
            [
                "s3://openeo-sentinelhub/foo",
                "s3://openeo-sentinelhub/bar",
            ],
        ),
        (
            {
                "dependencies": [
                    {
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                        "assembled_location": "s3://foo/bar",
                    }
                ]
            },
            [
                "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                "s3://foo/bar",
            ],
        ),
    ],
)
def test_get_dependency_sources(job_info, expected):
    assert get_dependency_sources(job_info) == expected


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


class TestDoubleJobRegistry:
    DUMMY_PROCESS = {
        "title": "dummy",
        "process_graph": {
            "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
        },
    }

    @pytest.fixture(autouse=True)
    def _default_time(self, time_machine):
        time_machine.move_to("2023-02-15T17:17:17Z")

    @pytest.fixture
    def zk_client(self) -> KazooClientMock:
        zk_client = KazooClientMock()
        with mock.patch(
            "openeogeotrellis.job_registry.KazooClient", return_value=zk_client
        ):
            yield zk_client

    @pytest.fixture
    def memory_jr(self) -> InMemoryJobRegistry:
        return InMemoryJobRegistry()

    @pytest.fixture
    def double_jr(self, zk_client, memory_jr) -> DoubleJobRegistry:
        return DoubleJobRegistry(
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)),
            elastic_job_registry=memory_jr,
        )

    @pytest.fixture
    def double_jr_no_zk(self, memory_jr) -> DoubleJobRegistry:
        return DoubleJobRegistry(
            zk_job_registry_factory=(lambda: None),
            elastic_job_registry=memory_jr,
        )

    def test_repr(self, double_jr):
        assert repr(double_jr) == "<DoubleJobRegistry NoneType+InMemoryJobRegistry>"

    def test_context_repr(self, double_jr, caplog):
        caplog.set_level(logging.DEBUG)
        with double_jr:
            pass
        assert "Context enter <DoubleJobRegistry ZkJobRegistry+InMemoryJobRegistry>" in caplog.text

    def test_create_job(self, double_jr, zk_client, memory_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )

        assert zk_client.get_json_decoded("/openeo.test/jobs/ongoing/john/j-123") == {
            "job_id": "j-123",
            "user_id": "john",
            "specification": '{"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}}',
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": None,
            "description": None,
        }
        assert memory_jr.db["j-123"] == {
            "job_id": "j-123",
            "user_id": "john",
            "process": self.DUMMY_PROCESS,
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": None,
            "description": None,
            "job_options": None,
            "parent_id": None,
        }

    def test_create_job_ejr_fail_just_log_errors(self, double_jr, zk_client, memory_jr, caplog, monkeypatch):
        """Check `_just_log_errors` + "job_id" extra feature with broken memory_jr"""

        class Formatter:
            """Custom formatter to include "job_id" extra"""

            def format(self, record: logging.LogRecord):
                job_id = getattr(record, "job_id", None)
                return f"[{job_id}] {record.levelname} {record.message}"

        monkeypatch.setattr(caplog.handler, "formatter", Formatter())

        with mock.patch.object(
            memory_jr, "create_job", side_effect=RuntimeError("Nope!")
        ):
            with double_jr:
                double_jr.create_job(
                    job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
                )
        zk_result = zk_client.get_json_decoded("/openeo.test/jobs/ongoing/john/j-123")
        assert zk_result == DictSubSet({"job_id": "j-123"})
        assert memory_jr.db == {}
        expected = "[j-123] WARNING In context 'DoubleJobRegistry.create_job': caught RuntimeError('Nope!')\n"
        assert caplog.text == expected

    def test_create_job_no_zk(self, double_jr_no_zk, zk_client, memory_jr):
        with double_jr_no_zk:
            double_jr_no_zk.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)

        assert zk_client.dump() == {"/": b""}
        assert memory_jr.db["j-123"] == {
            "job_id": "j-123",
            "user_id": "john",
            "process": self.DUMMY_PROCESS,
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": None,
            "description": None,
            "job_options": None,
            "parent_id": None,
        }

    def test_get_job(self, double_jr, caplog):
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
                job_options={"prio": "low"},
                title="John's job",
            )
            job = double_jr.get_job("j-123", user_id="john")
            job_metadata = double_jr.get_job_metadata("j-123", user_id="john")
        assert job == {
            "job_id": "j-123",
            "user_id": "john",
            "specification": '{"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}, "job_options": {"prio": "low"}}',
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": "John's job",
            "description": None,
        }
        assert job_metadata == BatchJobMetadata(
            id="j-123",
            status="created",
            created=datetime.datetime(2023, 2, 15, 17, 17, 17),
            process={"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}},
            job_options={"prio": "low"},
            title="John's job",
            description=None,
            updated=datetime.datetime(2023, 2, 15, 17, 17, 17),
            started=None,
            finished=None,
        )

        assert caplog.messages == []

    def test_get_job_mismatch(self, double_jr, memory_jr, caplog):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )
            memory_jr.db["j-123"]["status"] = "c0rRupt"
            job = double_jr.get_job("j-123", user_id="john")
        assert job == DictSubSet({"job_id": "j-123", "status": "created"})
        assert caplog.messages == [
            "DoubleJobRegistry mismatch"
            " zk_job_info={'job_id': 'j-123', 'status': 'created', 'created': '2023-02-15T17:17:17Z'}"
            " ejr_job_info={'job_id': 'j-123', 'status': 'c0rRupt', 'created': '2023-02-15T17:17:17Z'}"
        ]

    def test_set_status(self, double_jr, zk_client, memory_jr, time_machine):
        with double_jr:
            double_jr.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            time_machine.move_to("2023-02-15T18:18:18Z")
            double_jr.set_status(job_id="j-123", user_id="john", status=JOB_STATUS.RUNNING)

        expected = DictSubSet(
            {
                "job_id": "j-123",
                "created": "2023-02-15T17:17:17Z",
                "status": "running",
                "updated": "2023-02-15T18:18:18Z",
            }
        )
        assert zk_client.get_json_decoded("/openeo.test/jobs/ongoing/john/j-123") == expected
        assert memory_jr.db["j-123"] == expected

    def test_set_status_no_zk(self, double_jr_no_zk, zk_client, memory_jr, time_machine):
        with double_jr_no_zk:
            double_jr_no_zk.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            time_machine.move_to("2023-02-15T18:18:18Z")
            double_jr_no_zk.set_status(job_id="j-123", user_id="john", status=JOB_STATUS.RUNNING)

        expected = DictSubSet(
            {
                "job_id": "j-123",
                "created": "2023-02-15T17:17:17Z",
                "status": "running",
                "updated": "2023-02-15T18:18:18Z",
            }
        )
        assert zk_client.dump() == {"/": b""}
        assert memory_jr.db["j-123"] == expected

    def test_get_user_jobs(self, double_jr, caplog):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )
            double_jr.create_job(
                job_id="j-456", user_id="john", process=self.DUMMY_PROCESS
            )
            jobs = double_jr.get_user_jobs(user_id="john")
            alice_jobs = double_jr.get_user_jobs(user_id="alice")

        assert alice_jobs == []
        assert len(jobs) == 2
        assert jobs[0].id == "j-123"
        assert jobs[1].id == "j-456"
        assert caplog.messages == []

    def test_get_user_jobs_mismatch(self, double_jr, memory_jr, caplog):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )
            double_jr.create_job(
                job_id="j-456", user_id="john", process=self.DUMMY_PROCESS
            )
            del memory_jr.db["j-456"]
            jobs = double_jr.get_user_jobs(user_id="john")

        assert len(jobs) == 2
        assert jobs[0].id == "j-123"
        assert jobs[1].id == "j-456"

        assert caplog.messages == [
            "DoubleJobRegistry.get_user_jobs(user_id='john') mismatch Zk:len(zk_job_ids)=2 EJR:len(ejr_job_ids)=1"
        ]
