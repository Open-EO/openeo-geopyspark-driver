import logging
import datetime
import pytest
from unittest import mock
import kazoo.exceptions
from kazoo.handlers.threading import KazooTimeoutError

from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import DictSubSet
from openeogeotrellis.job_registry import ZkJobRegistry, InMemoryJobRegistry, DoubleJobRegistry, get_deletable_dependency_sources
from openeogeotrellis.testing import KazooClientMock


class TestZkJobRegistry:
    @pytest.fixture(autouse=True)
    def _default_time(self, time_machine):
        time_machine.move_to("2023-02-15T17:17:17Z")

    @pytest.fixture
    def zk_client(self) -> KazooClientMock:
        return KazooClientMock()

    def test_basic(self, zk_client):
        zjr = ZkJobRegistry(zk_client=zk_client)
        zjr.register(
            job_id="j123", user_id="u456", api_version="1.2.3", specification={"foo": "bar"}
        )

        data = zk_client.get_json_decoded("/openeo.test/jobs/ongoing/u456/j123")
        assert data == DictSubSet(
            user_id="u456", job_id="j123", specification='{"foo": "bar"}', status="created"
        )

    def test_set_status(self, zk_client, time_machine):
        zjr = ZkJobRegistry(zk_client=zk_client)
        zjr.register(
            job_id="j123", user_id="u456", api_version="1.2.3", specification={"foo": "bar"}
        )
        time_machine.move_to("2023-02-15T18:18:18Z")
        zjr.set_status(job_id="j123", user_id="u456", status=JOB_STATUS.FINISHED,
                       started="2023-02-15T17:18:17Z", finished="2023-02-15T18:17:18Z")

        expected = DictSubSet(
            {
                "job_id": "j123",
                "created": "2023-02-15T17:17:17Z",
                "status": "finished",
                "updated": "2023-02-15T18:18:18Z",
                "started": "2023-02-15T17:18:17Z",
                "finished": "2023-02-15T18:17:18Z"
            }
        )
        assert zk_client.get_json_decoded("/openeo.test/jobs/done/u456/j123") == expected

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
                        },
                        {
                            "partial_job_results_url": "https://oeo.org/jobs/j-abc123/results"
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
    def test_get_dependency_sources(self, job_info, expected):
        assert get_deletable_dependency_sources(job_info) == expected

    @pytest.mark.parametrize(
        ["root_path", "path"],
        [
            ("/oeo.test/jobs", "/oeo.test/jobs/ongoing/u456/j123"),
            ("/oeo/test/jobs/", "/oeo/test/jobs/ongoing/u456/j123"),
        ],
    )
    def test_root_path(self, zk_client, root_path, path):
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
            zk_job_registry_factory=None,
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

    def test_get_job_not_found(self, double_jr, caplog):
        with double_jr:
            with pytest.raises(JobNotFoundException):
                _ = double_jr.get_job("j-nope", user_id="john")
            with pytest.raises(JobNotFoundException):
                _ = double_jr.get_job_metadata("j-nope", user_id="john")
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

    @pytest.mark.parametrize(
        ["with_zk", "with_ejr", "expected_process_extra"],
        [
            (True, True, {}),
            (False, True, {"title": "dummy"}),
            (True, False, {}),
        ],
    )
    def test_get_job_consistency(
        self, double_jr, caplog, zk_client, memory_jr, with_zk, with_ejr, expected_process_extra
    ):
        """Consistent user job info (dict) and metadata (BatchJobMetadata) when ZK or EJR is broken?"""
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
                job_options={"prio": "low"},
                title="John's job",
            )

        other_double_jr = DoubleJobRegistry(
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)) if with_zk else None,
            elastic_job_registry=memory_jr if with_ejr else None,
        )
        with other_double_jr:
            job = other_double_jr.get_job("j-123", user_id="john")
            job_metadata = other_double_jr.get_job_metadata("j-123", user_id="john")
        expected_job = {
            "job_id": "j-123",
            "user_id": "john",
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": "John's job",
            "description": None,
        }
        if with_zk:
            expected_job[
                "specification"
            ] = '{"process_graph": {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}}, "job_options": {"prio": "low"}}'
        elif with_ejr:
            expected_job["process"] = self.DUMMY_PROCESS
            expected_job["job_options"] = {"prio": "low"}
            expected_job["parent_id"] = None
        assert job == expected_job
        assert job_metadata == BatchJobMetadata(
            id="j-123",
            status="created",
            created=datetime.datetime(2023, 2, 15, 17, 17, 17),
            process=dict(
                process_graph={"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}},
                **expected_process_extra,
            ),
            job_options={"prio": "low"},
            title="John's job",
            description=None,
            updated=datetime.datetime(2023, 2, 15, 17, 17, 17),
            started=None,
            finished=None,
        )

        assert caplog.messages == []

    def test_get_job_deleted_from_zk(self, double_jr, caplog, zk_client, memory_jr):
        """
        Make sure to fall back on EJR if no data found in ZK
        https://github.com/Open-EO/openeo-geopyspark-driver/issues/523
        """
        with double_jr:
            double_jr.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            # Wipe Zookeeper db
            zk_client.delete("/")

            job = double_jr.get_job("j-123", user_id="john")
            job_metadata = double_jr.get_job_metadata("j-123", user_id="john")

        expected_job = {
            "job_id": "j-123",
            "user_id": "john",
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": "John's job",
            "description": None,
        }
        assert job == DictSubSet(
            {"job_id": "j-123", "user_id": "john", "created": "2023-02-15T17:17:17Z", "status": "created"}
        )
        assert job_metadata == BatchJobMetadata(
            id="j-123",
            status="created",
            created=datetime.datetime(2023, 2, 15, 17, 17, 17),
            process=dict(
                process_graph={"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}},
                title="dummy",
            ),
            job_options=None,
            title=None,
            description=None,
            updated=datetime.datetime(2023, 2, 15, 17, 17, 17),
            started=None,
            finished=None,
        )

        assert caplog.messages == []

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

    def test_delete_job(self, double_jr, caplog, zk_client, memory_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
                job_options={"prio": "low"},
                title="John's job",
            )

            assert zk_client.get_json_decoded("/openeo.test/jobs/ongoing/john/j-123") == DictSubSet(job_id="j-123")
            assert "j-123" in memory_jr.db

            double_jr.delete_job(job_id="j-123", user_id="john")

            with pytest.raises(kazoo.exceptions.NoNodeError):
                zk_client.get("/openeo.test/jobs/ongoing/john/j-123")
            assert "j-123" not in memory_jr.db

        assert caplog.messages == []

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
        assert caplog.messages == [
            "DoubleJobRegistry.get_user_jobs(user_id='john') zk_jobs=2 ejr_jobs=2",
            "DoubleJobRegistry.get_user_jobs(user_id='alice') zk_jobs=[] ejr_jobs=[]",
        ]

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

        assert caplog.messages == ["DoubleJobRegistry.get_user_jobs(user_id='john') zk_jobs=2 ejr_jobs=1"]

    def test_get_user_jobs_no_zk(self, double_jr_no_zk, caplog):
        with double_jr_no_zk:
            double_jr_no_zk.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            double_jr_no_zk.create_job(job_id="j-456", user_id="john", process=self.DUMMY_PROCESS)
            jobs = double_jr_no_zk.get_user_jobs(user_id="john")
            alice_jobs = double_jr_no_zk.get_user_jobs(user_id="alice")

        assert alice_jobs == []
        assert len(jobs) == 2
        assert jobs[0].id == "j-123"
        assert jobs[1].id == "j-456"
        assert caplog.messages == [
            "DoubleJobRegistry.get_user_jobs(user_id='john') zk_jobs=None ejr_jobs=2",
            "DoubleJobRegistry.get_user_jobs(user_id='alice') zk_jobs=None ejr_jobs=[]",
        ]

    @pytest.mark.parametrize(
        ["with_zk", "with_ejr", "expected_process_extra", "expected_log"],
        [
            (True, True, {}, "zk_jobs=1 ejr_jobs=1"),
            (False, True, {"title": "dummy"}, "zk_jobs=None ejr_jobs=1"),
            (True, False, {}, "zk_jobs=1 ejr_jobs=None"),
        ],
    )
    def test_get_user_jobs_consistency(
        self, double_jr, zk_client, memory_jr, caplog, with_zk, with_ejr, expected_process_extra, expected_log
    ):
        """Consistent user job listings (using BatchJobMetadata) when ZK or EJR is broken?"""
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
                job_options={"prio": "low"},
                title="John's job",
            )

        other_double_jr = DoubleJobRegistry(
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)) if with_zk else None,
            elastic_job_registry=memory_jr if with_ejr else None,
        )
        with other_double_jr:
            jobs = other_double_jr.get_user_jobs(user_id="john")

        assert jobs == [
            BatchJobMetadata(
                id="j-123",
                status="created",
                created=datetime.datetime(2023, 2, 15, 17, 17, 17),
                updated=datetime.datetime(2023, 2, 15, 17, 17, 17),
                process=dict(
                    process_graph={"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}},
                    **expected_process_extra,
                ),
                job_options={"prio": "low"},
                title="John's job",
            )
        ]
        assert caplog.messages == [f"DoubleJobRegistry.get_user_jobs(user_id='john') {expected_log}"]

    def test_get_user_jobs_zk_timeout(self, zk_client, double_jr, caplog):
        with double_jr:
            double_jr.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)

        with mock.patch.object(zk_client, "start", side_effect=KazooTimeoutError("Connection time-out")), double_jr:
            jobs = double_jr.get_user_jobs(user_id="john")

        assert len(jobs) == 1
        assert jobs[0].id == "j-123"
        assert caplog.messages == [
            "Failed to enter ZkJobRegistry: KazooTimeoutError('Connection time-out')",
            "DoubleJobRegistry.get_user_jobs(user_id='john') zk_jobs=None ejr_jobs=1",
        ]

    def test_set_results_metadata_no_zk(self, double_jr_no_zk, zk_client, memory_jr, time_machine):
        with double_jr_no_zk:
            double_jr_no_zk.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            double_jr_no_zk.set_results_metadata(job_id="j-123", user_id="john", costs=1.23,
                                                 usage={"cpu": {"unit": "cpu-seconds", "value": 32}},
                                                 results_metadata={"epsg": 4326})

        expected = DictSubSet(
            {
                "costs": 1.23,
                "usage": {"cpu": {"unit": "cpu-seconds", "value": 32}},
                "results_metadata": {
                    "epsg": 4326
                },
            }
        )
        assert zk_client.dump() == {"/": b""}
        assert memory_jr.db["j-123"] == expected

    def test_get_results_metadata_no_zk(self, double_jr_no_zk):
        with double_jr_no_zk:
            double_jr_no_zk.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            double_jr_no_zk.set_results_metadata(job_id="j-123", user_id="john", costs=1.23,
                                                 usage={"cpu": {"unit": "cpu-seconds", "value": 32}},
                                                 results_metadata={
                                                     "start_datetime": "2023-09-24T00:00:00Z",
                                                     "end_datetime": "2023-09-29T00:00:00Z",
                                                     "geometry": {"type": "Polygon", "coordinates": [[[7, 51.3], [7, 51.75], [7.6, 51.75], [7.6, 51.3], [7, 51.3]]]},
                                                     "bbox": [4.0, 50.0, 5.0, 51.0],
                                                     "epsg": 32631,
                                                     "instruments": [],
                                                     "links": [
                                                         {
                                                             "href": "openEO_2023-09-27Z.tif",
                                                             "rel": "derived_from",
                                                             "title": "Derived from openEO_2023-09-27Z.tif",
                                                             "type": "application/json"
                                                         }
                                                     ],
                                                     "proj:bbox": [634111.429, 5654675.526, 634527.619, 5654913.158],
                                                     "proj:shape": [23, 43]
                                                 })
            job_metadata = double_jr_no_zk.get_job_metadata(job_id="j-123", user_id="john")

        assert job_metadata.costs == 1.23
        assert job_metadata.usage == {"cpu": {"unit": "cpu-seconds", "value": 32}}
        assert job_metadata.start_datetime == datetime.datetime(2023, 9, 24, 0, 0, 0)
        assert job_metadata.end_datetime == datetime.datetime(2023, 9, 29, 0, 0, 0)
        assert job_metadata.geometry == {"type": "Polygon",
                                         "coordinates": [[[7, 51.3], [7, 51.75], [7.6, 51.75], [7.6, 51.3], [7, 51.3]]]}
        assert job_metadata.bbox == [4.0, 50.0, 5.0, 51.0]
        assert job_metadata.epsg == 32631
        assert job_metadata.instruments == []
        assert job_metadata.links == [{
            "href": "openEO_2023-09-27Z.tif",
            "rel": "derived_from",
            "title": "Derived from openEO_2023-09-27Z.tif",
            "type": "application/json"
        }]
        assert job_metadata.proj_bbox == [634111.429, 5654675.526, 634527.619, 5654913.158]
        assert job_metadata.proj_shape == [23, 43]

    @pytest.mark.parametrize(
        ["with_zk", "with_ejr"],
        [
            (True, True),
            (False, True),
            (True, False),
        ],
    )
    def test_get_running_jobs(self, double_jr, zk_client, memory_jr, with_zk, with_ejr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )
            double_jr.create_job(
                job_id="j-456", user_id="alice", process=self.DUMMY_PROCESS
            )
            double_jr.create_job(
                job_id="j-789", user_id="john", process=self.DUMMY_PROCESS
            )
            double_jr.set_status(job_id="j-123", user_id="john", status=JOB_STATUS.FINISHED)

        other_double_jr = DoubleJobRegistry(
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)) if with_zk else None,
            elastic_job_registry=memory_jr if with_ejr else None,
        )

        with other_double_jr:
            running_jobs = list(other_double_jr.get_running_jobs())

        running_job_ids = set(job["job_id"] for job in running_jobs)
        assert running_job_ids == {"j-456", "j-789"}
