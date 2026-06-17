import datetime
import logging
import urllib.parse
from unittest import mock

import dirty_equals
import pytest
from openeo.rest.auth.testing import OidcMock
from openeo_driver.backend import BatchJobMetadata, JobListing
from openeo_driver.errors import JobNotFoundException
from openeo_driver.jobregistry import JOB_STATUS, ElasticJobRegistry
from openeo_driver.testing import DictSubSet
from openeo_driver.util.auth import ClientCredentials

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.job_registry import (
    DoubleJobRegistry,
    InMemoryJobRegistry,
    get_deletable_dependency_sources,
)
from openeogeotrellis.testing import gps_config_overrides


@pytest.fixture(scope="module", autouse=True)
def _prime_get_backend_config():
    """Prime get_backend_config so that logging emitted from that doesn't ruin caplog usage"""
    get_backend_config()


class TestGetDeletableDependencySources:
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

    def _build_url(self, params: dict):
        return "https://oeo.test/jobs?" + urllib.parse.urlencode(query=params)

    def test_list_user_jobs_paginated(self):
        jr = InMemoryJobRegistry()
        for j in range(1, 8):
            jr.create_job(job_id=f"j-{j}", user_id="alice", process={"foo": "bar"})

        # First page
        listing = jr.list_user_jobs(user_id="alice", limit=3)
        assert isinstance(listing, JobListing)
        assert listing.to_response_dict(build_url=self._build_url) == {
            "jobs": [
                dirty_equals.IsPartialDict(id="j-1"),
                dirty_equals.IsPartialDict(id="j-2"),
                dirty_equals.IsPartialDict(id="j-3"),
            ],
            "links": [{"href": "https://oeo.test/jobs?limit=3&page=1", "rel": "next"}],
        }

        # Second page
        listing = jr.list_user_jobs(user_id="alice", limit=3, request_parameters={"limit": 3, "page": 1})
        assert isinstance(listing, JobListing)
        assert listing.to_response_dict(build_url=self._build_url) == {
            "jobs": [
                dirty_equals.IsPartialDict(id="j-4"),
                dirty_equals.IsPartialDict(id="j-5"),
                dirty_equals.IsPartialDict(id="j-6"),
            ],
            "links": [{"href": "https://oeo.test/jobs?limit=3&page=2", "rel": "next"}],
        }

        # Third page
        listing = jr.list_user_jobs(user_id="alice", limit=3, request_parameters={"limit": 3, "page": 2})
        assert isinstance(listing, JobListing)
        assert listing.to_response_dict(build_url=self._build_url) == {
            "jobs": [
                dirty_equals.IsPartialDict(id="j-7"),
            ],
            "links": [],
        }


class TestDoubleJobRegistry:
    DUMMY_PROCESS = {
        "description": "dummy",
        "process_graph": {
            "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
        },
    }

    @pytest.fixture(autouse=True)
    def _default_time(self, time_machine):
        time_machine.move_to("2023-02-15T17:17:17Z")

    @pytest.fixture
    def memory_jr(self) -> InMemoryJobRegistry:
        return InMemoryJobRegistry()

    @pytest.fixture
    def double_jr(self, memory_jr) -> DoubleJobRegistry:
        return DoubleJobRegistry(
            elastic_job_registry=memory_jr,
        )

    def test_repr(self, double_jr):
        assert repr(double_jr) == "<DoubleJobRegistry InMemoryJobRegistry>"

    def test_context_repr(self, double_jr, caplog):
        caplog.set_level(logging.DEBUG)
        with double_jr:
            pass
        assert "Context enter <DoubleJobRegistry InMemoryJobRegistry>" in caplog.text

    def test_create_job(self, double_jr, memory_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )

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
            "job_options": {"prio": "low"},
            "process": self.DUMMY_PROCESS,
            "created": "2023-02-15T17:17:17Z",
            "status": "created",
            "updated": "2023-02-15T17:17:17Z",
            "api_version": None,
            "application_id": None,
            "title": "John's job",
            "description": None,
            "parent_id": None,
        }
        assert job_metadata == BatchJobMetadata(
            id="j-123",
            status="created",
            created=datetime.datetime(2023, 2, 15, 17, 17, 17),
            process=self.DUMMY_PROCESS,
            job_options={"prio": "low"},
            title="John's job",
            description=None,
            updated=datetime.datetime(2023, 2, 15, 17, 17, 17),
            started=None,
            finished=None,
        )

    def test_get_job_not_found(self, double_jr, caplog):
        with double_jr:
            with pytest.raises(JobNotFoundException):
                _ = double_jr.get_job("j-nope", user_id="john")
            with pytest.raises(JobNotFoundException):
                _ = double_jr.get_job_metadata("j-nope", user_id="john")

    def test_get_their_job(self, double_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
                job_options={"prio": "low"},
                title="John's job",
            )
            double_jr.get_job("j-123", user_id="john")
            double_jr.get_job_metadata("j-123", user_id="john")
            with pytest.raises(JobNotFoundException):
                double_jr.get_job("j-123", user_id="paul")
            with pytest.raises(JobNotFoundException):
                double_jr.get_job_metadata("j-123", user_id="paul")

    def test_set_status(self, double_jr, memory_jr, time_machine):
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
        assert memory_jr.db["j-123"] == expected

    def test_delete_job(self, double_jr, caplog, memory_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
                job_options={"prio": "low"},
                title="John's job",
            )

            assert "j-123" in memory_jr.db

            double_jr.delete_job(job_id="j-123", user_id="john")

            assert "j-123" not in memory_jr.db

    def test_delete_job_wrong_user(self, double_jr, memory_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123",
                user_id="john",
                process=self.DUMMY_PROCESS,
            )
            assert "j-123" in memory_jr.db

            with pytest.raises(JobNotFoundException):
                double_jr.delete_job(job_id="j-123", user_id="paul")
            assert "j-123" in memory_jr.db

            double_jr.delete_job(job_id="j-123", user_id="john")
            assert "j-123" not in memory_jr.db

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

    def _build_url(self, params: dict):
        return "https://oeo.test/jobs?" + urllib.parse.urlencode(query=params)

    def test_get_user_jobs_paginated(self, memory_jr, caplog):
        double_jr = DoubleJobRegistry(
            elastic_job_registry=memory_jr,
        )

        with double_jr:
            for j in range(1, 6):
                double_jr.create_job(job_id=f"j-{j}", user_id="alice", process=self.DUMMY_PROCESS)

            # First page
            listing = double_jr.get_user_jobs(user_id="alice", limit=3)
            assert isinstance(listing, JobListing)
            assert listing.to_response_dict(build_url=self._build_url) == {
                "jobs": [
                    dirty_equals.IsPartialDict(id="j-1"),
                    dirty_equals.IsPartialDict(id="j-2"),
                    dirty_equals.IsPartialDict(id="j-3"),
                ],
                "links": [{"href": "https://oeo.test/jobs?limit=3&page=1", "rel": "next"}],
            }

            # Second page
            listing = double_jr.get_user_jobs(user_id="alice", limit=3, request_parameters={"limit": 3, "page": 1})
            assert isinstance(listing, JobListing)
            assert listing.to_response_dict(build_url=self._build_url) == {
                "jobs": [
                    dirty_equals.IsPartialDict(id="j-4"),
                    dirty_equals.IsPartialDict(id="j-5"),
                ],
                "links": [],
            }

    def test_set_results_metadata(self, double_jr, memory_jr, time_machine):
        with double_jr:
            double_jr.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            double_jr.set_results_metadata(job_id="j-123", user_id="john", costs=1.23,
                                           usage={"cpu": {"unit": "cpu-seconds", "value": 32}},
                                           results_metadata={"epsg": 4326})

        assert memory_jr.db["j-123"] == DictSubSet(
            {
                "costs": 1.23,
                "usage": {"cpu": {"unit": "cpu-seconds", "value": 32}},
                "results_metadata": {
                    "epsg": 4326
                },
            }
        )

    def test_get_results_metadata(self, double_jr, memory_jr):
        with double_jr:
            double_jr.create_job(job_id="j-123", user_id="john", process=self.DUMMY_PROCESS)
            double_jr.set_results_metadata(job_id="j-123", user_id="john", costs=1.23,
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

        with double_jr:
            job_metadata = double_jr.get_job_metadata(job_id="j-123", user_id="john")

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

    def test_list_active_jobs(self, double_jr, memory_jr):
        with double_jr:
            double_jr.create_job(
                job_id="j-123", user_id="john", process=self.DUMMY_PROCESS
            )
            double_jr.set_application_id(job_id="j-123", user_id="john", application_id="a-123")
            double_jr.set_status(job_id="j-123", user_id="john", status=JOB_STATUS.FINISHED)

            double_jr.create_job(
                job_id="j-456", user_id="alice", process=self.DUMMY_PROCESS
            )
            double_jr.set_application_id(job_id="j-456", user_id="alice", application_id="a-456")
            double_jr.set_status(job_id="j-456", user_id="alice", status=JOB_STATUS.QUEUED)

            double_jr.create_job(
                job_id="j-789", user_id="john", process=self.DUMMY_PROCESS
            )

            active_jobs = list(double_jr.list_active_jobs(require_application_id=True))

        active_job_ids = set(job["job_id"] for job in active_jobs)
        assert active_job_ids == {"j-456"}


class TestDoubleJobRegistryWithEjr:
    EJR_API_URL = "https://ejr.test"
    OIDC_ISSUER = "https://oidc.test"
    OIDC_CLIENT_ID = "the_client_id"
    OIDC_CLIENT_SECRET = "the_client_secret"

    @pytest.fixture
    def oidc_mock(self, requests_mock) -> OidcMock:
        oidc_mock = OidcMock(
            requests_mock=requests_mock,
            oidc_issuer=self.OIDC_ISSUER,
            expected_grant_type="client_credentials",
            expected_client_id=self.OIDC_CLIENT_ID,
            expected_fields={"client_secret": self.OIDC_CLIENT_SECRET, "scope": "openid"},
        )
        return oidc_mock

    @pytest.fixture
    def ejr(self, oidc_mock) -> ElasticJobRegistry:
        """ElasticJobRegistry set up with authentication"""
        ejr = ElasticJobRegistry(api_url=self.EJR_API_URL, backend_id="unittests")
        credentials = ClientCredentials(
            oidc_issuer=self.OIDC_ISSUER,
            client_id=self.OIDC_CLIENT_ID,
            client_secret=self.OIDC_CLIENT_SECRET,
        )
        ejr.setup_auth_oidc_client_credentials(credentials)
        return ejr

    @pytest.fixture
    def double_jr(self, ejr) -> DoubleJobRegistry:
        return DoubleJobRegistry(
            elastic_job_registry=ejr,
        )

    def _build_url(self, params: dict):
        return "https://oeo.test/jobs?" + urllib.parse.urlencode(query=params)

    def test_get_user_jobs_legacy(self, double_jr, requests_mock):
        def post_jobs_search(request, context):
            """Handler of `POST /jobs/search"""
            return [
                {"job_id": "j-123", "status": "created"},
                {"job_id": "j-456", "status": "created"},
            ]

        requests_mock.post(f"{self.EJR_API_URL}/jobs/search", json=post_jobs_search)

        jobs = double_jr.get_user_jobs(user_id="john")
        assert len(jobs) == 2
        assert jobs[0].id == "j-123"
        assert jobs[1].id == "j-456"

    @pytest.mark.parametrize(
        ["limit", "request_parameters", "expected_url"],
        [
            (5, None, "https://oeo.test/jobs?limit=5&page=1"),
            (5, {"page": 3}, "https://oeo.test/jobs?limit=5&page=4"),
            (5, {"page": 8}, None),
        ],
    )
    def test_get_user_jobs_paginated(self, double_jr, requests_mock, limit, request_parameters, expected_url):
        def post_jobs_search_paginated(request, context):
            """Handler of `POST /jobs/search"""
            this_page = int(request.qs.get("page", ["0"])[0])
            return {
                "jobs": [
                    {"job_id": "j-123", "status": "created"},
                    {"job_id": "j-456", "status": "created"},
                ],
                "pagination": {"next": {"size": limit, "page": this_page + 1}} if this_page < 8 else {},
            }

        requests_mock.post(f"{self.EJR_API_URL}/jobs/search/paginated", json=post_jobs_search_paginated)

        jobs = double_jr.get_user_jobs(user_id="john", limit=limit, request_parameters=request_parameters)
        assert isinstance(jobs, JobListing)
        assert jobs.to_response_dict(build_url=self._build_url) == {
            "jobs": [
                {"id": "j-123", "status": "created", "progress": 0},
                {"id": "j-456", "status": "created", "progress": 0},
            ],
            "links": [{"rel": "next", "href": expected_url}] if expected_url else [],
        }
