import contextlib
import datetime
import json
import os
import re
import subprocess
from unittest import mock

import openeo_driver.testing
import pytest
from openeo_driver.testing import TEST_USER_AUTH_HEADER, TEST_USER, TIFF_DUMMY_DATA
from openeo_driver.views import app

import openeogeotrellis.job_registry
from openeogeotrellis.backend import GpsBatchJobs, JOB_METADATA_FILENAME
from openeogeotrellis.testing import KazooClientMock
from .data import TEST_DATA_ROOT


@pytest.fixture
def client():
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeo.net'
    return app.test_client()


class ApiTester(openeo_driver.testing.ApiTester):
    data_root = TEST_DATA_ROOT


@pytest.fixture
def api(api_version, client) -> ApiTester:
    return ApiTester(api_version=api_version, client=client)


@pytest.fixture
def api100(client) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=client)


def test_file_formats(api100):
    formats = api100.get('/file_formats').assert_status_code(200).json
    assert "GeoJSON" in formats["input"]
    assert "GTiff" in formats["output"]
    assert "CovJSON" in formats["output"]
    assert "NetCDF" in formats["output"]


def test_health(api):
    resp = api.get('/health').assert_status_code(200)
    assert resp.json == {"health": 'Health check: 14'}


class TestCollections:

    def test_all_collections(self, api):
        collections = api.get('/collections').assert_status_code(200).json["collections"]
        assert len(collections) > 2
        for collections in collections:
            assert re.match(r'^[A-Za-z0-9_\-\.~\/]+$', collections['id'])
            assert 'stac_version' in collections
            assert 'description' in collections
            assert 'license' in collections
            assert 'extent' in collections
            assert 'links' in collections

    def test_collections_s2_radiometry(self, api):
        resp = api.get('/collections/CGS_SENTINEL2_RADIOMETRY_V102_001').assert_status_code(200).json
        assert resp['id'] == "CGS_SENTINEL2_RADIOMETRY_V102_001"
        assert "Sentinel 2" in resp['description']
        eo_bands = [
            {"name": "2", "common_name": "blue", "center_wavelength": 0.4966},
            {"name": "3", "common_name": "green", "center_wavelength": 0.560},
            {"name": "4", "common_name": "red", "center_wavelength": 0.6645},
            {"name": "8", "common_name": "nir", "center_wavelength": 0.8351}
        ]
        if api.api_version_compare.at_least("1.0.0"):
            assert resp['stac_version'] == "0.9.0"
            assert resp['extent'] == {
                "spatial": {"bbox": [[-180, -56, 180, 83]]},
                "temporal": {"interval": [["2015-07-06T00:00:00Z", None]]},
            }
            assert resp['cube:dimensions'] == {
                'x': {'type': 'spatial', 'axis': 'x', 'extent': [-180, 180]},
                'y': {'type': 'spatial', 'axis': 'y', 'extent': [-56, 83]},
                't': {'type': 'temporal', 'extent': ["2015-07-06T00:00:00Z", None]},
                'bands': {'type': 'bands', 'values': ['2', '3', '4', '8']}
            }
            for f in eo_bands[0].keys():
                assert [b[f] for b in resp['summaries']['eo:bands']] == [b[f] for b in eo_bands]
        else:
            assert resp['stac_version'] == "0.6.2"
            assert resp['extent'] == {
                "spatial": [-180, -56, 180, 83],
                "temporal": ["2015-07-06T00:00:00Z", None]
            }
            assert resp["properties"]['cube:dimensions'] == {
                'x': {'type': 'spatial', 'axis': 'x'},
                'y': {'type': 'spatial', 'axis': 'y'},
                't': {'type': 'temporal'},
                'bands': {'type': 'bands', 'values': ['2', '3', '4', '8']}
            }

            for f in eo_bands[0].keys():
                assert [b[f] for b in resp['properties']["eo:bands"]] == [b[f] for b in eo_bands]


class TestBatchJobs:
    DUMMY_PROCESS_GRAPH = {
        "foo": {
            "process_id": "foo",
            "arguments": {}
        }
    }

    @staticmethod
    @contextlib.contextmanager
    def _mock_kazoo_client():
        zk_client = KazooClientMock()
        with mock.patch.object(openeogeotrellis.job_registry, 'KazooClient', return_value=zk_client):
            yield zk_client

    @staticmethod
    @contextlib.contextmanager
    def _mock_utcnow():
        with mock.patch('openeogeotrellis.job_registry.datetime', new=mock.Mock(wraps=datetime.datetime)) as dt:
            dt.utcnow.return_value = datetime.datetime(2020, 4, 20, 16, 4, 3)
            yield dt.utcnow

    def test_get_user_jobs_no_auth(self, api):
        api.get('/jobs').assert_status_code(401).assert_error_code("AuthenticationRequired")

    def test_get_user_jobs_empty(self, api):
        with self._mock_kazoo_client() as zk:
            result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert result == {"jobs": [], "links": []}

    def test_create_job(self, api):
        with self._mock_kazoo_client() as zk, self._mock_utcnow() as un:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy", description="Dummy job!")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            raw, _ = zk.get('/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id))
            meta_data = json.loads(raw.decode())
            assert meta_data["job_id"] == job_id
            assert meta_data["user_id"] == TEST_USER
            assert meta_data["status"] == "created"
            assert meta_data["api_version"] == api.api_version
            assert meta_data["application_id"] == None
            assert meta_data["created"] == "2020-04-20T16:04:03Z"
            assert meta_data["title"] == "Dummy"
            assert meta_data["description"] == "Dummy job!"

    def test_create_and_get(self, api):
        with self._mock_kazoo_client() as zk, self._mock_utcnow() as un:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

        if api.api_version_compare.at_least("1.0.0"):
            expected = {
                "id": job_id,
                "process": {"process_graph": self.DUMMY_PROCESS_GRAPH},
                "status": "created",
                "created": "2020-04-20T16:04:03Z",
                "title": "Dummy",
            }
        else:
            expected = {
                "id": job_id,
                "process_graph": self.DUMMY_PROCESS_GRAPH,
                "status": "submitted",
                "submitted": "2020-04-20T16:04:03Z",
                "title": "Dummy",
            }
        assert res == expected

    def test_get_legacy_zk_data(self, api):
        with self._mock_kazoo_client() as zk:
            job_id = 'ad597b92-e6f3-4241-88ce-d31739a740ff'
            raw = {
                'api_version': '0.4.0',
                'application_id': None,
                'job_id': job_id,
                'specification': '{"process_graph": {"foo": {"process_id": "foo", "arguments": {}}},'
                                 '"title": null, "description": null, "plan": null, "budget": null}',
                'status': 'submitted',
                'user_id': TEST_USER
            }
            zk.create(
                path='/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id),
                value=json.dumps(raw).encode(),
                makepath=True
            )

            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

        if api.api_version_compare.at_least("1.0.0"):
            expected = {
                "id": job_id,
                "process": {"process_graph": self.DUMMY_PROCESS_GRAPH,
                            "title": None, "description": None, "plan": None, "budget": None},
                "status": "created",
            }
        else:
            expected = {
                "id": job_id,
                "process_graph": self.DUMMY_PROCESS_GRAPH,
                "status": "submitted",
            }
        assert res == expected

    def test_create_and_get_user_jobs(self, api):
        with self._mock_kazoo_client() as zk, self._mock_utcnow() as un:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            created = "created" if api.api_version_compare.at_least("1.0.0") else "submitted"
            assert result == {
                "jobs": [
                    {"id": job_id, "status": created, created: "2020-04-20T16:04:03Z", "title": "Dummy"},
                ],
                "links": []
            }

    def test_create_and_start_and_download(self, api, tmp_path):
        with self._mock_kazoo_client() as zk, \
                self._mock_utcnow() as un:
            GpsBatchJobs._OUTPUT_ROOT_DIR = tmp_path

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                os.mkdir(tmp_path / job_id)
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    '/jobs/{j}/results'.format(j=job_id), json={}, headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()
                batch_job_args = run.call_args[0][0]

            # Check batch in/out files
            job_dir = (tmp_path / job_id)
            job_output = (job_dir / "out")
            job_log = (job_dir / "log")
            job_metadata = (job_dir / JOB_METADATA_FILENAME)
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_log.name
            assert batch_job_args[6] == job_metadata.name
            assert batch_job_args[9] == TEST_USER
            assert batch_job_args[10] == api.api_version
            assert batch_job_args[11:] == ['12G', '2G', '2G', '5', '2', '2G', 'default', 'false', 'no_dependencies']
            

            # Check metadata in zookeeper
            raw, _ = zk.get('/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id))
            meta_data = json.loads(raw.decode())
            assert meta_data["job_id"] == job_id
            assert meta_data["user_id"] == TEST_USER
            assert meta_data["status"] == "queued"
            assert meta_data["api_version"] == api.api_version
            assert json.loads(meta_data["specification"]) == (
                data['process'] if api.api_version_compare.at_least("1.0.0")
                else {"process_graph": data["process_graph"]})
            assert meta_data["application_id"] == 'application_1587387643572_0842'
            assert meta_data["created"] == "2020-04-20T16:04:03Z"
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "queued"

            # Get logs
            res = api.get(
                '/jobs/{j}/logs'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            assert res["logs"] == []

            # Fake update from job tracker
            with openeogeotrellis.job_registry.JobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status="running")
            raw, _ = zk.get('/openeo/jobs/ongoing/{u}/{j}'.format(u=TEST_USER, j=job_id))
            meta_data = json.loads(raw.decode())
            assert meta_data["status"] == "running"
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "running"

            # Try to download results too early
            res = api.get('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER)
            res.assert_error(status_code=400, error_code='JobNotFinished')

            # Set up fake output and finish
            with job_output.open('wb') as f:
                f.write(TIFF_DUMMY_DATA)
            with job_log.open('w') as f:
                f.write("[INFO] Hello world")
            with openeogeotrellis.job_registry.JobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status="finished")
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "finished"

            # Download
            res = api.get(
                '/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            if api.api_version_compare.at_least("1.0.0"):
                download_url = res["assets"]["out"]["href"]
            else:
                download_url = res["links"][0]["href"]
            res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
            assert res.status_code == 200
            assert res.data == TIFF_DUMMY_DATA

            # Get logs
            res = api.get(
                '/jobs/{j}/logs'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            assert res["logs"] == [{"id": "0", "level": "error", "message": "[INFO] Hello world"}]

    def test_create_and_start_job_options(self, api, tmp_path):
        with self._mock_kazoo_client() as zk, \
                self._mock_utcnow() as un:
            GpsBatchJobs._OUTPUT_ROOT_DIR = tmp_path

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            data["job_options"] = {"driver-memory": "3g", "executor-memory": "11g","executor-cores":"4","queue":"somequeue","driver-memoryOverhead":"10000G"}
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    '/jobs/{j}/results'.format(j=job_id), json={}, headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()
                batch_job_args = run.call_args[0][0]

            # Check batch in/out files
            job_dir = (tmp_path / job_id)
            job_output = (job_dir / "out")
            job_log = (job_dir / "log")
            job_metadata = (job_dir / JOB_METADATA_FILENAME)
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_log.name
            assert batch_job_args[6] == job_metadata.name
            assert batch_job_args[9] == TEST_USER
            assert batch_job_args[10] == api.api_version
            assert batch_job_args[11:] == ['3g', '11g', '2G', '5', '4', '10000G', 'somequeue', 'false',
                                           'no_dependencies']

    def test_cancel_job(self, api, tmp_path):
        with self._mock_kazoo_client() as zk:
            GpsBatchJobs._OUTPUT_ROOT_DIR = tmp_path

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    '/jobs/{j}/results'.format(j=job_id), json={},
                    headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()

            # Fake running
            with openeogeotrellis.job_registry.JobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status="running")
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "running"

            # Cancel
            with mock.patch('subprocess.run') as run:
                res = api.delete('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER)
                res.assert_status_code(204)
                run.assert_called_once()
                command = run.call_args[0][0]
                assert command == ["yarn", "application", "-kill", 'application_1587387643572_0842']
