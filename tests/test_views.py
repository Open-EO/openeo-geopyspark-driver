from moto import mock_s3
import boto3

import contextlib
import datetime
import json
import os
import re
import pathlib
import subprocess
from unittest import mock
import pytest

import openeogeotrellis.job_registry
from openeo_driver.jobregistry import JOB_STATUS
from openeogeotrellis.job_registry import ZkJobRegistry
from openeo.util import deep_get
from openeo_driver.testing import TEST_USER_AUTH_HEADER, TEST_USER, TIFF_DUMMY_DATA
from openeogeotrellis.backend import GpsBatchJobs, JOB_METADATA_FILENAME
from openeogeotrellis.testing import KazooClientMock
from openeogeotrellis.utils import to_s3_url


def test_file_formats(api100):
    formats = api100.get('/file_formats').assert_status_code(200).json
    assert "GeoJSON" in formats["input"]
    assert "GTiff" in formats["output"]
    assert "CovJSON" in formats["output"]
    assert "netCDF" in formats["output"]
    assert "description" in deep_get(formats, "output", "PNG", "parameters", "colormap")


@pytest.mark.parametrize(["path", "expected"], [
    ("/health", {"mode": "spark", "status": "OK", "count": 14}),
    ("/health?mode=spark", {"mode": "spark", "status": "OK", "count": 14}),
    ("/health?mode=jvm", {"mode": "jvm", "status": "OK", "pi": "3.141592653589793"}),
    ("/health?mode=basic", {"mode": "basic", "status": "OK"}),
])
def test_health_default(api, path, expected):
    resp = api.get(path).assert_status_code(200)
    assert resp.json == expected


class TestCollections:
    _CRS_AUTO_42001 = {'$schema': 'https://proj.org/schemas/v0.2/projjson.schema.json', 'type': 'GeodeticCRS',
                       'name': 'AUTO 42001 (Universal Transverse Mercator)',
                       'datum': {'type': 'GeodeticReferenceFrame', 'name': 'World Geodetic System 1984',
                                 'ellipsoid': {'name': 'WGS 84', 'semi_major_axis': 6378137,
                                               'inverse_flattening': 298.257223563}},
                       'coordinate_system': {'subtype': 'ellipsoidal', 'axis': [
                           {'name': 'Geodetic latitude', 'abbreviation': 'Lat', 'direction': 'north', 'unit': 'degree'},
                           {'name': 'Geodetic longitude', 'abbreviation': 'Lon', 'direction': 'east',
                            'unit': 'degree'}]}, 'area': 'World',
                       'bbox': {'south_latitude': -90, 'west_longitude': -180, 'north_latitude': 90,
                                'east_longitude': 180},
                       'id': {'authority': 'OGC', 'version': '1.3', 'code': 'Auto42001'}}

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
        resp = api.get('/collections/TERRASCOPE_S2_TOC_V2').assert_status_code(200).json
        assert resp['id'] == "TERRASCOPE_S2_TOC_V2"
        assert "Sentinel-2" in resp['description']
        eo_bands =  [
                    {
                      "name": "B01",
                      "aliases": ["TOC-B01_60M"],
                      "common_name": "coastal aerosol",
                      "wavelength_nm": 442.7,
                      "gsd": 60,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B02",
                      "aliases": ["TOC-B02_10M"],
                      "common_name": "blue",
                      "center_wavelength": 0.4966,
                      "wavelength_nm": 496.6,
                      "gsd": 10,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B03",
                      "aliases": ["TOC-B03_10M"],
                      "common_name": "green",
                      "center_wavelength": 0.560,
                      "wavelength_nm": 560,
                      "gsd": 10,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B04",
                      "aliases": ["TOC-B04_10M"],
                      "common_name": "red",
                      "center_wavelength": 0.6645,
                      "wavelength_nm": 664.5,
                      "gsd": 10,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B05",
                      "aliases": ["TOC-B05_20M"],
                      "common_name": "nir",
                      "wavelength_nm": 704.1,
                      "gsd": 20,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B06",
                      "aliases": ["TOC-B06_20M"],
                      "wavelength_nm": 740.5,
                      "gsd": 20,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B07",
                      "aliases": ["TOC-B07_20M"],
                      "wavelength_nm": 782.8,
                      "gsd": 20,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B08",
                      "aliases": ["TOC-B08_10M"],
                      "common_name": "nir",
                      "center_wavelength": 0.8351,
                      "wavelength_nm": 835.1,
                      "gsd": 10,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B8A",
                      "aliases": ["TOC-B8A_20M"],
                      "wavelength_nm": 864.7,
                      "gsd": 20,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B11",
                      "aliases": ["TOC-B11_20M"],
                      "common_name": "swir",
                      "wavelength_nm": 1613.7,
                      "gsd": 20,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "B12",
                      "aliases": ["TOC-B12_20M"],
                      "common_name": "swir",
                      "wavelength_nm": 2202.4,
                      "gsd": 20,
                      "scale": 0.0001,
                      "offset": 0,
                      "type": "int16",
                      "unit": "1"
                    },
                    {
                      "name": "SCL",
                      "aliases": ["SCENECLASSIFICATION_20M"],
                      "gsd": 20
                    },
                    {
                      "name": "relativeAzimuthAngles",
                      "aliases": ["RAA_60M"],
                      "gsd": 60
                    },
                    {
                      "name": "sunZenithAngles",
                      "aliases": ["SZA_60M"],
                      "gsd": 60
                    },
                    {
                      "name": "viewZenithAngles",
                      "aliases": ["VZA_60M", "viewZenithApproximate"],
                      "gsd": 60
                    }
                  ]
        if api.api_version_compare.at_least("1.0.0"):
            assert resp['stac_version'] == "0.9.0"
            assert resp['extent'] == {
                "spatial": {"bbox": [[-180, -56, 180, 83]]},
                "temporal": {"interval": [["2015-07-06T00:00:00Z", None]]},
            }
            assert resp['cube:dimensions'] == {'bands': {'type': 'bands',
                                                       'values': ['B01',
                                                                  'B02',
                                                                  'B03',
                                                                  'B04',
                                                                  'B05',
                                                                  'B06',
                                                                  'B07',
                                                                  'B08',
                                                                  'B8A',
                                                                  'B11',
                                                                  'B12',
                                                                  'SCL',
                                                                  'relativeAzimuthAngles',
                                                                  'sunZenithAngles',
                                                                  'viewZenithAngles']},
                                             't': {'extent': ['2015-07-06T00:00:00Z', None], 'type': 'temporal'},
                                             'x': {'axis': 'x',
                                                   'extent': [-180, 180],
                                                   'reference_system': TestCollections._CRS_AUTO_42001,
                                                   'step': 10,
                                                   'type': 'spatial'},
                                             'y': {'axis': 'y',
                                                   'extent': [-56, 83],
                                                   'reference_system': TestCollections._CRS_AUTO_42001,
                                                   'step': 10,
                                                   'type': 'spatial'}}
            for f in eo_bands[0].keys():
                assert [b[f] for b in resp['summaries']['eo:bands'] if f in b] == [b[f] for b in eo_bands if f in b]
        else:
            assert resp['stac_version'] == "0.6.2"
            assert resp['extent'] == {
                "spatial": [-180, -56, 180, 83],
                "temporal": ["2015-07-06T00:00:00Z", None]
            }
            assert resp["properties"]['cube:dimensions'] == {'bands': {'type': 'bands',
                                                               'values': ['B01',
                                                                          'B02',
                                                                          'B03',
                                                                          'B04',
                                                                          'B05',
                                                                          'B06',
                                                                          'B07',
                                                                          'B08',
                                                                          'B8A',
                                                                          'B11',
                                                                          'B12',
                                                                          'SCL',
                                                                          'relativeAzimuthAngles',
                                                                          'sunZenithAngles',
                                                                          'viewZenithAngles']},
                                                     't': {'type': 'temporal'},
                                                     'x': {'axis': 'x',
                                                           'reference_system': TestCollections._CRS_AUTO_42001,
                                                           'step': 10,
                                                           'type': 'spatial'},
                                                     'y': {'axis': 'y',
                                                           'reference_system': TestCollections._CRS_AUTO_42001,
                                                           'step': 10,
                                                           'type': 'spatial'}}

            for f in eo_bands[0].keys():
                assert [b[f] for b in resp['summaries']['eo:bands'] if f in b] == [b[f] for b in eo_bands if f in b]


TEST_AWS_REGION_NAME = 'eu-central-1'

@pytest.fixture(scope='function')
def aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', TEST_AWS_REGION_NAME)
    monkeypatch.setenv('AWS_REGION', TEST_AWS_REGION_NAME)


@pytest.fixture(scope='function')
def mock_s3_resource(aws_credentials):
    with mock_s3():
        yield boto3.resource("s3", region_name=TEST_AWS_REGION_NAME)


@pytest.fixture(scope='function')
def mock_s3_client(aws_credentials):
    with mock_s3():
        yield boto3.s3_client("s3", region_name=TEST_AWS_REGION_NAME)


@pytest.fixture(scope='function')
def mock_s3_bucket(mock_s3_resource, monkeypatch):
    # TODO: bucket_name: there could be a mismatch with ConfigParams().s3_bucket_name if any ConfigParams instances were created earlier in the test setup.
    bucket_name = "openeo-fake-bucketname"
    monkeypatch.setenv("SWIFT_BUCKET", bucket_name)
    from openeogeotrellis.configparams import ConfigParams
    assert ConfigParams().s3_bucket_name == bucket_name

    bucket = mock_s3_resource.Bucket(bucket_name)
    bucket.create(CreateBucketConfiguration={'LocationConstraint': TEST_AWS_REGION_NAME})
    yield bucket


def create_files_on_s3(s3_bucket, file_names, file_contents):
    for fname, contents in zip(file_names, file_contents):
        s3_bucket.put_object(Key=fname, Body=contents)


class TestBatchJobs:
    DUMMY_PROCESS_GRAPH = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "BIOPAR_FAPAR_V1_GLOBAL"
            },
            "result": True
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
                'specification': '{"process_graph": {"loadcollection1": {"process_id": "load_collection","arguments":'
                                 '{"id": "BIOPAR_FAPAR_V1_GLOBAL"}, "result": true}},'
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
                    {"id": job_id, "title": "Dummy", "status": created, created: "2020-04-20T16:04:03Z"},
                ],
                "links": []
            }

    def test_create_and_start_and_download(self, api, tmp_path, monkeypatch, batch_job_output_root):
        with self._mock_kazoo_client() as zk, \
                self._mock_utcnow() as un, \
                mock.patch.dict("os.environ", {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/custom_processes.py,data/deps/foolib.whl"}):

            openeo_flask_dir = tmp_path / "openeo-flask"
            openeo_flask_dir.mkdir()
            (openeo_flask_dir / "foolib.whl").touch()
            (openeo_flask_dir / "__pyfiles__").mkdir()
            (openeo_flask_dir / "__pyfiles__" / "custom_processes.py").touch()
            monkeypatch.chdir(openeo_flask_dir)

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                os.mkdir(batch_job_output_root / job_id)
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    '/jobs/{j}/results'.format(j=job_id), json={}, headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()
                batch_job_args = run.call_args[0][0]

            # Check batch in/out files
            job_dir = batch_job_output_root / job_id
            job_output = job_dir / "out"
            job_log = job_dir / "log"
            job_metadata = job_dir / JOB_METADATA_FILENAME
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_log.name
            assert batch_job_args[6] == job_metadata.name
            assert batch_job_args[9] == TEST_USER
            assert batch_job_args[10] == api.api_version
            assert batch_job_args[11:17] == ['8G', '2G', '3G', '5', '2', '2G']
            assert batch_job_args[17:22] == [
                'default', 'false', '[]',
                "__pyfiles__/custom_processes.py,foolib.whl", '100'
            ]
            assert batch_job_args[22:24] == [TEST_USER, job_id]
            assert batch_job_args[24] == '0.0'

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
            with openeogeotrellis.job_registry.ZkJobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
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
            with job_metadata.open('w') as f:
                metadata = api.load_json(JOB_METADATA_FILENAME)
                json.dump(metadata,f)

            with openeogeotrellis.job_registry.ZkJobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "finished"

            # Download
            res = api.get(
                '/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            if api.api_version_compare.at_least("1.0.0"):
                download_url = res["assets"]["out"]["href"]
                assert "openEO_2017-11-21Z.tif" in res["assets"]
                assert [255] == res["assets"]["openEO_2017-11-21Z.tif"]["file:nodata"]
            else:
                download_url = res["links"][0]["href"]

            res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
            assert res.status_code == 200
            assert res.data == TIFF_DUMMY_DATA

            # Get logs
            res = api.get(
                '/jobs/{j}/logs'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            #TODO: mock retrieval of logs from ES
            assert res["logs"] == []

    @mock.patch(
        "openeogeotrellis.configparams.ConfigParams.use_object_storage",
        new_callable=mock.PropertyMock,
    )
    def test_download_from_object_storage(
        self, mock_config_use_object_storage, api, batch_job_output_root, mock_s3_bucket
    ):
        """Test the scenario where the result files we want to download are stored on the objects storage,
        but they are not present in the container that receives the download request.

        Namely: the pod/container that ran the job has been replaced => new container, no files there.
        """

        mock_config_use_object_storage.return_value = True
        job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"
        job_dir: pathlib.Path = batch_job_output_root / job_id
        output_dir_s3_url = to_s3_url(job_dir)
        job_metadata = (job_dir / JOB_METADATA_FILENAME)

        job_metadata_contents = {
            'geometry': {
                'type':
                'Polygon',
                'coordinates': [[[2.0, 51.0], [2.0, 52.0], [3.0, 52.0],
                                 [3.0, 51.0], [2.0, 51.0]]]
            },
            'bbox': [2, 51, 3, 52],
            'start_datetime': '2017-11-21T00:00:00Z',
            'end_datetime': '2017-11-21T00:00:00Z',
            'links': [],
            'assets': {
                'openEO_2017-11-21Z.tif': {
                    'href': f'{output_dir_s3_url}/openEO_2017-11-21Z.tif',
                    'output_dir': output_dir_s3_url,  # Will not exist on the local file system at download time.
                    'type': 'image/tiff; application=geotiff',
                    'roles': ['data'],
                    'bands': [{
                        'name': 'ndvi',
                        'common_name': None,
                        'wavelength_um': None
                    }],
                    'nodata': 255
                }
            },
            'epsg': 4326,
            'instruments': [],
            'processing:facility': 'VITO - SPARK',
            'processing:software': 'openeo-geotrellis-0.3.3a1'
        }

        mock_s3_bucket.put_object(Key=str(job_metadata).strip("/"), Body=json.dumps(job_metadata_contents))
        output_file = str(job_dir / "openEO_2017-11-21Z.tif")
        mock_s3_bucket.put_object(Key=output_file.lstrip("/"), Body=TIFF_DUMMY_DATA)

        # Do a pre-test check: Make sure we are testing that it works when the job_dir is **not** present.
        # Otherwise the test may pass but it passes for the wrong reasons.
        assert not job_dir.exists()

        with self._mock_kazoo_client() as zk:
            # where to import dict_no_none from
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            job_options = {}

            with ZkJobRegistry() as registry:
                registry.register(
                    job_id=job_id,
                    user_id=TEST_USER,
                    api_version="1.0.0",
                    specification=dict(
                        process_graph=data,
                        job_options=job_options,
                    ),
                    title="Fake Test Job",
                    description="Fake job for the purpose of testing"
                )
                registry.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

                # Download
                res = api.get(
                    '/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(200).json
                if api.api_version_compare.at_least("1.0.0"):
                    assert "openEO_2017-11-21Z.tif" in res["assets"]
                    download_url = res["assets"]["openEO_2017-11-21Z.tif"]["href"]
                    assert [255] == res["assets"]["openEO_2017-11-21Z.tif"]["file:nodata"]
                else:
                    download_url = res["links"][0]["href"]

                res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
                assert res.status_code == 200
                assert res.data == TIFF_DUMMY_DATA

    @mock.patch(
        "openeogeotrellis.configparams.ConfigParams.use_object_storage",
        new_callable=mock.PropertyMock,
    )
    def test_download_without_object_storage(
        self, mock_config_use_object_storage, api, batch_job_output_root
    ):
        """Test explicitly that the scenario where we **do not* use the objects storage still works correctly.

        Some changes were introduced be able to download from S3, so we want to be sure the existing
        stuff works the same as before.
        """

        mock_config_use_object_storage.return_value = False
        job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"
        job_dir: pathlib.Path = batch_job_output_root / job_id
        job_metadata = job_dir / JOB_METADATA_FILENAME

        job_metadata_contents = {
            'geometry': {
                'type':
                'Polygon',
                'coordinates': [[[2.0, 51.0], [2.0, 52.0], [3.0, 52.0],
                                 [3.0, 51.0], [2.0, 51.0]]]
            },
            'bbox': [2, 51, 3, 52],
            'start_datetime': '2017-11-21T00:00:00Z',
            'end_datetime': '2017-11-21T00:00:00Z',
            'links': [],
            'assets': {
                'openEO_2017-11-21Z.tif': {
                    'href': f'{job_dir}/openEO_2017-11-21Z.tif',
                    'output_dir': str(job_dir),  # dir on local file, not in object storage
                    'type': 'image/tiff; application=geotiff',
                    'roles': ['data'],
                    'bands': [{
                        'name': 'ndvi',
                        'common_name': None,
                        'wavelength_um': None
                    }],
                    'nodata': 255
                }
            },
            'epsg': 4326,
            'instruments': [],
            'processing:facility': 'VITO - SPARK',
            'processing:software': 'openeo-geotrellis-0.3.3a1'
        }

        # Set up fake output files and job metadata on the local file system.
        job_dir.mkdir(parents=True)

        # We want to check that download succeeds for both files "openEO_2017-11-21Z.tif" and "out".
        # The generic name "out" has a different decision branch handling it, so we test it explicitely.
        job_output1 = (job_dir / "out")
        with job_output1.open('wb') as f:
            f.write(TIFF_DUMMY_DATA)

        job_output2 = (job_dir / "openEO_2017-11-21Z.tif")
        with job_output2.open('wb') as f:
            f.write(TIFF_DUMMY_DATA)

        with job_metadata.open('w') as f:
            json.dump(job_metadata_contents,f)

        with self._mock_kazoo_client() as zk:
            # where to import dict_no_none from
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            job_options = {}

            with ZkJobRegistry() as registry:
                registry.register(
                    job_id=job_id,
                    user_id=TEST_USER,
                    api_version="1.0.0",
                    specification=dict(
                        process_graph=data,
                        job_options=job_options,
                    ),
                    title="Fake Test Job",
                    description="Fake job for the purpose of testing"
                )
                registry.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

                # Download
                res = api.get(
                    '/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(200).json

                # Verify download of "openEO_2017-11-21Z.tif" works
                if api.api_version_compare.at_least("1.0.0"):
                    assert "openEO_2017-11-21Z.tif" in res["assets"]
                    download_url = res["assets"]["openEO_2017-11-21Z.tif"]["href"]
                    download_url_out = res["assets"]["out"]["href"]
                    assert [255] == res["assets"]["openEO_2017-11-21Z.tif"]["file:nodata"]
                else:
                    download_url = res["links"][0]["href"]

                res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
                assert res.status_code == 200
                assert res.data == TIFF_DUMMY_DATA

                # Also verify that downloading the file named "out" works.
                if api.api_version_compare.at_least("1.0.0"):
                    res = api.client.get(download_url_out, headers=TEST_USER_AUTH_HEADER)
                    assert res.status_code == 200
                    assert res.data == TIFF_DUMMY_DATA

    def test_create_and_start_job_options(self, api, tmp_path, monkeypatch, batch_job_output_root):
        with self._mock_kazoo_client() as zk, \
                self._mock_utcnow() as un, \
                mock.patch.dict("os.environ", {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/custom_processes.py,data/deps/foolib.whl"}):

            openeo_flask_dir = tmp_path / "openeo-flask"
            openeo_flask_dir.mkdir()
            (openeo_flask_dir / "foolib.whl").touch()
            (openeo_flask_dir / "__pyfiles__").mkdir()
            (openeo_flask_dir / "__pyfiles__" / "custom_processes.py").touch()
            monkeypatch.chdir(openeo_flask_dir)

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
            job_dir = batch_job_output_root / job_id
            job_output = job_dir / "out"
            job_log = job_dir / "log"
            job_metadata = job_dir / JOB_METADATA_FILENAME
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_log.name
            assert batch_job_args[6] == job_metadata.name
            assert batch_job_args[9] == TEST_USER
            assert batch_job_args[10] == api.api_version
            assert batch_job_args[11:17] == ['3g', '11g', '3G', '5', '4', '10000G']
            assert batch_job_args[17:22] == [
                'somequeue', 'false', '[]',
                '__pyfiles__/custom_processes.py,foolib.whl', '100'
            ]
            assert batch_job_args[22:24] == [TEST_USER, job_id]
            assert batch_job_args[24] == '0.0'

    def test_cancel_job(self, api):
        with self._mock_kazoo_client() as zk:
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
            with openeogeotrellis.job_registry.ZkJobRegistry() as reg:
                reg.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "running"

            # Cancel
            with mock.patch('subprocess.run') as run:
                res = api.delete('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER)
                res.assert_status_code(204)
                run.assert_called_once()
                command = run.call_args[0][0]
                assert command == [
                    'curl',
                    '--location-trusted',
                    '--fail',
                    '--negotiate',
                    '-u',
                    ':',
                    '--insecure',
                    '-X',
                    'PUT',
                    '-H',
                    'Content-Type: application/json',
                    '-d',
                    '{"state": "KILLED"}',
                    'https://epod-master1.vgt.vito.be:8090/ws/v1/cluster/apps/application_1587387643572_0842/state'
                ]
