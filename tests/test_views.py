import collections
import contextlib
import datetime
import decimal
import json
import logging
import os
import pathlib
import re
import subprocess
from typing import Optional
from unittest import mock

import boto3
import kazoo.exceptions
import pytest
import requests
import time_machine
from elasticsearch.exceptions import ConnectionTimeout
from openeo.util import deep_get
from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.testing import (
    TEST_USER,
    TEST_USER_AUTH_HEADER,
    TEST_USER_BEARER_TOKEN,
    TIFF_DUMMY_DATA,
    ApiTester,
    DictSubSet,
    ListSubSet,
    RegexMatcher,
)

import openeogeotrellis.job_registry
import openeogeotrellis.sentinel_hub.batchprocessing
from openeogeotrellis.integrations.s3proxy.asset_urls import PresignedS3AssetUrls
from openeogeotrellis.backend import JOB_METADATA_FILENAME, GpsBatchJobs
from openeogeotrellis.job_registry import DoubleJobRegistry, ZkJobRegistry
from openeogeotrellis.testing import KazooClientMock, gps_config_overrides
from openeogeotrellis.utils import to_s3_url

from .data import TEST_DATA_ROOT


class TestCapabilities:

    def test_capabilities(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        assert deep_get(capabilities, "billing", "currency") == "credits"


    def test_file_formats(self, api100):
        formats = api100.get("/file_formats").assert_status_code(200).json
        assert "GeoJSON" in formats["input"]
        assert "GTiff" in formats["output"]
        assert "CovJSON" in formats["output"]
        assert "netCDF" in formats["output"]
        assert "Parquet" in formats["output"]
        assert "description" in deep_get(formats, "output", "PNG", "parameters", "colormap")

    @pytest.mark.parametrize(
        ["path", "expected"],
        [
            ("/health", {"mode": "spark", "status": "OK", "count": 14}),
            ("/health?mode=spark", {"mode": "spark", "status": "OK", "count": 14}),
            ("/health?mode=jvm", {"mode": "jvm", "status": "OK", "pi": "3.141592653589793"}),
            ("/health?mode=basic", {"mode": "basic", "status": "OK"}),
        ],
    )
    def test_health_default(self, api, path, expected):
        resp = api.get(path).assert_status_code(200)
        assert resp.json == expected


    def test_credentials_oidc(self, api):
        resp = api.get("/credentials/oidc").assert_status_code(200)
        assert resp.json == {
            "providers": [
                {
                    "title": "Test ID",
                    "id": "testid",
                    "issuer": "https://oidc.test",
                    "scopes": ["openid"],
                    "default_clients": [
                        {
                            "grant_types": ["urn:ietf:params:oauth:grant-type:device_code+pkce", "refresh_token"],
                            "id": "badcafef00d",
                        }
                    ],
                }
            ]
        }


    def test_deploy_metadata(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        semver_alike = RegexMatcher(r"^\d+\.\d+\.\d+")
        assert deep_get(capabilities, "_backend_deploy_metadata") == {
            "date": RegexMatcher(r"\d{4}-\d{2}-\d{2}.*Z$"),
            "versions": {
                "openeo": semver_alike,
                "openeo_driver": semver_alike,
                "openeo-geopyspark": semver_alike,
                "geopyspark": semver_alike,
                "geotrellis-extensions": semver_alike,
            },
        }
        assert deep_get(capabilities, "processing:software") == {
            "openeo": semver_alike,
            "openeo_driver": semver_alike,
            "openeo-geopyspark": semver_alike,
            "geopyspark": semver_alike,
            "geotrellis-extensions": semver_alike,
        }

    def test_capabilities_extras(self, api100):
        with gps_config_overrides(
            capabilities_extras={
                "foo": ["bar", "baz"],
                "links": [{"rel": "flavor", "href": "https://flavors.test/sweet"}],
            }
        ):
            capabilities = api100.get("/").assert_status_code(200).json

        assert capabilities["foo"] == ["bar", "baz"]
        assert len(capabilities["links"]) > 1
        assert capabilities["links"][-1] == {"rel": "flavor", "href": "https://flavors.test/sweet"}



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
    def _mock_kazoo_client() -> KazooClientMock:
        # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking of `KazooClient`
        zk_client = KazooClientMock()
        with mock.patch.object(openeogeotrellis.job_registry, 'KazooClient', return_value=zk_client):
            yield zk_client

    @staticmethod
    @contextlib.contextmanager
    def _mock_sentinelhub_batch_processing_service():
        service = mock.Mock()
        with mock.patch.object(
            openeogeotrellis.sentinel_hub.batchprocessing.SentinelHubBatchProcessing,
            "get_batch_processing_service",
            return_value=service,
        ):
            yield service

    def test_get_user_jobs_no_auth(self, api):
        api.get('/jobs').assert_status_code(401).assert_error_code("AuthenticationRequired")

    def test_get_user_jobs_empty(self, api):
        with self._mock_kazoo_client() as zk:
            result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert result == {"jobs": [], "links": []}

    def test_create_job(self, api, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")
        with self._mock_kazoo_client() as zk:
            data = api.get_process_graph_dict(
                self.DUMMY_PROCESS_GRAPH, title="Dummy", description="Dummy job!"
            )
            res = api.post(
                "/jobs", json=data, headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(201)
            job_id = res.headers["OpenEO-Identifier"]
            meta_data = zk.get_json_decoded(
                f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
            )
            assert meta_data["job_id"] == job_id
            assert meta_data["user_id"] == TEST_USER
            assert meta_data["status"] == "created"
            assert meta_data["api_version"] == api.api_version
            assert meta_data["application_id"] == None
            assert meta_data["created"] == "2020-04-20T16:04:03Z"
            assert meta_data["title"] == "Dummy"
            assert meta_data["description"] == "Dummy job!"

    def test_create_and_get(self, api, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")
        with self._mock_kazoo_client() as zk:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

        if api.api_version_compare.at_least("1.0.0"):
            expected = DictSubSet({
                "id": job_id,
                "process": {"process_graph": self.DUMMY_PROCESS_GRAPH},
                "status": "created",
                "created": "2020-04-20T16:04:03Z",
                "updated": "2020-04-20T16:04:03Z",
                "title": "Dummy",
            })
        else:
            expected = DictSubSet({
                "id": job_id,
                "process_graph": self.DUMMY_PROCESS_GRAPH,
                "status": "submitted",
                "submitted": "2020-04-20T16:04:03Z",
                "title": "Dummy",
            })
        assert res == expected

    def test_create_and_get_user_jobs(self, api, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")
        with self._mock_kazoo_client() as zk:
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            created = "created" if api.api_version_compare.at_least("1.0.0") else "submitted"
            assert result == {
                "jobs": [
                    DictSubSet({
                        "id": job_id,
                        "title": "Dummy",
                        "status": created,
                        created: "2020-04-20T16:04:03Z",
                        "updated": "2020-04-20T16:04:03Z",
                    }),
                ],
                "links": []
            }

    @mock.patch("openeogeotrellis.logs.Elasticsearch.search")
    def test_create_and_start_and_download(
        self, mock_search, api, tmp_path, monkeypatch, batch_job_output_root, job_registry, time_machine
    ):
        time_machine.move_to("2020-04-20T16:04:03Z")

        with self._mock_kazoo_client() as zk, \
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
            assert job_id.startswith("j-")

            # Start job
            with mock.patch('subprocess.run') as run:
                os.mkdir(batch_job_output_root / job_id)
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
            run.assert_called_once()
            batch_job_args = run.call_args[0][0]

            # Check batch in/out files
            job_dir = batch_job_output_root / job_id
            job_output = job_dir / "out"
            job_metadata = job_dir / JOB_METADATA_FILENAME
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_metadata.name
            assert batch_job_args[8] == TEST_USER
            assert batch_job_args[9] == api.api_version

            assert batch_job_args[10:16] == ['8G', '2G', '3G', '5', '2', '2G']
            assert batch_job_args[16:21] == [
                'default', 'false', '[]',
                "__pyfiles__/custom_processes.py,foolib.whl", '100'
            ]
            assert batch_job_args[21:23] == [TEST_USER, job_id]
            assert batch_job_args[23] == '0.1'

            # Check metadata in zookeeper
            meta_data = zk.get_json_decoded(
                f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
            )
            assert meta_data["job_id"] == job_id
            assert meta_data["user_id"] == TEST_USER
            assert meta_data["status"] == "queued"
            assert meta_data["api_version"] == api.api_version
            assert json.loads(meta_data["specification"]) == {
                "process_graph": {
                    "loadcollection1": {
                        "process_id": "load_collection",
                        "arguments": {"id": "BIOPAR_FAPAR_V1_GLOBAL"},
                        "result": True,
                    }
                },
                "job_options": {"log_level": "info"},
            }
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
            dbl_job_registry = DoubleJobRegistry(
                # TODO #236/#498/#632 phase out ZkJobRegistry
                zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk)),
                elastic_job_registry=job_registry,
            )
            with dbl_job_registry as jr:
                jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)

            meta_data = zk.get_json_decoded(
                f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
            )
            assert meta_data["status"] == "running"
            res = (
                api.get(f"/jobs/{job_id}", headers=TEST_USER_AUTH_HEADER)
                .assert_status_code(200)
                .json
            )
            assert res["status"] == "running"

            # Try to download results too early
            res = api.get(f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER)
            res.assert_error(status_code=400, error_code="JobNotFinished")

            # Set up fake output and finish
            with job_output.open("wb") as f:
                f.write(TIFF_DUMMY_DATA)
            with job_metadata.open("w") as f:
                metadata = api.load_json(JOB_METADATA_FILENAME)
                json.dump(metadata, f)

            with dbl_job_registry as jr:
                jr.set_status(
                    job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED
                )
            res = (
                api.get(f"/jobs/{job_id}", headers=TEST_USER_AUTH_HEADER)
                .assert_status_code(200)
                .json
            )
            assert res["status"] == "finished"

            # Download
            res = (
                api.get(f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER)
                .assert_status_code(200)
                .json
            )
            if api.api_version_compare.at_least("1.0.0"):
                download_url = res["assets"]["out"]["href"]
                assert "openEO_2017-11-21Z.tif" in res["assets"]
            else:
                download_url = res["links"][0]["href"]

            res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
            assert res.status_code == 200
            assert res.data == TIFF_DUMMY_DATA

            search_hits = [
                {
                    "_source": {
                        "levelname": "ERROR",
                        "message": "A message with the loglevel filled in",
                    },
                    "sort": 1,
                }
            ]
            expected_log_entries = [
                {
                    "id": "1",
                    "level": "error",
                    "message": "A message with the loglevel filled in",
                }
            ]

            mock_search.return_value = {
                "hits": {"hits": search_hits},
            }
            # Get logs
            res = (
                api.get(f"/jobs/{job_id}/logs", headers=TEST_USER_AUTH_HEADER)
                .assert_status_code(200)
                .json
            )

            assert res["logs"] == expected_log_entries

    def test_providers_present(self, api, tmp_path, monkeypatch, batch_job_output_root, job_registry, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")

        with self._mock_kazoo_client() as zk, mock.patch.dict(
            "os.environ", {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/custom_processes.py,data/deps/foolib.whl"}
        ):
            openeo_flask_dir = tmp_path / "openeo-flask"
            openeo_flask_dir.mkdir()
            (openeo_flask_dir / "foolib.whl").touch()
            (openeo_flask_dir / "__pyfiles__").mkdir()
            (openeo_flask_dir / "__pyfiles__" / "custom_processes.py").touch()
            monkeypatch.chdir(openeo_flask_dir)

            # Create job
            processing_graph = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            res = api.post("/jobs", json=processing_graph, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers["OpenEO-Identifier"]
            assert job_id.startswith("j-")

            # Start job
            with mock.patch("subprocess.run") as run:
                os.mkdir(batch_job_output_root / job_id)
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER).assert_status_code(202)
            run.assert_called_once()

            # Check batch in/out files
            job_dir = batch_job_output_root / job_id
            job_output = job_dir / "out"
            job_metadata = job_dir / JOB_METADATA_FILENAME

            from openeogeotrellis._version import __version__

            expected_providers = [
                {
                    "name": "VITO",
                    "description": "This data was processed on an openEO backend maintained by VITO.",
                    "roles": ["processor"],
                    "processing:facility": "openEO Geotrellis backend",
                    "processing:software": {"Geotrellis backend": __version__},
                    "processing:expression": [{"format": "openeo", "expression": processing_graph}],
                }
            ]

            # Set up fake output
            job_metadata_contents = {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[2.0, 51.0], [2.0, 52.0], [3.0, 52.0], [3.0, 51.0], [2.0, 51.0]]],
                },
                "bbox": [2, 51, 3, 52],
                "start_datetime": "2017-11-21T00:00:00Z",
                "end_datetime": "2017-11-21T00:00:00Z",
                "links": [],
                "assets": {
                    "openEO_2017-11-21Z.tif": {
                        "href": f"{job_dir}/openEO_2017-11-21Z.tif",
                        "output_dir": str(job_dir),  # dir on local file, not in object storage
                        "type": "image/tiff; application=geotiff",
                        "roles": ["data"],
                        "bands": [{"name": "ndvi", "common_name": None, "wavelength_um": None}],
                        "nodata": 255,
                    }
                },
                "epsg": 4326,
                "instruments": [],
                "providers": expected_providers,
            }

            with job_output.open("wb") as f:
                f.write(TIFF_DUMMY_DATA)
            with job_metadata.open("w") as f:
                # metadata = api.load_json(JOB_METADATA_FILENAME)
                json.dump(job_metadata_contents, f)

            # Fake update from job tracker
            dbl_job_registry = DoubleJobRegistry(
                # TODO #236/#498/#632 phase out ZkJobRegistry
                zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk)),
                elastic_job_registry=job_registry,
            )
            with dbl_job_registry as jr:
                jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

            res = api.get(f"/jobs/{job_id}", headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "finished"

            # Get the job results and verify the contents.
            res = api.get(f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

            assert "providers" in res
            assert res["providers"] == expected_providers

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

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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
                else:
                    download_url = res["links"][0]["href"]

                res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
                assert res.status_code == 200
                assert res.data == TIFF_DUMMY_DATA

    @mock.patch(
        "openeogeotrellis.configparams.ConfigParams.use_object_storage",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "config_overrides,idp_enabled,auth_header,expected_code", [
            # When using the new PresignedS3AssetUrls with ipd in place but not required config in place we should
            # be on old behavior
            [
                {
                    "asset_url": PresignedS3AssetUrls(),
                    "s3_region_proxy_endpoints": {},  # Mimic no proxy configured => missing required config
                }, True, False, 401
            ],
            [
                {
                    "asset_url": PresignedS3AssetUrls(),
                    "s3_region_proxy_endpoints": {},  # Mimic no proxy configured => missing required config
                }, True, True, 200
            ],
            # When using the new PresignedS3AssetUrls with ipd & required config in place we should never fail
            [
                {
                    "asset_url": PresignedS3AssetUrls()
                }, True, False, 200
            ],
            [
                {
                    "asset_url": PresignedS3AssetUrls()
                }, True, True, 200
            ],
            # When using the new PresignedS3AssetUrls without having the required config in place for IDP
            # we should not fail so if request occur like before (auth header present) it works, without if fails
            [
                {
                    "asset_url": PresignedS3AssetUrls(),
                }, False, True, 200
            ],
            [
                {
                    "asset_url": PresignedS3AssetUrls(),
                }, False, False, 401
            ],
            # When using the new PresignedS3AssetUrls without having the required config in place we should not fail
            # so if request occur like before (auth header present) it works, without if fails
            [
                {
                    "asset_url": PresignedS3AssetUrls(),
                    "s3_region_proxy_endpoints": {},  # Mimic no proxy configured
                }, False, True, 200
            ],
            [
                {
                    "asset_url": PresignedS3AssetUrls(),
                    "s3_region_proxy_endpoints": {},  # Mimic no proxy configured
                }, False, False, 401
            ],
            [{}, False, True, 200],  # Old signer works if auth header is used
            [{}, False, False, 401],  # Old signer fails if auth header not used for retrieval
        ]

    )
    def test_download_from_object_storage_via_proxy(
        self, mock_config_use_object_storage, moto_server, batch_job_output_root, mock_s3_bucket, mock_sts_client,
        sts_endpoint_on_driver, api, config_overrides, idp_enabled, auth_header: bool, expected_code: int
    ):
        """Test the scenario where the result files we want to download are stored on the objects storage,
        but they are not present in the container that receives the download request.

        Namely: the pod/container that ran the job has been replaced => new container, no files there.
        Because there is an S3 proxy it will be downloaded straight.
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

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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
                else:
                    download_url = res["links"][0]["href"]

                retrieve_url = api.client.get
                if download_url.startswith("http://127.0.0.1:"):
                    # pre-signed urls don't woark with flask retriever
                    def retrieve_url_and_set_data(*args, **kwargs):
                        result = requests.get(*args, **kwargs)
                        setattr(result, "data", result.text.encode("utf-8"))
                        return result
                    retrieve_url = retrieve_url_and_set_data

                if auth_header:
                    res = retrieve_url(download_url, headers=TEST_USER_AUTH_HEADER)
                else:
                    res = retrieve_url(download_url)

                assert res.status_code == expected_code
                if 200 <= expected_code < 300:
                    # For successfull api requests check response data
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

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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

    def test_create_and_start_job_options(self, api, tmp_path, monkeypatch, batch_job_output_root, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")

        with self._mock_kazoo_client() as zk, \
                mock.patch.dict("os.environ", {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/custom_processes.py,data/deps/foolib.whl"}):

            openeo_flask_dir = tmp_path / "openeo-flask"
            openeo_flask_dir.mkdir()
            (openeo_flask_dir / "foolib.whl").touch()
            (openeo_flask_dir / "__pyfiles__").mkdir()
            (openeo_flask_dir / "__pyfiles__" / "custom_processes.py").touch()
            monkeypatch.chdir(openeo_flask_dir)

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            data["job_options"] = {"driver-memory": "3g", "executor-memory": "11g","executor-cores":"4","queue":"somequeue","driver-memoryOverhead":"10G", "soft-errors":"false", "udf-dependency-archives":["https://host.com/my.jar"]}
            res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
            job_id = res.headers['OpenEO-Identifier']
            # Start job
            with mock.patch('subprocess.run') as run:
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
                # Trigger job start
                api.post(
                    f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()
                batch_job_args = run.call_args[0][0]

            # Check batch in/out files
            job_dir = batch_job_output_root / job_id
            job_output = job_dir / "out"
            job_metadata = job_dir / JOB_METADATA_FILENAME
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_metadata.name
            assert batch_job_args[8] == TEST_USER
            assert batch_job_args[9] == api.api_version
            assert batch_job_args[10:16] == ['3g', '11g', '3G', '5', '4', '10G']
            assert batch_job_args[16:21] == [
                'somequeue', 'false', '[]',
                '__pyfiles__/custom_processes.py,foolib.whl', '100'
            ]
            assert batch_job_args[21:23] == [TEST_USER, job_id]
            assert batch_job_args[23] == '0.0'
            assert batch_job_args[27] == 'https://host.com/my.jar'

    @pytest.mark.parametrize(["boost"], [
        [("driver-memory", "99999g")],
        [("executor-memory", "99999g")],
        [("driver-cores", "99")],
        [("executor-cores", "99")],
        [("driver-memoryOverhead", "99999G")],
        [("executor-memoryOverhead", "99999G")],
        [("python-memory", "99999G")],
    ])
    def test_create_and_start_job_options_too_large(self, api, boost, monkeypatch):
        monkeypatch.setenv("KUBE", "TRUE")
        nonsense_process_graph = {
            "process_graph": {
                "add1": {
                    "process_id": "add",
                    "arguments": {
                        "x": 1,
                        "y": 2
                    },
                    "result": True
                }
            },
            "parameters": []
        }
        # Create job
        data = api.get_process_graph_dict(nonsense_process_graph, title="nonsense_process_graph")
        job_options = {
            "driver-memory": "1g",
            "executor-memory": "1g",

            "driver-cores": "2",
            "executor-cores": "2",

            "driver-memoryOverhead": "1G",
            "executor-memoryOverhead": "1G",
            "queue": "somequeue",
        }
        print(boost)
        job_options[boost[0]] = boost[1]

        data["job_options"] = job_options
        res = api.post("/jobs", json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        # Trigger job start
        api.post(f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER).assert_status_code(400)

    def test_create_and_start_job_options_too_large_python(self, api, monkeypatch):
        monkeypatch.setenv("KUBE", "TRUE")
        nonsense_process_graph = {
            "process_graph": {"add1": {"process_id": "add", "arguments": {"x": 1, "y": 2}, "result": True}},
            "parameters": [],
        }
        # Create job
        data = api.get_process_graph_dict(nonsense_process_graph, title="nonsense_process_graph")
        job_options = {
            "executor-memory": "33g",
            "python-memory": "33g",
        }

        data["job_options"] = job_options
        res = api.post('/jobs', json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
        job_id = res.headers['OpenEO-Identifier']
        # Trigger job start
        res2 = api.post(f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER)
        print(res2.text)
        res2.assert_status_code(400)

    def test_cancel_job(self, api, job_registry):
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
                    f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()

            # Fake running
            dbl_job_registry = DoubleJobRegistry(
                # TODO #236/#498/#632 phase out ZkJobRegistry
                zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk)),
                elastic_job_registry=job_registry,
            )
            with dbl_job_registry as jr:
                jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "running"

            # Cancel
            with mock.patch('subprocess.run') as run:
                res = api.delete('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER)
            res.assert_status_code(204)
            run.assert_called_once()
            command = run.call_args[0][0]
            assert command == [
                "curl",
                "--location-trusted",
                "--fail",
                "--negotiate",
                "-u",
                ":",
                "--insecure",
                "-X",
                "PUT",
                "-H",
                "Content-Type: application/json",
                "-d",
                '{"state": "KILLED"}',
                "https://epod-master1.vgt.vito.be:8090/ws/v1/cluster/apps/application_1587387643572_0842/state",
            ]
            meta_data = zk.get_json_decoded(
                f"/openeo.test/jobs/done/{TEST_USER}/{job_id}"
            )
            assert meta_data == DictSubSet({"status": "canceled"})
            assert job_registry.db[job_id] == DictSubSet({"status": "canceled"})

    def test_delete_job(self, api, job_registry):
        with self._mock_kazoo_client() as zk:
            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH)
            res = api.post(
                "/jobs", json=data, headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(201)
            job_id = res.headers["OpenEO-Identifier"]
            # Start job
            with mock.patch("subprocess.run") as run:
                stdout = api.read_file("spark-submit-stdout.txt")
                run.return_value = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=stdout, stderr=""
                )
                # Trigger job start
                api.post(
                    f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER
                ).assert_status_code(202)
                run.assert_called_once()

            # Fake running
            dbl_job_registry = DoubleJobRegistry(
                # TODO #236/#498/#632 phase out ZkJobRegistry
                zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk)),
                elastic_job_registry=job_registry,
            )
            with dbl_job_registry as jr:
                jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
            res = (
                api.get(f"/jobs/{job_id}", headers=TEST_USER_AUTH_HEADER)
                .assert_status_code(200)
                .json
            )
            assert res["status"] == "running"

            # Cancel
            with mock.patch("subprocess.run") as run:
                res = api.delete(
                    f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER
                )
            res.assert_status_code(204)
            run.assert_called_once()
            meta_data = zk.get_json_decoded(
                f"/openeo.test/jobs/done/{TEST_USER}/{job_id}"
            )
            assert meta_data == DictSubSet({"status": "canceled"})
            assert job_registry.db[job_id] == DictSubSet({"status": "canceled"})

            # Delete
            res = api.delete(f"/jobs/{job_id}", headers=TEST_USER_AUTH_HEADER)
            res.assert_status_code(204)
            with pytest.raises(kazoo.exceptions.NoNodeError):
                _ = zk.get_json_decoded(f"/openeo.test/jobs/done/{TEST_USER}/{job_id}")
            # TODO
            # assert job_registry.db[job_id] == DictSubSet({"deleted": True})

    @mock.patch("openeogeotrellis.logs.Elasticsearch.search")
    def test_get_job_logs_skips_lines_with_empty_loglevel(self, mock_search, api):
        search_hits = [
            {
                "_source": {
                    "levelname": "ERROR",
                    "message": "A message with the loglevel filled in",
                },
                "sort": 1,
            },
            {
                "_source": {
                    "levelname": None,
                    "message": "A message with an empty loglevel",
                },
                "sort": 2,
            },
        ]
        expected_log_entries = [
            {
                "id": "1",
                "level": "error",
                "message": "A message with the loglevel filled in",
            }
        ]

        mock_search.return_value = {
            "hits": {"hits": search_hits},
        }
        job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"

        with self._mock_kazoo_client() as zk:
            # where to import dict_no_none from
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            job_options = {}

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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
                    description="Fake job for the purpose of testing",
                )
                registry.set_status(
                    job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED
                )

            # Get logs
            res = (
                api.get(f"/jobs/{job_id}/logs", headers=TEST_USER_AUTH_HEADER)
                .assert_status_code(200)
                .json
            )

            assert res["logs"] == expected_log_entries

    @mock.patch("openeogeotrellis.logs.Elasticsearch.search")
    def test_get_job_logs_with_connection_timeout(self, mock_search, api, caplog):
        caplog.set_level(logging.ERROR)

        sensitive_info = "our server"

        mock_search.side_effect = ConnectionTimeout(
            'TIMEOUT',
            f"HTTPSConnectionPool(host='{sensitive_info}', port=443): Read timed out. (read timeout=60)",
            Exception(f"HTTPSConnectionPool(host='{sensitive_info}', port=443): Read timed out. (read timeout=60)"))
        job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"

        expected_message = (
            'Log collection for job 6d11e901-bb5d-4589-b600-8dfb50524740 '
            + 'failed. (req_id: r-1234-5678-91011) '
            + "OpenEOApiException(status_code=504, code='Internal', "
            + "message='Temporary failure while retrieving logs: "
            + 'ConnectionTimeout. Please try again and report this error if it '
            + "persists. (ref: no-request)', "
            + "id='no-request')"
        )
        expected_log_entries = [
            {
                "id": "",
                "code": "Internal",
                "level": "error",
                "message": expected_message,
            }
        ]

        with self._mock_kazoo_client() as zk:
            # where to import dict_no_none from
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            job_options = {}

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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
                    description="Fake job for the purpose of testing",
                )
                registry.set_status(
                    job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED
                )

            from openeo_driver.util.logging import FlaskRequestCorrelationIdLogging
            with mock.patch.object(FlaskRequestCorrelationIdLogging, "_build_request_id",
                                   return_value="r-1234-5678-91011"):
                # Get logs
                res = (
                    api.get(f"/jobs/{job_id}/logs", headers=TEST_USER_AUTH_HEADER)
                    .assert_status_code(200)
                    .json
                )

            #
            # Also explicitly verify that the security sensitive info from the old message is no longer present.
            # In particular, we don't want "host=" to leak the URL to our server.
            # This is an extra precaution, though normally the assert below would also fail.
            #
            # Example of the old message:
            # "Log collection failed: ConnectionTimeout('TIMEOUT', \"HTTPSConnectionPool(host='our server', port=443):
            # Read timed out. (read timeout=60)\", ReadTimeoutError(\"HTTPSConnectionPool(host='our server', port=443):
            # Read timed out. (read timeout=60)\"))"
            assert len(res["logs"]) == 1
            assert sensitive_info not in res["logs"][0]["message"]

            # Verify the expected log entry in full
            assert res["logs"] == expected_log_entries

            # The original ConnectionTimeout should be sent to the current (general) logs
            # which are different from the job-specific logs we just retrieved.
            # The ConnectionTimeout info should be in the traceback.
            # To view these logs in caplog.text, run pytest with the '-s' option.
            print(caplog.text)
            assert sensitive_info in caplog.text

    def test_api_job_results_contains_proj_metadata_at_asset_level(self, api, batch_job_output_root):
        """Test that projection metadata at the asset level in the job results
        comes through via the API / view.
        """

        job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"
        job_dir: pathlib.Path = batch_job_output_root / job_id
        job_metadata = job_dir / JOB_METADATA_FILENAME

        job_metadata_contents = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[2.0, 51.0], [2.0, 52.0], [3.0, 52.0], [3.0, 51.0], [2.0, 51.0]]],
            },
            "bbox": [2, 51, 3, 52],
            "start_datetime": "2017-11-21T00:00:00Z",
            "end_datetime": "2017-11-21T00:00:00Z",
            "links": [],
            "assets": {
                "openEO_2017-11-21Z.tif": {
                    "href": f"{job_dir}/openEO_2017-11-21Z.tif",
                    "output_dir": str(job_dir),  # dir on local file, not in object storage
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "ndvi", "common_name": None, "wavelength_um": None}],
                    "nodata": 255,
                    # For this test: make the asset"s projection metadata different
                    # from the values at the top level, so we detect the difference.
                    # All three properties have just fake nonsensical values.
                    "proj:epsg": 6234,
                    "proj:bbox": [1.0, 2.0, 3.0, 4.0],
                    "proj:shape": [321, 654],
                    "raster:bands": [
                        {
                            "statistics": {
                                "maximum": 641.22131,
                                "mean": 403.31786,
                                "minimum": 149.76655,
                                "stddev": 98.38930,
                                "valid_percent": 100.0,
                            }
                        }
                    ],
                }
            },
            "epsg": 4326,
            "instruments": [],
            "processing:facility": "VITO - SPARK",
            "processing:software": "openeo-geotrellis-0.3.3a1",
        }

        # Set up fake output files and job metadata on the local file system.
        job_dir.mkdir(parents=True)

        # We want to check that download succeeds for both files "openEO_2017-11-21Z.tif" and "out".
        # The generic name "out" has a different decision branch handling it, so we test it explicitly.
        job_output1 = job_dir / "out"
        with job_output1.open("wb") as f:
            f.write(TIFF_DUMMY_DATA)

        job_output2 = job_dir / "openEO_2017-11-21Z.tif"
        with job_output2.open("wb") as f:
            f.write(TIFF_DUMMY_DATA)

        with job_metadata.open("w") as f:
            json.dump(job_metadata_contents, f)

        with self._mock_kazoo_client() as zk:
            # where to import dict_no_none from
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            job_options = {}

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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
                    description="Fake job for the purpose of testing",
                )
                registry.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

                # Download
                res = (
                    api.get("/jobs/{j}/results".format(j=job_id), headers=TEST_USER_AUTH_HEADER)
                    .assert_status_code(200)
                    .json
                )

                assert "openEO_2017-11-21Z.tif" in res["assets"]
                assert "proj:epsg" in res["assets"]["openEO_2017-11-21Z.tif"]
                assert res["assets"]["openEO_2017-11-21Z.tif"]["proj:epsg"] == 6234

                assert "proj:bbox" in res["assets"]["openEO_2017-11-21Z.tif"]
                assert res["assets"]["openEO_2017-11-21Z.tif"]["proj:bbox"] == [1.0, 2.0, 3.0, 4.0]

                assert "proj:shape" in res["assets"]["openEO_2017-11-21Z.tif"]
                assert res["assets"]["openEO_2017-11-21Z.tif"]["proj:shape"] == [321, 654]

                assert res["assets"]["openEO_2017-11-21Z.tif"]["raster:bands"] == [
                    {
                        "statistics": {
                            "maximum": 641.22131,
                            "mean": 403.31786,
                            "minimum": 149.76655,
                            "stddev": 98.38930,
                            "valid_percent": 100.0,
                        }
                    }
                ]

    # TODO: This test is known to fail with API v1.1.0 / api110. Add coverage, or update the test.
    #   https://github.com/Open-EO/openeo-geopyspark-driver/issues/440
    def test_api_job_results_contains_proj_metadata_at_item_level(self, api100, batch_job_output_root):
        """Test explicitly that the scenario where we **do not* use the objects storage still works correctly.

        Some changes were introduced be able to download from S3, so we want to be sure the existing
        stuff works the same as before.
        """

        job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"
        job_dir: pathlib.Path = batch_job_output_root / job_id
        job_metadata = job_dir / JOB_METADATA_FILENAME

        job_metadata_contents = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[2.0, 51.0], [2.0, 52.0], [3.0, 52.0], [3.0, 51.0], [2.0, 51.0]]],
            },
            "bbox": [2, 51, 3, 52],
            "start_datetime": "2017-11-21T00:00:00Z",
            "end_datetime": "2017-11-21T00:00:00Z",
            "links": [],
            "assets": {
                "openEO_2017-11-21Z.tif": {
                    "href": f"{job_dir}/openEO_2017-11-21Z.tif",
                    "output_dir": str(job_dir),  # dir on local file, not in object storage
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "ndvi", "common_name": None, "wavelength_um": None}],
                    "nodata": 255,
                    "raster:bands": [
                        {
                            "statistics": {
                                "maximum": 641.22131,
                                "mean": 403.31786,
                                "minimum": 149.76655,
                                "stddev": 98.38930,
                                "valid_percent": 100.0,
                            }
                        }
                    ],
                }
            },
            "epsg": 4326,
            "instruments": [],
            "processing:facility": "VITO - SPARK",
            "processing:software": "openeo-geotrellis-0.3.3a1",
            "proj:shape": [321, 654],
            "proj:bbox": [2, 51, 3, 52],
        }

        # Set up fake output files and job metadata on the local file system.
        job_dir.mkdir(parents=True)

        job_output2 = job_dir / "openEO_2017-11-21Z.tif"
        with job_output2.open("wb") as f:
            f.write(TIFF_DUMMY_DATA)

        with job_metadata.open("w") as f:
            json.dump(job_metadata_contents, f)

        with self._mock_kazoo_client() as zk:
            data = api100.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            job_options = {}

            # TODO #236/#498/#632 eliminate direct dependency on deprecated ZkJobRegistry and related mocking (e.g. `self._mock_kazoo_client()` above)
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
                    description="Fake job for the purpose of testing",
                )
                registry.patch(job_id=job_id, user_id=TEST_USER, **job_metadata_contents)
                registry.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

                # Download
                res = (
                    api100.get("/jobs/{j}/results".format(j=job_id), headers=TEST_USER_AUTH_HEADER)
                    .assert_status_code(200)
                    .json
                )

                assert res == DictSubSet(
                    {
                        "properties": DictSubSet(
                            {
                                "proj:epsg": 4326,
                                "proj:shape": [321, 654],
                                "proj:bbox": [2, 51, 3, 52],
                            }
                        ),
                        "bbox": [2, 51, 3, 52],
                    }
                )

                assert "openEO_2017-11-21Z.tif" in res["assets"]
                assert res["assets"]["openEO_2017-11-21Z.tif"]["raster:bands"] == [
                    {
                        "statistics": {
                            "maximum": 641.22131,
                            "mean": 403.31786,
                            "minimum": 149.76655,
                            "stddev": 98.38930,
                            "valid_percent": 100.0,
                        }
                    }
                ]


class TestSentinelHubBatchJobs:
    """Tests for batch jobs involving SentinelHub collections and batch processes"""

    @pytest.fixture
    def api(self, api_version, client) -> ApiTester:
        """ApiTester, with authentication headers by default"""
        api = ApiTester(
            api_version=api_version, client=client, data_root=TEST_DATA_ROOT
        )
        api.set_auth_bearer_token(TEST_USER_BEARER_TOKEN)
        return api

    @pytest.fixture
    def zk_client(self) -> KazooClientMock:
        zk_client = KazooClientMock()
        with mock.patch.object(
            openeogeotrellis.job_registry, "KazooClient", return_value=zk_client
        ):
            yield zk_client

    @staticmethod
    @contextlib.contextmanager
    def _mock_sentinelhub_batch_processing_service(
        batch_request_id: Optional[str] = "224635b-7d60-40f2-bae6-d30e923bcb83",
    ):
        service = mock.Mock()
        with mock.patch.object(
            openeogeotrellis.sentinel_hub.batchprocessing.SentinelHubBatchProcessing,
            "get_batch_processing_service",
            return_value=service,
        ):
            if batch_request_id:
                service.start_batch_process.return_value = batch_request_id

            yield service

    def _build_process_graph(self, spatial_size: float = 1):
        # Small (no SH batch processing) or large spatial extent (trigger SH batch processing code path)?
        spatial_extent = {
            "west": 3,
            "south": 51,
            "east": 3 + spatial_size,
            "north": 51 + spatial_size,
        }
        process_graph = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "SENTINEL2_L2A_SENTINELHUB",
                    "spatial_extent": spatial_extent,
                    "temporal_extent": ["2023-02-01", "2023-02-20"],
                },
                "result": True,
            }
        }
        return process_graph

    @contextlib.contextmanager
    def _submit_batch_job_mock(self, api: ApiTester):
        """Mock for subprocess calling of batch job submit scripts like submit_batch_job_*.sh"""

        def subprocess_run(cmd, *args, **kwargs):
            assert re.search(r"/submit_batch_job.*\.sh$", cmd[0])
            stdout = api.read_file("spark-submit-stdout.txt")
            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout=stdout, stderr=""
            )

        with mock.patch("subprocess.run", wraps=subprocess_run) as run_mock:
            yield run_mock

    def _check_submit_batch_job_cmd(
        self,
        submit_batch_job_mock,
        batch_job_output_root,
        job_id,
        expected_dependencies="[]",
    ):
        submit_batch_job_mock.assert_called_once()
        args = submit_batch_job_mock.call_args[0][0]

        job_dir = batch_job_output_root / job_id
        job_output = job_dir / "out"
        job_metadata = job_dir / JOB_METADATA_FILENAME
        assert args[2].endswith(".in")
        assert args[3] == str(job_dir)
        assert args[4] == job_output.name
        assert args[5] == job_metadata.name
        assert args[8] == TEST_USER
        assert args[16] == "default"
        assert args[18] == expected_dependencies
        assert args[21:23] == [TEST_USER, job_id]
        return args

    @pytest.mark.parametrize(["spatial_size"], [(0.1,), (1,)])
    def test_create(self, api, job_registry, time_machine, zk_client, spatial_size):
        time_machine.move_to("2020-04-20T12:01:01Z")

        job_data = api.get_process_graph_dict(
            process_graph=self._build_process_graph(spatial_size=spatial_size)
        )
        res = api.post("/jobs", json=job_data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "created",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:01:01Z",
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "created",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:01:01Z",
                "process": {
                    "process_graph": {
                        "loadcollection1": DictSubSet({"process_id": "load_collection"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "created"

    def test_start_small_extent(
        self,
        api,
        job_registry,
        time_machine,
        zk_client,
        batch_job_output_root,
    ):
        time_machine.move_to("2020-04-20T12:01:01Z")

        job_data = api.get_process_graph_dict(
            process_graph=self._build_process_graph(spatial_size=0.1)
        )
        res = api.post("/jobs", json=job_data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet({"status": "created"})
        assert job_registry.db[job_id] == DictSubSet({"status": "created"})
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "created"

        # Start job
        time_machine.move_to("2020-04-20T12:02:02Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, self._mock_sentinelhub_batch_processing_service() as sh_batch_service:
            # Trigger job start
            api.post(f"/jobs/{job_id}/results", json={}).assert_status_code(202)

        # no SH batch processes: submit_batch_job should be called
        self._check_submit_batch_job_cmd(
            submit_batch_job_mock=submit_batch_job,
            batch_job_output_root=batch_job_output_root,
            job_id=job_id,
        )
        sh_batch_service.start_batch_process.assert_not_called()

        # Check metadata in zookeeper
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
            }
        )
        # Check metadata in (elastic) job tracker
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

    def test_start_large_extent(
        self,
        api,
        job_registry,
        time_machine,
        zk_client,
        batch_job_output_root,
    ):
        time_machine.move_to("2020-04-20T12:01:01Z")

        job_data = api.get_process_graph_dict(
            process_graph=self._build_process_graph(spatial_size=1)
        )
        res = api.post("/jobs", json=job_data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet({"status": "created"})
        # Check metadata in (elastic) job tracker
        assert job_registry.db[job_id] == DictSubSet({"status": "created"})
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "created"

        # Start job
        time_machine.move_to("2020-04-20T12:02:02Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, self._mock_sentinelhub_batch_processing_service() as sh_batch_service, mock.patch(
            "kafka.KafkaProducer"
        ) as KafkaProducer:
            # Trigger job start
            api.post(f"/jobs/{job_id}/results", json={}).assert_status_code(202)

        # submit_batch_job not called yet
        submit_batch_job.assert_not_called()
        sh_batch_service.start_batch_process.assert_called_once_with(
            "sentinel-2-l2a", "sentinel-2-l2a", *([mock.ANY] * 8)
        )
        KafkaProducer.return_value.send.assert_called_once_with(
            topic="openeo-async-tasks", value=mock.ANY, headers=None
        )

        # Check metadata in zookeeper
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ],
            }
        )
        # Check metadata in (elastic) job tracker
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ],
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

    def test_start_large_extent_with_polarization_based_on_bands(
        self,
        api,
        job_registry,
        time_machine,
        zk_client,
        batch_job_output_root,
    ):
        time_machine.move_to("2020-04-20T12:01:01Z")
        true = True
        null = None
        job_data = {
            "job_options": {"sentinel-hub": {"input": "batch"}},
            "process": {
                "process_graph": {
                    "filterspatial1": {
                        "arguments": {
                            "data": {
                                "from_node": "loadcollection1"
                            },
                            "geometries": {
                                "features": [
                                    {
                                        "geometry": {
                                            "coordinates": [
                                                [
                                                    [
                                                        6.42,
                                                        51.03
                                                    ],
                                                    [
                                                        6.42,
                                                        51.11
                                                    ],
                                                    [
                                                        6.54,
                                                        51.11
                                                    ],
                                                    [
                                                        6.54,
                                                        51.03
                                                    ]
                                                ]
                                            ],
                                            "type": "Polygon"
                                        },
                                        "properties": {},
                                        "type": "Feature"
                                    },
                                    {
                                        "geometry": {
                                            "coordinates": [
                                                [
                                                    [
                                                        -30,
                                                        56.9
                                                    ],
                                                    [
                                                        -30,
                                                        56.9
                                                    ],
                                                    [
                                                        -29.9,
                                                        57
                                                    ],
                                                    [
                                                        -29.9,
                                                        57
                                                    ]
                                                ]
                                            ],
                                            "type": "Polygon"
                                        },
                                        "properties": {},
                                        "type": "Feature"
                                    }
                                ],
                                "type": "FeatureCollection"
                            }
                        },
                        "process_id": "filter_spatial"
                    },
                    "loadcollection1": {
                        "arguments": {
                            "bands": [
                                "VV",
                                "VH"
                            ],
                            "id": "SENTINEL1_GRD",
                            "properties": {
                                # polarization will be automatically added:
                                # "polarization": {
                                #     "process_graph": {
                                #         "eq1": {
                                #             "arguments": {
                                #                 "x": {
                                #                     "from_parameter": "value"
                                #                 },
                                #                 "y": "DV"
                                #             },
                                #             "process_id": "eq",
                                #             "result": true
                                #         }
                                #     }
                                # }
                            },
                            "spatial_extent": null,
                            "temporal_extent": [
                                "2017-03-01",
                                "2017-03-15"
                            ]
                        },
                        "process_id": "load_collection"
                    },
                    "saveresult1": {
                        "arguments": {
                            "data": {
                                "from_node": "filterspatial1"
                            },
                            "format": "GTiff",
                            "options": {}
                        },
                        "process_id": "save_result",
                        "result": true
                    }
                }
            }
        }
        res = api.post("/jobs", json=job_data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet({"status": "created"})
        # Check metadata in (elastic) job tracker
        assert job_registry.db[job_id] == DictSubSet({"status": "created"})
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "created"

        # Start job
        time_machine.move_to("2020-04-20T12:02:02Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, self._mock_sentinelhub_batch_processing_service() as sh_batch_service, mock.patch(
            "kafka.KafkaProducer"
        ) as KafkaProducer:
            # Trigger job start
            api.post(f"/jobs/{job_id}/results", json={}).assert_status_code(202)

        # submit_batch_job not called yet
        submit_batch_job.assert_not_called()
        sh_batch_service.start_batch_process.assert_called_once_with(
            "sentinel-1-grd", "sentinel-1-grd", mock.ANY, mock.ANY, mock.ANY, mock.ANY, ['VV', 'VH'], mock.ANY,
            {'polarization': {'eq': 'DV'}}, mock.ANY
        )

    def test_download_large_extent(
        self,
        api,
        tmp_path,
        batch_job_output_root,
        job_registry,
        backend_implementation,
        time_machine,
        zk_client,
    ):
        time_machine.move_to("2020-04-20T12:01:01Z")

        # Create job
        data = api.get_process_graph_dict(
            process_graph=self._build_process_graph(spatial_size=1)
        )
        res = api.post("/jobs", json=data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Start job
        time_machine.move_to("2020-04-20T12:02:02Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, self._mock_sentinelhub_batch_processing_service() as sh_batch_service, mock.patch(
            "kafka.KafkaProducer"
        ) as KafkaProducer:
            api.post(f"/jobs/{job_id}/results", json={}).assert_status_code(202)

        submit_batch_job.assert_not_called()
        sh_batch_service.start_batch_process.assert_called_once_with(
            "sentinel-2-l2a", "sentinel-2-l2a", *([mock.ANY] * 8)
        )
        KafkaProducer.return_value.send.assert_called_once_with(
            topic="openeo-async-tasks", value=mock.ANY, headers=None
        )

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ],
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ],
                "process": {
                    "process_graph": {
                        "loadcollection1": DictSubSet({"process_id": "load_collection"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

        # Poll SentinelHub batch process
        time_machine.move_to("2020-04-20T12:03:03Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, self._mock_sentinelhub_batch_processing_service() as sh_batch_service:

            def get_batch_process(batch_request_id: str):
                # TODO make this more reusable
                class BatchProcess:
                    def status(self):
                        return "DONE"

                    def value_estimate(self):
                        return decimal.Decimal("12.34")

                return BatchProcess()

            sh_batch_service.get_batch_process = get_batch_process

            gps_batch_jobs: GpsBatchJobs = backend_implementation.batch_jobs
            job_info = job_registry.db[job_id]
            gps_batch_jobs.poll_job_dependencies(
                job_info=job_info, sentinel_hub_client_alias="default"
            )

        submit_batch_job.assert_called_once()
        self._check_submit_batch_job_cmd(
            submit_batch_job_mock=submit_batch_job,
            batch_job_output_root=batch_job_output_root,
            job_id=job_id,
            expected_dependencies='[{"source_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83", "card4l": false}]',
        )

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:03:03Z",
                "dependency_status": "available",
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ],
                "dependency_usage": "4.93600000000000000000000000",
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:03:03Z",
                "dependency_status": "available",
                "dependencies": [
                    {
                        "batch_request_ids": ["224635b-7d60-40f2-bae6-d30e923bcb83"],
                        "card4l": False,
                        "collection_id": "SENTINEL2_L2A_SENTINELHUB",
                        "results_location": "s3://openeo-sentinelhub/224635b-7d60-40f2-bae6-d30e923bcb83",
                    }
                ],
                "dependency_usage": "4.93600000000000000000000000",
                "process": {
                    "process_graph": {
                        "loadcollection1": DictSubSet({"process_id": "load_collection"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

        # Fake update from job tracker
        dbl_job_registry = DoubleJobRegistry(
            # TODO #236/#498/#632 phase out ZkJobRegistry
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)),
            elastic_job_registry=job_registry,
        )
        with dbl_job_registry as jr:
            jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet({"status": "running"})
        assert job_registry.db[job_id] == DictSubSet(
            {
                "status": "running",
            }
        )

        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "running"

        # Set up fake output and finish
        job_dir = batch_job_output_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        job_output = job_dir / "out"
        job_metadata = job_dir / JOB_METADATA_FILENAME

        with job_output.open("wb") as f:
            f.write(TIFF_DUMMY_DATA)
        with job_metadata.open("w") as f:
            metadata = api.load_json(JOB_METADATA_FILENAME)
            json.dump(metadata, f)

        with dbl_job_registry as jr:
            jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "finished"

        # Download
        res = api.get(f"/jobs/{job_id}/results").assert_status_code(200).json
        if api.api_version_compare.at_least("1.0.0"):
            download_url = res["assets"]["out"]["href"]
            assert "openEO_2017-11-21Z.tif" in res["assets"]
        else:
            download_url = res["links"][0]["href"]

        res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
        assert res.status_code == 200
        assert res.data == TIFF_DUMMY_DATA

    def test_download_pending_signed_job_results(
        self,
        api,
        tmp_path,
        batch_job_output_root,
        job_registry,
        backend_implementation,
        time_machine,
        zk_client,
        requests_mock,
    ):
        partial_job_results_url = "https://openeo.test/jobs/j-a778cc99-f741-4512-b304-07fdd692ae22/results/s1gn4turE?partial=true"
        requests_mock.get(partial_job_results_url, [
            {'json': {"type": "Collection", "openeo:status": "running"}},  # when starting the job = web app context
            {'json': {"type": "Collection", "openeo:status": "finished"}},  # when polling the job = async_task context
        ])

        time_machine.move_to("2020-04-20T12:01:01Z")

        # Create job
        data = api.get_process_graph_dict(
            process_graph={
                "loadstac1": {
                    "process_id": "load_stac",
                    "arguments": {
                        "url": partial_job_results_url,
                    },
                    "result": True,
                }
            }
        )
        res = api.post("/jobs", json=data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Start job
        time_machine.move_to("2020-04-20T12:02:02Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, mock.patch(
            "kafka.KafkaProducer"
        ) as KafkaProducer:
            api.post(f"/jobs/{job_id}/results", json={}).assert_status_code(202)

        submit_batch_job.assert_not_called()
        KafkaProducer.return_value.send.assert_called_once_with(
            topic="openeo-async-tasks", value=mock.ANY, headers=None
        )

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
                "process": {
                    "process_graph": {
                        "loadstac1": DictSubSet({"process_id": "load_stac"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

        # Poll SentinelHub batch process
        time_machine.move_to("2020-04-20T12:03:03Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job:
            gps_batch_jobs: GpsBatchJobs = backend_implementation.batch_jobs
            job_info = job_registry.db[job_id]
            gps_batch_jobs.poll_job_dependencies(
                job_info=job_info, sentinel_hub_client_alias="default"
            )

        submit_batch_job.assert_called_once()
        self._check_submit_batch_job_cmd(
            submit_batch_job_mock=submit_batch_job,
            batch_job_output_root=batch_job_output_root,
            job_id=job_id,
            expected_dependencies='[]',
        )

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:03:03Z",
                "dependency_status": "available",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:03:03Z",
                "dependency_status": "available",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
                "process": {
                    "process_graph": {
                        "loadstac1": DictSubSet({"process_id": "load_stac"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

        # Fake update from job tracker
        dbl_job_registry = DoubleJobRegistry(
            # TODO #236/#498/#632 phase out ZkJobRegistry
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)),
            elastic_job_registry=job_registry,
        )
        with dbl_job_registry as jr:
            jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet({"status": "running"})
        assert job_registry.db[job_id] == DictSubSet(
            {
                "status": "running",
            }
        )

        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "running"

        # Set up fake output and finish
        job_dir = batch_job_output_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        job_output = job_dir / "out"
        job_metadata = job_dir / JOB_METADATA_FILENAME

        with job_output.open("wb") as f:
            f.write(TIFF_DUMMY_DATA)
        with job_metadata.open("w") as f:
            metadata = api.load_json(JOB_METADATA_FILENAME)
            json.dump(metadata, f)

        with dbl_job_registry as jr:
            jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "finished"

        # Download
        res = api.get(f"/jobs/{job_id}/results").assert_status_code(200).json
        if api.api_version_compare.at_least("1.0.0"):
            download_url = res["assets"]["out"]["href"]
            assert "openEO_2017-11-21Z.tif" in res["assets"]
        else:
            download_url = res["links"][0]["href"]

        res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
        assert res.status_code == 200
        assert res.data == TIFF_DUMMY_DATA

    def test_download_pending_own_job_results(
        self,
        api,
        tmp_path,
        batch_job_output_root,
        job_registry,
        backend_implementation,
        time_machine,
        zk_client,
    ):
        job_registry.create_job(process={}, user_id=TEST_USER, job_id='j-a778cc99-f741-4512-b304-07fdd692ae22')
        partial_job_results_url = "https://openeo.test/jobs/j-a778cc99-f741-4512-b304-07fdd692ae22/results?partial=true"

        time_machine.move_to("2020-04-20T12:01:01Z")

        # Create job
        data = api.get_process_graph_dict(
            process_graph={
                "loadstac1": {
                    "process_id": "load_stac",
                    "arguments": {
                        "url": partial_job_results_url,
                    },
                    "result": True,
                }
            }
        )
        res = api.post("/jobs", json=data).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_id.startswith("j-")

        # Start job
        time_machine.move_to("2020-04-20T12:02:02Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job, mock.patch(
            "kafka.KafkaProducer"
        ) as KafkaProducer:
            api.post(f"/jobs/{job_id}/results", json={}).assert_status_code(202)

        submit_batch_job.assert_not_called()
        KafkaProducer.return_value.send.assert_called_once_with(
            topic="openeo-async-tasks", value=mock.ANY, headers=None
        )

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": None,
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:02:02Z",
                "dependency_status": "awaiting",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
                "process": {
                    "process_graph": {
                        "loadstac1": DictSubSet({"process_id": "load_stac"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

        # job dependency finished in the meanwhile
        job_registry.set_status(job_id='j-a778cc99-f741-4512-b304-07fdd692ae22', status=JOB_STATUS.FINISHED)

        # Poll SentinelHub batch process
        time_machine.move_to("2020-04-20T12:03:03Z")
        with self._submit_batch_job_mock(
            api=api
        ) as submit_batch_job:
            gps_batch_jobs: GpsBatchJobs = backend_implementation.batch_jobs
            job_info = job_registry.db[job_id]
            gps_batch_jobs.poll_job_dependencies(
                job_info=job_info, sentinel_hub_client_alias="default"
            )

        submit_batch_job.assert_called_once()
        self._check_submit_batch_job_cmd(
            submit_batch_job_mock=submit_batch_job,
            batch_job_output_root=batch_job_output_root,
            job_id=job_id,
            expected_dependencies='[]',
        )

        # Check status
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:03:03Z",
                "dependency_status": "available",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
            }
        )
        assert job_registry.db[job_id] == DictSubSet(
            {
                "job_id": job_id,
                "user_id": TEST_USER,
                "status": "queued",
                "application_id": "application_1587387643572_0842",
                "created": "2020-04-20T12:01:01Z",
                "updated": "2020-04-20T12:03:03Z",
                "dependency_status": "available",
                "dependencies": [
                    {
                        "partial_job_results_url": partial_job_results_url,
                    }
                ],
                "process": {
                    "process_graph": {
                        "loadstac1": DictSubSet({"process_id": "load_stac"})
                    }
                },
            }
        )
        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "queued"

        # Fake update from job tracker
        dbl_job_registry = DoubleJobRegistry(
            # TODO #236/#498/#632 phase out ZkJobRegistry
            zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)),
            elastic_job_registry=job_registry,
        )
        with dbl_job_registry as jr:
            jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)
        assert zk_client.get_json_decoded(
            f"/openeo.test/jobs/ongoing/{TEST_USER}/{job_id}"
        ) == DictSubSet({"status": "running"})
        assert job_registry.db[job_id] == DictSubSet(
            {
                "status": "running",
            }
        )

        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "running"

        # Set up fake output and finish
        job_dir = batch_job_output_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        job_output = job_dir / "out"
        job_metadata = job_dir / JOB_METADATA_FILENAME

        with job_output.open("wb") as f:
            f.write(TIFF_DUMMY_DATA)
        with job_metadata.open("w") as f:
            metadata = api.load_json(JOB_METADATA_FILENAME)
            json.dump(metadata, f)

        with dbl_job_registry as jr:
            jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

        res = api.get(f"/jobs/{job_id}").assert_status_code(200).json
        assert res["status"] == "finished"

        # Download
        res = api.get(f"/jobs/{job_id}/results").assert_status_code(200).json
        if api.api_version_compare.at_least("1.0.0"):
            download_url = res["assets"]["out"]["href"]
            assert "openEO_2017-11-21Z.tif" in res["assets"]
        else:
            download_url = res["links"][0]["href"]

        res = api.client.get(download_url, headers=TEST_USER_AUTH_HEADER)
        assert res.status_code == 200
        assert res.data == TIFF_DUMMY_DATA
