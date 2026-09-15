import contextlib
import json
import logging
import os
import pathlib
import re
import subprocess
import urllib.parse
from typing import Iterable, Union
from unittest import mock

import dirty_equals
import jsonschema
import pytest
import requests
from elasticsearch.exceptions import ConnectionTimeout
from openeo.util import deep_get
from openeo_driver.jobregistry import JOB_STATUS, JobRegistryInterface
from openeo_driver.testing import (
    TEST_USER,
    TEST_USER_AUTH_HEADER,
    TIFF_DUMMY_DATA,
    ApiResponse,
    DictSubSet,
    RegexMatcher,
)
from openeo_driver.urlsigning import UrlSigner

import openeogeotrellis.deploy.batch_job
import openeogeotrellis.job_registry
import openeogeotrellis.sentinel_hub.batchprocessing
from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.integrations.s3proxy.asset_urls import PresignedS3AssetUrls
from openeogeotrellis.integrations.s3proxy.s3_shared_context import _client_cache
from openeogeotrellis.job_registry import DoubleJobRegistry
from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.utils import to_s3_url


class TestCapabilities:

    def test_capabilities(self, api100):
        capabilities = api100.get("/").assert_status_code(200).json
        assert deep_get(capabilities, "billing", "currency") == "credits"


    def test_file_formats(self, api100):
        formats = api100.get("/file_formats").assert_status_code(200).json
        formats_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://processes.openeo.org/2.0.0-rc.1/meta/subtype-schemas.json",
            "type": "object",
            "properties": formats["output"],
        }
        jsonschema.validate(instance=formats_schema, schema={"$ref": "http://json-schema.org/draft-07/schema#"})
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
        assert deep_get(capabilities, "_backend_deploy_metadata") == dirty_equals.IsPartialDict(
            {
                "date": RegexMatcher(r"\d{4}-\d{2}-\d{2}.*Z$"),
                "versions": {
                    "openeo": semver_alike,
                    "openeo_driver": semver_alike,
                    "openeo-geopyspark": semver_alike,
                    "geopyspark-openeo": semver_alike,
                    "geotrellis-extensions": dirty_equals.IsAnyStr,
                },
            }
        )
        assert deep_get(capabilities, "processing:software") == {
            "openeo": semver_alike,
            "openeo_driver": semver_alike,
            "openeo-geopyspark": semver_alike,
            "geopyspark-openeo": semver_alike,
            "geotrellis-extensions": dirty_equals.IsAnyStr,
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

    def test_udf_runtimes(self, api100):
        udf_runtimes = api100.get("/udf_runtimes").assert_status_code(200).json
        assert udf_runtimes == {
            "Python": {
                "title": "Python",
                "type": "language",
                "default": "3",
                "versions": {
                    "3.8": {"libraries": {"numpy": {"version": "1.22.4"}, "pandas": {"version": "1.5.3"}}},
                    "3.11": {"libraries": {"numpy": {"version": "2.3.3"}, "pandas": {"version": "2.3.3"}}},
                    "3": {"libraries": {"numpy": {"version": "2.3.3"}, "pandas": {"version": "2.3.3"}}},
                },
            },
            "Python-Jep": {
                "default": "3.8",
                "title": "Python-Jep",
                "type": "language",
                "versions": {
                    "3.8": {"libraries": {"numpy": {"version": "1.22.4"}, "pandas": {"version": "1.5.3"}}},
                },
            },
        }


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
            assert resp['stac_version'] == "1.0.0"
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


def _upload_job_assets_to_s3(
    *,
    mock_s3_client,
    bucket: str = "OpenEO-data",
    paths: Iterable[pathlib.Path],
):
    """Helper to upload all job assets to mocked s3 instance"""
    mock_s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-central-1"})
    for path in paths:
        key = (str(path)).lstrip("/")
        mock_s3_client.put_object(Bucket=bucket, Key=key, Body=path.read_bytes())


@pytest.fixture(scope="function")
def disable_s3_client_cache():
    """
    Fixture for tests that would have different endpoint configurations for the same bucket disable s3 client cache.
    To be safe clean before AND after the test.
    """
    _client_cache.flush()
    yield
    _client_cache.flush()


class TestBatchJobs:

    # TODO: use YarnMocker/yarn_mocker fixture here to eliminate subprocess.run mocking boilerplate?

    DUMMY_PROCESS_GRAPH = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "BIOPAR_FAPAR_V1_GLOBAL"
            },
            "result": True
        }
    }

    DUMMY_PROCESS_GRAPH_WITH_UDF = {
        "loadcollection1": {
            "arguments": {
                "id": "BIOPAR_FAPAR_V1_GLOBAL",
                "callback": {
                    "process_graph": {
                        "deep_udf": {
                            "arguments": {
                                "udf": "some code",
                                "runtime": "python",
                                "version": "3.11"
                            },
                            "process_id": "run_udf",
                            "result": True
                        }
                    }
                },

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
                    "from_node": "loadcollection1"
                },
                "format": "GTiff",
                "options": {}
            },
            "process_id": "save_result",
            "result": True
        }
    }

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
        result = api.get('/jobs', headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
        assert result == {"jobs": [], "links": []}

    def test_create_job(self, api, job_registry, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")
        data = api.get_process_graph_dict(
            self.DUMMY_PROCESS_GRAPH, title="Dummy", description="Dummy job!"
        )
        res = api.post(
            "/jobs", json=data, headers=TEST_USER_AUTH_HEADER
        ).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]
        assert job_registry.db[job_id]["job_id"] == job_id
        assert job_registry.db[job_id]["user_id"] == TEST_USER
        assert job_registry.db[job_id]["status"] == "created"
        assert job_registry.db[job_id]["api_version"] == api.api_version
        assert job_registry.db[job_id]["application_id"] == None
        assert job_registry.db[job_id]["created"] == "2020-04-20T16:04:03Z"
        assert job_registry.db[job_id]["title"] == "Dummy"
        assert job_registry.db[job_id]["description"] == "Dummy job!"

    def test_create_and_get(self, api, time_machine):
        time_machine.move_to("2020-04-20T16:04:03Z")
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
        self,
        mock_search,
        api,
        tmp_path,
        monkeypatch,
        batch_job_output_root,
        job_registry,
        time_machine,
        mock_yarn_backend_config,
    ):
        time_machine.move_to("2020-04-20T16:04:03Z")

        with mock.patch.dict("os.environ", {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/custom_processes.py,data/deps/foolib.whl"}):

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
            assert batch_job_args[9] == api.api_version

            assert batch_job_args[10:16] == ['8G', '2G', '3G', '5', '2', '2G']
            assert batch_job_args[16:21] == [
                'default', 'false', '[]',
                "__pyfiles__/custom_processes.py,foolib.whl", '100'
            ]
            assert batch_job_args[21:23] == [TEST_USER, job_id]
            assert batch_job_args[23] == '0.1'

            assert job_registry.db[job_id]["job_id"] == job_id
            assert job_registry.db[job_id]["user_id"] == TEST_USER
            assert job_registry.db[job_id]["status"] == "queued"
            assert job_registry.db[job_id]["application_id"] == 'application_1587387643572_0842'
            assert job_registry.db[job_id]["created"] == "2020-04-20T16:04:03Z"
            res = api.get('/jobs/{j}'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
            assert res["status"] == "queued"

            # Get logs
            res = api.get(
                '/jobs/{j}/logs'.format(j=job_id), headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(200).json
            assert res["logs"] == []

            # Fake update from job tracker
            dbl_job_registry = DoubleJobRegistry(
                elastic_job_registry=job_registry,
            )
            with dbl_job_registry as jr:
                jr.set_status(job_id=job_id, user_id=TEST_USER, status=JOB_STATUS.RUNNING)

            assert job_registry.db[job_id]["status"] == "running"
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

    @pytest.mark.parametrize(
        ["freeipa_response", "expected_proxy_user"],
        [
            ((200, []), ""),
            ((200, [{"uid": TEST_USER}]), TEST_USER),
            ((500, []), ""),
        ],
    )
    def test_create_and_start_proxy_user(
        self,
        api,
        tmp_path,
        batch_job_output_root,
        job_registry,
        requests_mock,
        freeipa_response,
        expected_proxy_user,
        mock_yarn_backend_config,
    ):
        def freeipa_user_find_handler(request, context):
            request_data = request.json()
            assert request_data.get("method") == "user_find"
            status_code, result = freeipa_response
            context.status_code = status_code
            return {
                "id": request_data["id"],
                "result": {"result": result},
            }

        requests_mock.post("https://freeipa.test/ipa/json", json=freeipa_user_find_handler)

        # Create job
        data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
        res = api.post("/jobs", json=data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
        job_id = res.headers["OpenEO-Identifier"]

        # Start job
        with mock.patch("subprocess.run") as run:
            os.mkdir(batch_job_output_root / job_id)
            stdout = api.read_file("spark-submit-stdout.txt")
            run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
            # Trigger job start
            api.post(f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER).assert_status_code(202)
        run.assert_called_once()
        batch_job_args = run.call_args[0][0]

        assert batch_job_args[8] == expected_proxy_user

    @pytest.mark.parametrize("api", ["api100", "api110"])
    def test_results_metadata(
        self,
        api,
        tmp_path,
        monkeypatch,
        batch_job_output_root,
        job_registry,
        time_machine,
        request,
        mock_yarn_backend_config,
    ):
        api = request.getfixturevalue(api)
        time_machine.move_to("2020-04-20T16:04:03Z")

        with mock.patch.dict(
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

            if api.api_version_compare.at_least("1.1.0"):
                assert res["extent"]["spatial"]["bbox"] == [[2, 51, 3, 52]]

    @mock.patch(
        "openeogeotrellis.configparams.ConfigParams.use_object_storage",
        new_callable=mock.PropertyMock,
    )
    def test_download_from_object_storage(
        self, mock_config_use_object_storage, api, batch_job_output_root, mock_s3_bucket, job_registry
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

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
        self,
        mock_config_use_object_storage,
        moto_server,
        batch_job_output_root,
        mock_s3_bucket,
        mock_sts_client,
        sts_endpoint_on_driver,
        api,
        job_registry,
        config_overrides,
        idp_enabled,
        auth_header: bool,
        expected_code: int,
        disable_s3_client_cache,
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

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
            # pre-signed urls don't work with flask retriever
            def retrieve_url_and_set_data(*args, **kwargs):
                result = requests.get(*args, **kwargs)
                setattr(result, "data", result.text.encode("utf-8"))
                return result
            retrieve_url = retrieve_url_and_set_data
            # Proxy should allow Head requests which requires extra header.
            assert "X-Proxy-Head-As-Get=true" in download_url

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
        self, mock_config_use_object_storage, api, batch_job_output_root, job_registry
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

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

    def test_yarn_mode_create_and_start_job_options(
        self, api, tmp_path, monkeypatch, batch_job_output_root, time_machine, mock_yarn_backend_config
    ):
        time_machine.move_to("2020-04-20T16:04:03Z")

        with mock.patch.dict("os.environ", {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/custom_processes.py,data/deps/foolib.whl"}):

            openeo_flask_dir = tmp_path / "openeo-flask"
            openeo_flask_dir.mkdir()
            (openeo_flask_dir / "foolib.whl").touch()
            (openeo_flask_dir / "__pyfiles__").mkdir()
            (openeo_flask_dir / "__pyfiles__" / "custom_processes.py").touch()
            monkeypatch.chdir(openeo_flask_dir)

            # Create job
            data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH, title="Dummy")
            data["job_options"] = {"driver-memory": "3g", "executor-memory": "11g","executor-cores":"4","queue":"somequeue","driver-memoryOverhead":"10G", "soft-errors":"false", "udf-dependency-archives":["https://host.com/my.jar"]}
            batch_job_args, job_id, env = self._create_and_start_job_yarn_mode(data, api)

            # Check batch in/out files
            job_dir = batch_job_output_root / job_id
            job_output = job_dir / "out"
            job_metadata = job_dir / JOB_METADATA_FILENAME
            assert batch_job_args[2].endswith(".in")
            assert batch_job_args[3] == str(job_dir)
            assert batch_job_args[4] == job_output.name
            assert batch_job_args[5] == job_metadata.name
            assert batch_job_args[9] == api.api_version
            assert batch_job_args[10:16] == ['3g', '11g', '3G', '5', '4', '10G']
            assert batch_job_args[16:21] == [
                'somequeue', 'false', '[]',
                '__pyfiles__/custom_processes.py,foolib.whl', '100'
            ]
            assert batch_job_args[21:23] == [TEST_USER, job_id]
            assert batch_job_args[23] == '0.0'
            assert batch_job_args[27] == 'https://host.com/my.jar'

    def _create_and_start_job_yarn_mode(self, job_data, api):
        res = api.post('/jobs', json=job_data, headers=TEST_USER_AUTH_HEADER).assert_status_code(201)
        job_id = res.headers['OpenEO-Identifier']
        # Start job
        # TODO: reuse `yarn_mocker/YarnMocker` here?
        with mock.patch('subprocess.run') as run:
            stdout = api.read_file("spark-submit-stdout.txt")
            run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
            # Trigger job start
            api.post(
                f"/jobs/{job_id}/results", json={}, headers=TEST_USER_AUTH_HEADER
            ).assert_status_code(202)
            run.assert_called_once()
            batch_job_args = run.call_args[0][0]
            env = run.call_args[1]['env']
        return batch_job_args, job_id,env

    def test_yarn_mode_start_custom_udf_runtime(
        self, api, job_registry, time_machine, batch_job_output_root, mock_yarn_backend_config
    ):
        time_machine.move_to("2020-04-20T12:01:01Z")

        job_data = api.get_process_graph_dict(self.DUMMY_PROCESS_GRAPH_WITH_UDF, title="Dummy")
        batch_job_args, job_id, env = self._create_and_start_job_yarn_mode(job_data, api)

        # Check batch in/out files
        job_dir = batch_job_output_root / job_id
        job_output = job_dir / "out"
        job_metadata = job_dir / JOB_METADATA_FILENAME
        assert batch_job_args[2].endswith(".in")
        assert batch_job_args[3] == str(job_dir)
        assert batch_job_args[4] == job_output.name
        assert batch_job_args[5] == job_metadata.name
        assert env["YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"] == "docker.test/openeo-geopy311:7.9.11"

    @pytest.mark.parametrize(
        ["args", "expected"],
        [
            ({"runtime": "python"}, "docker.test/openeo-geopy311:7.9.11"),
            ({"runtime": "Python"}, "docker.test/openeo-geopy311:7.9.11"),
            ({"runtime": "Python", "version": "3.8"}, "docker.test/openeo-geopy38:3.5.8"),
            ({"runtime": "Python", "version": "3.11"}, "docker.test/openeo-geopy311:7.9.11"),
            ({"runtime": "Python", "version": "3"}, "docker.test/openeo-geopy311:7.9.11"),
        ],
    )
    def test_yarn_mode_start_job_udf_runtime_image_handling(
        self, api, job_registry, batch_job_output_root, mock_yarn_backend_config, args, expected
    ):
        pg = {
            "lc": {
                "process_id": "load_collection",
                "arguments": {"id": "TERRASCOPE_S2_TOC_V2"},
            },
            "apply": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "lc"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {"data": [1, 2, 3], "udf": "print('hello')", **args},
                                "result": True,
                            }
                        }
                    },
                },
                "result": True,
            },
        }
        job_data = api.get_process_graph_dict(pg)
        batch_job_args, job_id, env = self._create_and_start_job_yarn_mode(job_data, api)
        assert env["YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"] == expected

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

    def test_cancel_job(self, api, job_registry, mock_yarn_backend_config):
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
        assert job_registry.db[job_id] == DictSubSet({"status": "canceled"})

    def test_delete_job(self, api, job_registry, mock_yarn_backend_config):
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
        assert job_registry.db[job_id] == DictSubSet({"status": "canceled"})

        # Delete
        res = api.delete(f"/jobs/{job_id}", headers=TEST_USER_AUTH_HEADER)
        res.assert_status_code(204)
        # TODO
        # assert job_registry.db[job_id] == DictSubSet({"deleted": True})

    @mock.patch("openeogeotrellis.logs.Elasticsearch.search")
    def test_get_job_logs_skips_lines_with_empty_loglevel(self, mock_search, api, job_registry):
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

        # Get logs
        res = (
            api.get(f"/jobs/{job_id}/logs", headers=TEST_USER_AUTH_HEADER)
            .assert_status_code(200)
            .json
        )

        assert res["logs"] == expected_log_entries

    @mock.patch("openeogeotrellis.logs.Elasticsearch.search")
    def test_get_job_logs_with_connection_timeout(self, mock_search, api, caplog, job_registry):
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

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

    def test_api_job_results_contains_proj_metadata_at_asset_level(self, api, batch_job_output_root, job_registry):
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

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
    def test_api_job_results_contains_proj_metadata_at_item_level(self, api100, batch_job_output_root, job_registry):
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

        job_registry.create_job(
            job_id=job_id,
            user_id=TEST_USER,
            api_version="1.0.0",
            process={"process_graph": self.DUMMY_PROCESS_GRAPH},
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

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


    @pytest.fixture
    def load_stac_dummy_collection_123(self, dummy_stac_api) -> dict:
        """Process graph to do simple load_stac of `collection-123` from the dummy STAC API server"""
        return {
            "loadstac1": {
                "process_id": "load_stac",
                "arguments": {"url": f"{dummy_stac_api}/collections/collection-123"},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "loadstac1"},
                    "format": "GTiff",
                },
                "result": True,
            },
        }

    def _run_job(
        self,
        *,
        process_graph: dict,
        batch_job_work_dir_root: pathlib.Path,
        job_registry: JobRegistryInterface,
        job_id: str = "job-123",
        stac_version="1.1",
    ) -> str:
        """
        Helper to:
        - do real processing of a given process graph using openeogeotrellis.deploy.batch_job.run_job
        - set up related batch job metadata in the given job registry
        """
        job_specification = {
            "process_graph": process_graph,
            "job_options": {"stac-version": stac_version},
        }
        job_dir = batch_job_work_dir_root / job_id
        metadata_path = job_dir / JOB_METADATA_FILENAME
        job_registry.create_job(job_id=job_id, process=process_graph, user_id=TEST_USER)
        openeogeotrellis.deploy.batch_job.run_job(
            job_specification=job_specification,
            output_file=job_dir / "out",
            metadata_file=metadata_path,
            job_dir=job_dir,
        )
        job_registry.set_status(job_id=job_id, status=JOB_STATUS.FINISHED)

        return job_id

    def test_get_job_results_metadata_basic(
        self,
        api110,
        tmp_path,
        batch_job_output_root,
        job_registry,
        load_stac_dummy_collection_123,
    ):
        job_id = self._run_job(
            process_graph=load_stac_dummy_collection_123,
            batch_job_work_dir_root=batch_job_output_root,
            job_registry=job_registry,
        )

        # Get the job results and verify the contents.
        res = api110.get(f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

        assert res == dirty_equals.IsPartialDict(
            {
                "id": job_id,
                "type": "Collection",
                "stac_version": "1.1.0",
                "openeo:status": "finished",
            }
        )

    @pytest.mark.parametrize(
        [
            "config_overrides",
            "expected_derived_from_href_on_disk",
            "expected_s3_bucket",
            "expected_derived_from_href",
            "get_with_auth",
        ],
        [
            (
                {},
                dirty_equals.IsStr(regex="file:///.*/job-123/stac-item-collection-loadstac1.json"),
                None,
                "http://oeo.net/openeo/1.1.0/jobs/job-123/results/aux/stac-item-collection-loadstac1.json",
                True,
            ),
            (
                {"url_signer": UrlSigner(secret="Secret!")},
                dirty_equals.IsStr(regex="file:///.*/job-123/stac-item-collection-loadstac1.json"),
                None,
                dirty_equals.IsStr(
                    regex=r"http://oeo\.net/openeo/1\.1\.0/jobs/job-123/results/aux/\w+=*/[0-9a-f]+/stac-item-collection-loadstac1\.json"
                ),
                False,
            ),
            (
                {"url_signer": UrlSigner(secret="Secret!"), "job_local_href_format": "s3"},
                dirty_equals.IsStr(regex="s3://OpenEO-data/.*/job-123/stac-item-collection-loadstac1.json"),
                "OpenEO-data",
                dirty_equals.IsStr(
                    # TODO: note that the URL building is badly aligned here:
                    #       `/aux/` would be expected, but it's `/assets/` at the moment
                    regex=r"http://oeo\.net/openeo/1\.1\.0/jobs/job-123/results/assets/\w+=*/[0-9a-f]+/stac-item-collection-loadstac1\.json"
                ),
                False,
            ),
        ],
    )
    def test_get_job_results_metadata_derived_from_item_collection(
        self,
        api110,
        tmp_path,
        batch_job_output_root,
        job_registry,
        load_stac_dummy_collection_123,
        expected_derived_from_href_on_disk,
        expected_derived_from_href,
        get_with_auth: bool,
        mock_s3_client,
        expected_s3_bucket: Union[str, None],
    ):
        job_id = self._run_job(
            process_graph=load_stac_dummy_collection_123,
            batch_job_work_dir_root=batch_job_output_root,
            job_registry=job_registry,
        )

        if expected_s3_bucket:
            _upload_job_assets_to_s3(
                mock_s3_client=mock_s3_client,
                bucket=expected_s3_bucket,
                paths=(batch_job_output_root / job_id).rglob("*.json"),
            )

        # Check hrefs on disk
        with (batch_job_output_root / job_id / JOB_METADATA_FILENAME).open() as f:
            metadata_on_disk = json.load(f)
        links = metadata_on_disk.get("links", [])
        derived_from_links = [k for k in links if k.get("rel") == "derived_from"]
        assert len(derived_from_links) == 1
        derived_from_href = derived_from_links[0].get("href")
        assert derived_from_href == expected_derived_from_href_on_disk

        res = api110.get(f"/jobs/{job_id}/results", headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json
        assert res == dirty_equals.IsPartialDict(
            {"id": job_id, "openeo:status": "finished", "stac_version": "1.1.0", "type": "Collection"}
        )
        links = res.get("links", [])
        derived_from_links = [k for k in links if k.get("rel") == "derived_from"]
        assert len(derived_from_links) == 1
        derived_from_href = derived_from_links[0].get("href")
        assert derived_from_href == expected_derived_from_href
        # TODO: this href parsing/getting should be supported in ApiTester.get directly
        parsed = urllib.parse.urlparse(derived_from_href)
        headers = TEST_USER_AUTH_HEADER if get_with_auth else {}
        derived_from_data = (
            ApiResponse(api110.client.get(parsed.path, headers=headers)).assert_http_status_code(200).json
        )

        assert derived_from_data == dirty_equals.IsPartialDict(
            {
                "type": "FeatureCollection",
                "features": [
                    dirty_equals.IsPartialDict(id="item-1"),
                    dirty_equals.IsPartialDict(id="item-2"),
                    dirty_equals.IsPartialDict(id="item-3"),
                ],
            }
        )
