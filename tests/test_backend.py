import logging

import os
from typing import Union

import dirty_equals
import mock
import pytest
import shapely
from openeo.utils.version import ComparableVersion
from openeo_driver.config.load import ConfigGetter
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.processes import ProcessRegistry
from openeo_driver.ProcessGraphDeserializer import ENV_SOURCE_CONSTRAINTS
from openeo_driver.specs import read_spec
from openeo_driver.users import User
from openeo_driver.utils import EvalEnv

from openeogeotrellis.backend import (
    GpsBatchJobs,
    GpsProcessing,
)
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.s3_config import S3Config
from openeogeotrellis.integrations.kubernetes import k8s_render_manifest_template
from openeogeotrellis.integrations.yarn_jobrunner import YARNBatchJobRunner
from openeogeotrellis.testing import gps_config_overrides


def test_extract_application_id():
    yarn_log = """
19/07/10 15:56:39 WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
19/07/10 15:56:39 INFO Client: Attempting to login to the Kerberos using principal: jenkins@VGT.VITO.BE and keytab: jenkins.keytab-2322e03c-bf97-4f59-b9ad-7c2ecb2d1c70
19/07/10 15:56:39 INFO RequestHedgingRMFailoverProxyProvider: Created wrapped proxy for [rm1, rm2]
19/07/10 15:56:39 INFO RequestHedgingRMFailoverProxyProvider: Looking for the active RM in [rm1, rm2]...
19/07/10 15:56:39 INFO RequestHedgingRMFailoverProxyProvider: Found active RM [rm2]
19/07/10 15:56:39 INFO Client: Requesting a new application from cluster with 99 NodeManagers
19/07/10 15:56:39 INFO Configuration: resource-types.xml not found
19/07/10 15:56:39 INFO ResourceUtils: Unable to find 'resource-types.xml'.
19/07/10 15:56:39 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (55296 MB per container)
19/07/10 15:56:39 INFO Client: Will allocate AM container, with 1408 MB memory including 384 MB overhead
19/07/10 15:56:39 INFO Client: Setting up container launch context for our AM
19/07/10 15:56:39 INFO Client: Setting up the launch environment for our AM container
19/07/10 15:56:39 INFO Client: Credentials file set to: credentials-4bfb4d79-eb95-4578-bd0a-cbfa2bf7d298
19/07/10 15:56:39 INFO Client: Preparing resources for our AM container
19/07/10 15:56:39 INFO HadoopFSDelegationTokenProvider: getting token for: DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_885268276_1, ugi=jenkins@VGT.VITO.BE (auth:KERBEROS)]]
19/07/10 15:56:39 INFO DFSClient: Created token for jenkins: HDFS_DELEGATION_TOKEN owner=jenkins@VGT.VITO.BE, renewer=yarn, realUser=, issueDate=1562766999634, maxDate=1563371799634, sequenceNumber=1296276, masterKeyId=1269 on ha-hdfs:hacluster
19/07/10 15:56:39 INFO HadoopFSDelegationTokenProvider: getting token for: DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_885268276_1, ugi=jenkins@VGT.VITO.BE (auth:KERBEROS)]]
19/07/10 15:56:39 INFO DFSClient: Created token for jenkins: HDFS_DELEGATION_TOKEN owner=jenkins@VGT.VITO.BE, renewer=jenkins, realUser=, issueDate=1562766999721, maxDate=1563371799721, sequenceNumber=1296277, masterKeyId=1269 on ha-hdfs:hacluster
19/07/10 15:56:39 INFO HadoopFSDelegationTokenProvider: Renewal interval is 86400059 for token HDFS_DELEGATION_TOKEN
19/07/10 15:56:40 INFO Client: To enable the AM to login from keytab, credentials are being copied over to the AM via the YARN Secure Distributed Cache.
19/07/10 15:56:40 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/container_e3344_1562328661428_5538_01_000001/jenkins.keytab-2322e03c-bf97-4f59-b9ad-7c2ecb2d1c70 -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/jenkins.keytab-2322e03c-bf97-4f59-b9ad-7c2ecb2d1c70
19/07/10 15:56:41 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
19/07/10 15:56:46 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/spark-ad3a2402-36d5-407a-8b30-392033d45899/__spark_libs__4608991107087829959.zip -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/__spark_libs__4608991107087829959.zip
19/07/10 15:56:51 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/container_e3344_1562328661428_5538_01_000001/geotrellis-extensions-1.3.0-SNAPSHOT.jar -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/geotrellis-extensions-1.3.0-SNAPSHOT.jar
19/07/10 15:56:52 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/container_e3344_1562328661428_5538_01_000001/geotrellis-backend-assembly-0.4.6-openeo.jar -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/geotrellis-backend-assembly-0.4.6-openeo.jar
19/07/10 15:56:54 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/container_e3344_1562328661428_5538_01_000001/layercatalog.json -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/layercatalog.json
19/07/10 15:56:54 INFO Client: Uploading resource file:/mnt/ceph/Projects/OpenEO/f5ddcb98-a9ca-440e-a705-da6d71aaab44/in -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/in
19/07/10 15:56:54 INFO Client: Uploading resource https://artifactory.vgt.vito.be/artifactory/auxdata-public/openeo/venv.zip#venv -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/venv.zip
19/07/10 15:57:01 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/container_e3344_1562328661428_5538_01_000001/venv/lib64/python3.5/site-packages/openeogeotrellis/deploy/batch_job.py -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/batch_job.py
19/07/10 15:57:01 INFO Client: Uploading resource file:/usr/hdp/3.0.0.0-1634/spark2/python/lib/pyspark.zip -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/pyspark.zip
19/07/10 15:57:01 INFO Client: Uploading resource file:/usr/hdp/3.0.0.0-1634/spark2/python/lib/py4j-0.10.7-src.zip -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/py4j-0.10.7-src.zip
19/07/10 15:57:02 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/spark-ad3a2402-36d5-407a-8b30-392033d45899/__spark_conf__2177799938793019578.zip -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/__spark_conf__.zip
19/07/10 15:57:02 INFO SecurityManager: Changing view acls to: jenkins
19/07/10 15:57:02 INFO SecurityManager: Changing modify acls to: jenkins
19/07/10 15:57:02 INFO SecurityManager: Changing view acls groups to:
19/07/10 15:57:02 INFO SecurityManager: Changing modify acls groups to:
19/07/10 15:57:02 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(jenkins); groups with view permissions: Set(); users  with modify permissions: Set(jenkins); groups with modify permissions: Set()
19/07/10 15:57:02 INFO Client: Submitting application application_1562328661428_5542 to ResourceManager
19/07/10 15:57:02 INFO YarnClientImpl: Submitted application application_1562328661428_5542
19/07/10 15:57:03 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:03 INFO Client:
	 client token: Token { kind: YARN_CLIENT_TOKEN, service:  }
	 diagnostics: AM container is launched, waiting for AM container to Register with RM
	 ApplicationMaster host: N/A
	 ApplicationMaster RPC port: -1
	 queue: default
	 start time: 1562767022250
	 final status: UNDEFINED
	 tracking URL: http://epod17.vgt.vito.be:8088/proxy/application_1562328661428_5542/
	 user: jenkins
19/07/10 15:57:04 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:05 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:06 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:07 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:08 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:13 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:59 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:58:00 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:58:01 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:58:02 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:02 INFO Client:
	 client token: Token { kind: YARN_CLIENT_TOKEN, service:  }
	 diagnostics: N/A
	 ApplicationMaster host: 192.168.207.182
	 ApplicationMaster RPC port: 0
	 queue: default
	 start time: 1562767022250
	 final status: UNDEFINED
	 tracking URL: http://epod17.vgt.vito.be:8088/proxy/application_1562328661428_5542/
	 user: jenkins
19/07/10 15:58:03 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:04 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:05 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:06 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:07 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:08 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:09 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:10 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:11 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
    """
    assert YARNBatchJobRunner._extract_application_id(yarn_log) == "application_1562328661428_5542"


def test_get_submit_py_files_basic(tmp_path, caplog):
    (tmp_path / "lib.whl").touch()
    (tmp_path / "zop.zip").touch()
    (tmp_path / "__pyfiles__").mkdir()
    (tmp_path / "__pyfiles__" / "stuff.py").touch()
    env = {"OPENEO_SPARK_SUBMIT_PY_FILES": "stuff.py,lib.whl,foo.py"}
    py_files = YARNBatchJobRunner.get_submit_py_files(env=env, cwd=tmp_path, log = logging.getLogger("openeogeotrellis"))
    assert py_files == "__pyfiles__/stuff.py,lib.whl"
    warn_logs = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert warn_logs == ["Could not find 'py-file' foo.py: skipping"]


def test_get_submit_py_files_deep_paths(tmp_path, caplog):
    # Originally submitted py-files
    env = {"OPENEO_SPARK_SUBMIT_PY_FILES": "data/deps/stuff.py,data/deps/lib.whl"}
    # Resources of flask app job.
    (tmp_path / "lib.whl").touch()
    (tmp_path / "__pyfiles__").mkdir()
    (tmp_path / "__pyfiles__" / "stuff.py").touch()
    py_files = YARNBatchJobRunner.get_submit_py_files(env=env, cwd=tmp_path)
    assert py_files == "__pyfiles__/stuff.py,lib.whl"
    warn_logs = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert warn_logs == []


def test_get_submit_py_files_no_env(tmp_path):
    py_files = YARNBatchJobRunner.get_submit_py_files(env={}, cwd=tmp_path)
    assert py_files == ""


def test_get_submit_py_files_empty(tmp_path):
    env = {"OPENEO_SPARK_SUBMIT_PY_FILES": ""}
    py_files = YARNBatchJobRunner.get_submit_py_files(env=env, cwd=tmp_path)
    assert py_files == ""


def test_extra_validation_layer_too_large_drivervectorcube(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    source_id2 = "load_collection", ("COPERNICUS_30", None)
    polygon = {"type": "Polygon", "coordinates": [[(0, 0), (180, 0), (0, 90), (180, 90)]]}
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2023-01-09"],
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929, "crs": "EPSG:32632"},
            "bands": ["HH", "HV", "VV"],
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0},
            "bands": ["DEM"],
            "aggregate_spatial": {
                "geometries": DriverVectorCube.from_geojson(polygon),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 2
    assert errors[0]['code'] == "ExtentTooLarge"
    assert errors[1]['code'] == "ExtentTooLarge"


def test_extra_validation_layer_too_large_open_time_interval(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": [None, None],  # Will go from 2014 till current time
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929,
                               "crs": "EPSG:32632"},
            "bands": ["VV"],
        })
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]['code'] == "ExtentTooLarge"

def test_extra_validation_layer_too_large_copernicus_30(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("COPERNICUS_30", None)
    env_source_constraints = [
        (source_id1, {
            # taken from user example in 'process_graph_list_mep.jsonl'
            # Specifying a temporal extent is unneeded here, and caused the old extra_validation to fail.
            "temporal_extent": ["2010-01-01", "2030-12-31"],
            "spatial_extent": {"east": 1941516.7822, "south": -637292.4712999999, "crs": "EPSG:32637",
                               "north": 2493707.5287, "west": -1285483.2178},
            "bands": ["DEM"],
        })
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert errors == []


def test_extra_validation_layer_fail(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("!!BOGUS_LAYER!!", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": None,
            "spatial_extent": None,
        })
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]['code'] == "Internal"


def test_extra_validation_without_extent(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("ESA_WORLDCOVER_10M_2021_V2", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": None,
            "spatial_extent": {"east": 1941516.7822, "south": -637292.4712999999, "crs": "EPSG:32637",
                               "north": 2493707.5287, "west": 0},
            "bands": ["MAP"],
        })
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert errors == []


def test_extra_validation_layer_too_large_area(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2022-01-01", "2022-01-01"],
            "spatial_extent": {"south": -952987.7582, "west": 1495130.8875, "north": 910166.7419, "east": 9088482.3929,
                               "crs": "EPSG:32632"},
            "bands": ["VV"],
        })
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "sync_job": True,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]["code"] == "ExtentTooLarge"
    assert "spatial extent" in errors[0]["message"].lower()


def test_extra_validation_layer_timezone(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2022-01-01T00:00:00Z", "2022-01-09"],
            "spatial_extent": {"south": -952987.7582, "west": 1495130.8875, "north": 910166.7419, "east": 9088482.3929,
                               "crs": "EPSG:32632"},
            "bands": ["VV"],
        })
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "sync_job": True,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]["code"] == "ExtentTooLarge"
    assert "spatial extent" in errors[0]["message"].lower()


def test_extra_validation_layer_too_large_delayedvector(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    source_id2 = "load_collection", ("COPERNICUS_30", None)
    polygon1 = {"type": "Polygon", "coordinates": [[(0.0, 0.0), (0.05, 0.0), (0.0, 0.05), (0.05, 0.05)]]}
    polygon2 = {"type": "Polygon", "coordinates": [[(0.0, 0.0), (90.0, 0.0), (0.0, 180.0), (90.0, 180.0)]]}
    geom_coll = {"type": "GeometryCollection", "geometries": [polygon1, polygon2]}
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0},
            "bands": ["HH", "HV", "VV"],
            "resample": {"target_crs": "EPSG:4326"},
            "aggregate_spatial": {
                "geometries": DelayedVector.from_json_dict(polygon1),
            },
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0},
            "bands": ["DEM"],
            "resample": {"target_crs": "EPSG:4326"},
            "aggregate_spatial": {
                "geometries": DelayedVector.from_json_dict(geom_coll),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]['code'] == "ExtentTooLarge"


def test_extra_validation_layer_too_large_geometrycollection(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    source_id2 = "load_collection", ("COPERNICUS_30", None)
    polygon1 = shapely.geometry.Polygon([(0, 0), (0.2, 0), (0, 0.2), (0.2, 0.2)])
    polygon2 = shapely.geometry.Polygon([(0, 0), (90, 0), (0, 180), (90, 180)])
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0},
            "bands": ["HH", "HV", "VV"],
            "resample": {"target_crs": "EPSG:4326"},
            "aggregate_spatial": {
                "geometries": shapely.geometry.MultiPolygon([polygon1]),
            },
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0},
            "bands": ["DEM"],
            "resample": { "target_crs": "EPSG:4326" },
            "aggregate_spatial": {
                "geometries": shapely.geometry.GeometryCollection([polygon1, polygon2]),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]['code'] == "ExtentTooLarge"


def test_extra_validation_layer_too_large_custom_crs(backend_implementation):
    # The user can specify their own CRS in load_collection.
    # Here: The native crs of AGERA5 is LatLon but the user specifies a spatial_extent in EPSG:3035.
    processing = GpsProcessing()
    source_id1 = "load_collection", ("AGERA5", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 5000000.0, "west": 420000.0, "north": 5110000.0, "east": 430000.0, "crs": "EPSG:3035"},
            "bands": ["wind-speed"],
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 0


def test_extra_validation_layer_too_large_custom_crs_hourly(backend_implementation):
    processing = GpsProcessing()
    source_id1 = "load_collection", ("AGERA5_HOURLY", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 5000000.0, "west": 420000.0, "north": 5110000.0, "east": 430000.0,
                               "crs": "EPSG:3035"},
            "bands": ["wind-speed"],
        }),
    ]
    env = EvalEnv(
        values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation,
                "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 0


def test_extra_validation_missing_gsd(backend_implementation):
    # Layers with missing GSD should not crash
    processing = GpsProcessing()
    source_id1 = "load_collection", ("TERRASCOPE_S1_SLC_COHERENCE_V1", None)
    polygon = {"type": "Polygon", "coordinates": [[(0, 0), (180.0, 0), (0, 90.0), (180.0, 90.0)]]}
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0, 'crs': 'EPSG:4326'},
            "bands": ["VV", "VH"],
            "aggregate_spatial": {
                "geometries": DelayedVector.from_json_dict(polygon),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    processing.extra_validation({}, env, None, env_source_constraints)


def test_extra_validation_layer_too_large_resample_spatial(backend_implementation):
    # When resample_spatial or resample_cube_spatial is used, the resolution and crs of the layer is changed.
    # So that needs to be taken into account when estimating the number of pixels.
    processing = GpsProcessing()
    source_id1 = "load_collection", ("SENTINEL1_GAMMA0_SENTINELHUB", None)
    source_id2 = "load_collection", ("COPERNICUS_30", None)
    polygon = {"type": "Polygon", "coordinates": [[(0, 0), (180.0, 0), (0, 90.0), (180.0, 90.0)]]}
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0, 'crs': 'EPSG:4326'},
            "bands": ['VV', 'VH', 'HV', 'HH'],
            "resample": {
                "target_crs": "EPSG:4326",
                "resolution": [10.0, 10.0],
                "method": "near",
            },
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0, 'crs': 'EPSG:4326'},
            "bands": ["DEM"],
            "aggregate_spatial": {
                "geometries": DelayedVector.from_json_dict(polygon),
            },
            "resample": {
                "target_crs": "EPSG:3035",
                "resolution": [10000.0, 10000.0],
                "method": "near",
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 0


def test_extra_validation_layer_too_large_resample_spatial_auto42001(backend_implementation):
    # Resample spatial with Auto42001 as target projection.
    processing = GpsProcessing()
    source_id1 = "load_collection", ("COPERNICUS_30", None)
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 50.0, "east": 60.0, 'crs': 'EPSG:4326'},
            "bands": ["DEM"],
            "resample": {
                "target_crs": {
                    "id": {
                        "code": "Auto42001"
                    }
                },
                "resolution": [10000.0, 10000.0],
                "method": "near",
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 0

def test_extra_validation_layer_too_large_resample_spatial_zero(backend_implementation):
    # Resample with different CRS, but resolution 0 should be ok.
    processing = GpsProcessing()
    source_id1 = "load_collection", ("COPERNICUS_30", None)
    env_source_constraints = [
        (
            source_id1,
            {
                "temporal_extent": ["2019-01-01", "2019-01-02"],
                "spatial_extent": {"south": 0.0, "west": 0.0, "north": 50.0, "east": 60.0, "crs": "EPSG:4326"},
                "bands": ["DEM"],
                "resample": {
                    "target_crs": {"id": {"code": "Auto42001"}},
                    "resolution": [0.0, 0.0],
                    "method": "near",
                },
            },
        ),
    ]
    env = EvalEnv(
        values={
            ENV_SOURCE_CONSTRAINTS: env_source_constraints,
            "backend_implementation": backend_implementation,
            "version": "1.0.0",
        }
    )
    errors = list(processing.extra_validation({}, env, None, env_source_constraints))
    assert len(errors) == 0


class TestGpsProcessing:
    # TODO: move all these test_extra_validation_* inhere too.

    @pytest.mark.parametrize(
        ["sar_backscatter_spec", "expected"],
        [
            (
                None,
                {"coefficient": "gamma0-terrain"},
            ),
            (
                read_spec("openeo-processes/2.x/proposals/sar_backscatter.json"),
                {"coefficient": "gamma0-terrain"},
            ),
            (
                {"id": "sar_backscatter", "parameters": [{"name": "coefficient", "default": "omega666"}]},
                {"coefficient": "omega666"},
            ),
        ],
    )
    def test_get_default_sar_backscatter_arguments(self, sar_backscatter_spec, expected, caplog):
        caplog.set_level(logging.WARNING)

        class MyGpsProcessing(GpsProcessing):
            def __init__(self):
                self.process_registry = ProcessRegistry()

            def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
                return self.process_registry

        my_processing = MyGpsProcessing()
        if sar_backscatter_spec:
            my_processing.process_registry.add_spec(sar_backscatter_spec)

        sar_backscatter_arguments = my_processing.get_default_sar_backscatter_arguments(api_version="1.2.0")
        assert isinstance(sar_backscatter_arguments, SarBackscatterArgs)
        assert sar_backscatter_arguments._asdict() == dirty_equals.IsPartialDict(expected)
        assert caplog.text == ""


@pytest.mark.parametrize("success, state, status",
                         [
                             (True, "FINISHED", "SUCCEEDED"),
                             (False, "FAILED", "FAILED"),
                         ])
@pytest.mark.parametrize("shpu", [123.0, 0.0])
@gps_config_overrides(use_etl_api_on_sync_processing=True)
@mock.patch("openeogeotrellis.integrations.etl_api.get_etl_api_credentials_from_env")
def test_request_costs(mock_get_etl_api_credentials_from_env, backend_implementation, success, shpu, state, status):
    # TODO: this test does quite a bit of mocking, which might break when internals change
    #       The tested functionality is also tested in TestEtlApiReporting (test_api_result.py), so maybe we can remove this test?
    user_id = 'testuser'

    with mock.patch("openeogeotrellis.integrations.etl_api.EtlApi") as MockEtlApi, mock.patch(
        "openeogeotrellis.backend.get_jvm"
    ) as get_jvm:
        mock_etl_api = MockEtlApi.return_value
        mock_etl_api.log_resource_usage.return_value = 6

        tracker = get_jvm.return_value.org.openeo.geotrelliscommon.ScopedMetadataTracker.apply.return_value
        tracker.sentinelHubProcessingUnits.return_value = shpu

        credit_cost = backend_implementation.request_costs(
            user=User(user_id=user_id), request_id="r-abc123", success=success
        )

        mock_etl_api.log_resource_usage.assert_called_once_with(
            batch_job_id='r-abc123',
            title="Synchronous processing request 'r-abc123'",
            execution_id='r-abc123',
            user_id=user_id,
            started_ms=None,
            finished_ms=None,
            state=state,
            status=status,
            cpu_seconds=5400,
            mb_seconds=11059200,
            duration_ms=None,
            sentinel_hub_processing_units=shpu if shpu else None,
            additional_credits_cost=None,
            organization_id=None,
        )

        assert credit_cost == 6


def test_k8s_sparkapplication_dict_udf_python_deps(backend_config_path):
    # TODO: this is minimal attempt to have a bit of test coverage for UDF dep env var handling
    #       in the K8s code path. But there is a lot of room for improvement.
    #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/915
    app_dict = k8s_render_manifest_template(
        "sparkapplication.yaml.j2",
        udf_python_dependencies_folder_path="/jobs/j123/udfdepz.d",
        udf_python_dependencies_archive_path="/jobs/j123/udfdepz.zip",
        # TODO: avoid duplication of the actual code here
        openeo_backend_config=os.environ.get(ConfigGetter.OPENEO_BACKEND_CONFIG, ""),
        propagatable_web_app_driver_envars={},
    )
    assert app_dict == dirty_equals.IsPartialDict(
        spec=dirty_equals.IsPartialDict(
            driver=dirty_equals.IsPartialDict(
                env=dirty_equals.Contains(
                    {
                        "name": "PYTHONPATH",
                        "value": dirty_equals.IsStr(regex=r".+:/jobs/j123/udfdepz\.d(:|$)"),
                    },
                    {
                        "name": "UDF_PYTHON_DEPENDENCIES_FOLDER_PATH",
                        "value": "/jobs/j123/udfdepz.d",
                    },
                    {
                        "name": "UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH",
                        "value": "/jobs/j123/udfdepz.zip",
                    },
                    {"name": "OPENEO_BACKEND_CONFIG", "value": str(backend_config_path)},
                ),
            ),
            executor=dirty_equals.IsPartialDict(
                env=dirty_equals.Contains(
                    {
                        "name": "PYTHONPATH",
                        "value": dirty_equals.IsStr(regex=r".+:/jobs/j123/udfdepz\.d(:|$)"),
                    },
                    {
                        "name": "UDF_PYTHON_DEPENDENCIES_FOLDER_PATH",
                        "value": "/jobs/j123/udfdepz.d",
                    },
                    {
                        "name": "UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH",
                        "value": "/jobs/j123/udfdepz.zip",
                    },
                    {"name": "OPENEO_BACKEND_CONFIG", "value": str(backend_config_path)},
                ),
            ),
        )
    )


def test_k8s_s3_profiles_and_token():
    # Given a test token and test content for a AWS config file
    test_token = "mytoken1"
    test_profile = "[default]\nregion = eu-west-1\n"
    # Given a job id
    test_job_id = "j-123test"

    # Given how the base64 encoded form in a kubernetes manifest must look
    test_tokenb64 = "bXl0b2tlbjE="
    test_profile_b64 = "W2RlZmF1bHRdCnJlZ2lvbiA9IGV1LXdlc3QtMQo="

    # When we render the kubernetes manifest
    app_dict = k8s_render_manifest_template(
        "batch_job_cfg_secret.yaml.j2",
        secret_name="my-app-secret",
        job_id=test_job_id,
        token=test_token,
        profile_file_content=test_profile
    )
    # Then we expect an opaque secret with the base64 encoded format
    assert app_dict == dirty_equals.IsPartialDict(
        data=dirty_equals.IsPartialDict(
            profile_file=test_profile_b64,
            token=test_tokenb64,
        ),
        metadata=dirty_equals.IsPartialDict(
            labels=dirty_equals.IsPartialDict(job_id=test_job_id),
            name="my-app-secret"
        ),
        kind="Secret",
        type="Opaque"
    )


def test_k8s_s3_profiles_and_token_must_be_cleanable(backend_config_path, fast_sleep, time_machine):
    time_machine.move_to(1745410732)  # Wed 2025-04-23 12:18:52 UTC)
    # WHEN we render the kubernetes manifest for s3 access
    token_path = get_backend_config().batch_job_config_dir / "token"
    app_dict = k8s_render_manifest_template(
        "batch_job_cfg_secret.yaml.j2",
        secret_name="my-app",
        job_id="test_id",
        token="test",
        profile_file_content=S3Config.from_backend_config("j-any", str(token_path)),
    )
    # THEN we expect it to be cleanable by having a starttime set to the time it was created.
    # We can only clean files if we know they are stale
    assert app_dict == dirty_equals.IsPartialDict(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": dirty_equals.IsPartialDict(
                {
                    "labels": {"job_id": "test_id"},
                    "name": "my-app",
                    "annotations": {"created_at": "1745410732.0"},
                }
            ),
        }
    )


def test_k8s_sparkapplication_dict_propagatable_web_app_driver_envars(backend_config_path):
    app_dict = k8s_render_manifest_template(
        "sparkapplication.yaml.j2",
        propagatable_web_app_driver_envars={
            "OPENEO_SOME_ENVAR": "somevalue",
            "OPENEO_SOME_OTHER_ENVAR": "someothervalue",
        },
    )

    assert app_dict == dirty_equals.IsPartialDict(
        spec=dirty_equals.IsPartialDict(
            driver=dirty_equals.IsPartialDict(
                env=dirty_equals.Contains(
                    {
                        "name": "OPENEO_SOME_ENVAR",
                        "value": "somevalue",
                    },
                    {
                        "name": "OPENEO_SOME_OTHER_ENVAR",
                        "value": "someothervalue",
                    },
                ),
            ),
        )
    )
