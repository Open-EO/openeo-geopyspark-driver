import shapely

from openeo_driver.ProcessGraphDeserializer import ENV_SOURCE_CONSTRAINTS
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.utils import EvalEnv

from openeogeotrellis.backend import GpsBatchJobs, GpsProcessing

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
19/07/10 15:56:54 INFO Client: Uploading resource https://artifactory.vgt.vito.be/auxdata-public/openeo/venv.zip#venv -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/venv.zip
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
    assert GpsBatchJobs._extract_application_id(yarn_log) == "application_1562328661428_5542"


def test_get_submit_py_files_basic(tmp_path, caplog):
    (tmp_path / "lib.whl").touch()
    (tmp_path / "zop.zip").touch()
    (tmp_path / "__pyfiles__").mkdir()
    (tmp_path / "__pyfiles__" / "stuff.py").touch()
    env = {"OPENEO_SPARK_SUBMIT_PY_FILES": "stuff.py,lib.whl,foo.py"}
    py_files = GpsBatchJobs.get_submit_py_files(env=env, cwd=tmp_path)
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
    py_files = GpsBatchJobs.get_submit_py_files(env=env, cwd=tmp_path)
    assert py_files == "__pyfiles__/stuff.py,lib.whl"
    warn_logs = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert warn_logs == []


def test_get_submit_py_files_no_env(tmp_path):
    py_files = GpsBatchJobs.get_submit_py_files(env={}, cwd=tmp_path)
    assert py_files == ""


def test_get_submit_py_files_empty(tmp_path):
    env = {"OPENEO_SPARK_SUBMIT_PY_FILES": ""}
    py_files = GpsBatchJobs.get_submit_py_files(env=env, cwd=tmp_path)
    assert py_files == ""


def test_extra_validation_layer_too_large_drivervectorcube(backend_implementation):
    processing = GpsProcessing()
    pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    source_id2 = "load_collection", ("COPERNICUS_30", None)
    polygon = {"type": "Polygon", "coordinates": [[(0, 0), (180, 0), (0, 90), (180, 90)]]}
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929},
            "bands": ["B01", "B02", "B03"],
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": 0.0, "west": 0.0, "north": 90.0, "east": 180.0},
            "bands": ["B01", "B02", "B03"],
            "aggregate_spatial": {
                "geometries": DriverVectorCube.from_geojson(polygon),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation(pg, env, None, env_source_constraints))
    assert len(errors) == 2
    assert errors[0]['code'] == "LayerTooLarge"
    assert errors[1]['code'] == "LayerTooLarge"


def test_extra_validation_layer_too_large_delayedvector(backend_implementation):
    processing = GpsProcessing()
    pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    source_id2 = "load_collection", ("AGERA5", None)
    polygon1 = {"type": "Polygon", "coordinates": [[(0, 0), (300, 500), (800, 200), (0, 0)]]}
    polygon2 = {"type": "Polygon", "coordinates": [[(0, 0), (pow(10,9), 0), (0, pow(10,9)), (pow(10,9), pow(10,9))]]}
    geom_coll = {"type": "GeometryCollection", "geometries": [polygon1, polygon2]}
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929},
            "bands": ["B01", "B02", "B03"],
            "aggregate_spatial": {
                "geometries": DelayedVector.from_json_dict(polygon1),
            },
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929},
            "bands": ["B01", "B02", "B03"],
            "aggregate_spatial": {
                "geometries": DelayedVector.from_json_dict(geom_coll),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation(pg, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]['code'] == "LayerTooLarge"


def test_extra_validation_layer_too_large_geometrycollection(backend_implementation):
    processing = GpsProcessing()
    pg = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
    source_id1 = "load_collection", ("SENTINEL1_GRD", None)
    source_id2 = "load_collection", ("AGERA5", None)
    polygon1 = shapely.geometry.Polygon([(0, 0), (pow(10,6), 0), (0, pow(10,6)), (pow(10,6), pow(10,6))])
    polygon2 = shapely.geometry.Polygon([(pow(10,6), pow(10,6)), (pow(10,6), pow(10,9)), (pow(10,9), pow(10,6)), (pow(10,9), pow(10,9))])
    env_source_constraints = [
        (source_id1, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929},
            "bands": ["B01", "B02", "B03"],
            "aggregate_spatial": {
                "geometries": shapely.geometry.MultiPolygon([polygon1]),
            },
        }),
        (source_id2, {
            "temporal_extent": ["2019-01-01", "2019-01-02"],
            "spatial_extent": {"south": -952987.7582, "west": 4495130.8875, "north": 910166.7419, "east": 7088482.3929},
            "bands": ["B01", "B02", "B03"],
            "aggregate_spatial": {
                "geometries": shapely.geometry.GeometryCollection([polygon1, polygon2]),
            },
        }),
    ]
    env = EvalEnv(values={ENV_SOURCE_CONSTRAINTS: env_source_constraints, "backend_implementation": backend_implementation, "version": "1.0.0"})
    errors = list(processing.extra_validation(pg, env, None, env_source_constraints))
    assert len(errors) == 1
    assert errors[0]['code'] == "LayerTooLarge"
