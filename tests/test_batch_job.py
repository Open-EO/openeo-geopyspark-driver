from unittest import TestCase


class TestBatchJob(TestCase):

    def test_parse_yarn(self):
        from openeogeotrellis import _extract_application_id
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
19/07/10 15:56:52 INFO Client: Uploading resource file:/data1/hadoop/yarn/local/usercache/jenkins/appcache/application_1562328661428_5538/container_e3344_1562328661428_5538_01_000001/geotrellis-backend-assembly-0.4.5-openeo.jar -> hdfs://hacluster/user/jenkins/.sparkStaging/application_1562328661428_5542/geotrellis-backend-assembly-0.4.5-openeo.jar
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
19/07/10 15:57:09 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:10 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:11 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:12 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:13 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:14 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:15 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:16 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:17 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:18 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:19 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:20 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:21 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:22 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:23 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:24 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:25 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:26 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:27 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:28 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:29 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:30 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:31 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:32 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:33 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:34 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:35 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:36 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:37 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:38 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:39 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:40 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:41 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:42 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:43 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:44 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:45 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:46 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:47 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:48 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:49 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:50 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:51 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:52 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:53 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:54 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:55 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:56 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:57 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
19/07/10 15:57:58 INFO Client: Application report for application_1562328661428_5542 (state: ACCEPTED)
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
19/07/10 15:58:12 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:13 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:14 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:15 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:16 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:17 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:18 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:19 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
19/07/10 15:58:20 INFO Client: Application report for application_1562328661428_5542 (state: RUNNING)
        """
        self.assertEquals("application_1562328661428_5542",_extract_application_id(yarn_log))