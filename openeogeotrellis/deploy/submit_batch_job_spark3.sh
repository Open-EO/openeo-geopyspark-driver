#!/bin/sh -e

if [ "$#" -lt 9 ]; then
    >&2 echo "Usage: $0 <job name> <process graph input file> <output directory> <results output file> <principal> <key tab file> <OpenEO user> <api version> [driver memory] [executor memory]"
    exit 1
fi

if [ -z "${YARN_CONTAINER_RUNTIME_DOCKER_IMAGE}" ]; then
    >&2 echo "Environment variable YARN_CONTAINER_RUNTIME_DOCKER_IMAGE is not set"
    exit 1
fi

sparkSubmitLog4jConfigurationFile="/opt/venv/openeo-geopyspark-driver/submit_batch_job_log4j2.xml"

if [ ! -f ${sparkSubmitLog4jConfigurationFile} ]; then
    # TODO: is this path still valid?
    sparkSubmitLog4jConfigurationFile='scripts/submit_batch_job_log4j2.xml'
    if [ ! -f ${sparkSubmitLog4jConfigurationFile} ]; then
        >&2 echo "${sparkSubmitLog4jConfigurationFile} is missing"
        exit 1
    fi

fi

jobName=$1
processGraphFile=$2
outputDir=$3
outputFileName=$4
metadataFileName=$5
principal=$6
keyTab=$7
proxyUser=$8
apiVersion=${9}
drivermemory=${10-22G}
executormemory=${11-4G}
executormemoryoverhead=${12-3G}
drivercores=${13-14}
executorcores=${14-2}
drivermemoryoverhead=${15-8G}
queue=${16-default}
profile=${17-false}
dependencies=${18-"[]"}
pyfiles=${19}
maxexecutors=${20-500}
userId=${21}
batchJobId=${22}
maxSoftErrorsRatio=${23-"0.0"}
taskCpus=${24}
sentinelHubClientAlias=${25}
propertiesFile=${26}
archives=${27}
logging_threshold=${28}
openeo_backend_config=${29}
udf_python_dependencies_folder_path=${30}
ejr_api=${31}
ejr_backend_id=${32}
ejr_oidc_client_credentials=${33}
docker_mounts=${34-"/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/data/MTDA:/data/MTDA:ro,/data/projects/OpenEO:/data/projects/OpenEO:rw,/data/MEP:/data/MEP:ro,/data/users:/data/users:rw,/tmp_epod:/tmp_epod:rw"}
udf_python_dependencies_archive_path=${35}
propagatable_web_app_driver_envars=${36}
python_max_memory=${37-"-1"}


pysparkPython="/opt/venv/bin/python"

kinit -kt ${keyTab} ${principal} || true

export HDP_VERSION=3.1.4.0-315
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_SUBMIT_OPTS="-Dlog4j2.configurationFile=file:${sparkSubmitLog4jConfigurationFile}"
export LD_LIBRARY_PATH="/opt/venv/lib64"

if [ -n "$udf_python_dependencies_folder_path" ]; then
  export PYTHONPATH="$PYTHONPATH:$udf_python_dependencies_folder_path"
fi

extensions=${OPENEO_GEOTRELLIS_JAR:-local:///opt/geotrellis-extensions-static.jar}
backend_assembly="geotrellis-backend-assembly-static.jar"
logging_jar=$(ls openeo-logging-static.jar) || true

files="layercatalog.json,${processGraphFile}"
if [ -n "${logging_jar}" ]; then
  files="${files},${logging_jar}"
fi
if [ -f "client.conf" ]; then
  files="${files},client.conf"
fi
if [ -f "http_credentials.json" ]; then
  files="${files},http_credentials.json"
fi

main_py_file="/opt/venv/bin/openeo_batch.py"

sparkDriverJavaOptions="-Dscala.concurrent.context.maxThreads=2 -Dpixels.treshold=100000000\
 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/projects/OpenEO/${batchJobId}\
 -XX:ErrorFile=/data/projects/OpenEO/${batchJobId}/hs_err_pid%p.log\
 -Dlog4j2.configurationFile=file:/opt/venv/openeo-geopyspark-driver/batch_job_log4j2.xml\
 -Dhdp.version=3.1.4.0-315\
 -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
 -Dopeneo.logging.threshold=$logging_threshold"

sparkExecutorJavaOptions="-Dlog4j2.configurationFile=file:/opt/venv/openeo-geopyspark-driver/batch_job_log4j2.xml\
 -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
 -Dscala.concurrent.context.numThreads=8 -Djava.library.path=/opt/venv/lib/python3.8/site-packages/jep\
 -Dopeneo.logging.threshold=$logging_threshold"

# TODO: reuse FreeIpaClient here for better decoupling, logging, observability, ...
ipa_request='{"id": 0, "method": "user_find", "params": [["'${proxyUser}'"], {"all": false, "no_members": true, "sizelimit": 40000, "whoami": false}]}'
ipa_response=$(curl --negotiate -u : --insecure -X POST https://ipa01.vgt.vito.be/ipa/session/json   -H 'Content-Type: application/json' -H 'referer: https://ipa01.vgt.vito.be/ipa'  -d "${ipa_request}")
# TODO: fail early instead of trying to parse an IPA error page as JSON
echo "${ipa_response}"
ipa_user_count=$(echo "${ipa_response}" | python3 -c 'import json,sys;obj=json.load(sys.stdin);print(obj["result"]["count"])')
if [ "${ipa_user_count}" != "0" ]; then
  run_as="--proxy-user ${proxyUser}"
else
  run_as="--principal ${principal} --keytab ${keyTab}"
fi

python_max_conf=""
if [ "${python_max_memory}" != "-1" ]; then
  python_max_conf="--conf spark.executor.pyspark.memory=${python_max_memory}b"
fi


batch_job_driver_envar_arguments()
{
  arguments=""

  for name in ${propagatable_web_app_driver_envars}; do
    value=$(eval echo "\$${name}")
    arguments="${arguments} --conf spark.yarn.appMasterEnv.${name}=${value}"
  done

  echo "${arguments}"
}

spark-submit \
 --master yarn --deploy-mode cluster \
 ${run_as} \
 --conf spark.yarn.submit.waitAppCompletion=false \
 --queue "${queue}" \
 --driver-memory "${drivermemory}" \
 --executor-memory "${executormemory}" \
 --driver-java-options "${sparkDriverJavaOptions}" \
 --properties-file "${propertiesFile}" \
 --conf spark.executor.extraJavaOptions="${sparkExecutorJavaOptions}" \
 --conf spark.python.profile=$profile \
 --conf spark.executor.processTreeMetrics.enabled=true \
 --conf spark.kryoserializer.buffer.max=1G \
 --conf spark.kryo.classesToRegister=org.openeo.geotrellis.layers.BandCompositeRasterSource,geotrellis.raster.RasterRegion,geotrellis.raster.geotiff.GeoTiffResampleRasterSource,geotrellis.raster.RasterSource,geotrellis.raster.SourceName,geotrellis.raster.geotiff.GeoTiffPath \
 --conf spark.rpc.message.maxSize=200 \
 --conf spark.rdd.compress=true \
 --conf spark.driver.cores=${drivercores} \
 --conf spark.executor.cores=${executorcores} \
 --conf spark.task.cpus=${taskCpus} \
 --conf spark.driver.maxResultSize=5g \
 --conf spark.driver.memoryOverhead=${drivermemoryoverhead} \
 --conf spark.executor.memoryOverhead=${executormemoryoverhead} \
 ${python_max_conf} \
 --conf spark.excludeOnFailure.enabled=true \
 --conf spark.speculation=true \
 --conf spark.speculation.interval=5000ms \
 --conf spark.speculation.multiplier=8 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --conf spark.dynamicAllocation.maxExecutors=${maxexecutors} \
 --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf spark.executorEnv.LD_LIBRARY_PATH=/opt/venv/lib64 \
 --conf spark.executorEnv.PATH=/opt/venv/bin:$PATH \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=/opt/venv/lib64 \
 --conf spark.yarn.am.waitTime=900s \
 --conf spark.yarn.appMasterEnv.BATCH_JOBS_ZOOKEEPER_ROOT_PATH=${BATCH_JOBS_ZOOKEEPER_ROOT_PATH} \
 --conf spark.yarn.appMasterEnv.OPENEO_USER_ID=${userId} \
 --conf spark.yarn.appMasterEnv.OPENEO_BATCH_JOB_ID=${batchJobId} \
 --conf spark.yarn.appMasterEnv.OPENEO_LOGGING_THRESHOLD=${logging_threshold} \
 --conf spark.yarn.appMasterEnv.GDAL_HTTP_MAX_RETRY=10 \
 --conf spark.yarn.appMasterEnv.OPENEO_EJR_API=${ejr_api} \
 --conf spark.yarn.appMasterEnv.OPENEO_EJR_BACKEND_ID=${ejr_backend_id} \
 --conf spark.yarn.appMasterEnv.OPENEO_EJR_OIDC_CLIENT_CREDENTIALS=${ejr_oidc_client_credentials} \
 $(batch_job_driver_envar_arguments) \
 --conf spark.executorEnv.AWS_REGION=${AWS_REGION} --conf spark.yarn.appMasterEnv.AWS_REGION=${AWS_REGION} \
 --conf spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} --conf spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
 --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --conf spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
 --conf spark.executorEnv.OPENEO_USER_ID=${userId} \
 --conf spark.executorEnv.OPENEO_BATCH_JOB_ID=${batchJobId} \
 --conf spark.executorEnv.OPENEO_LOGGING_THRESHOLD=${logging_threshold} \
 --conf spark.yarn.appMasterEnv.OPENEO_BACKEND_CONFIG=${openeo_backend_config} \
 --conf spark.executorEnv.OPENEO_BACKEND_CONFIG=${openeo_backend_config} \
 --conf spark.dynamicAllocation.shuffleTracking.enabled=false --conf spark.dynamicAllocation.enabled=true \
 --conf spark.shuffle.service.enabled=true \
 --conf spark.ui.view.acls.groups=vito \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=${docker_mounts} \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=${docker_mounts} \
 --conf spark.yarn.appMasterEnv.UDF_PYTHON_DEPENDENCIES_FOLDER_PATH="$udf_python_dependencies_folder_path" \
 --conf spark.yarn.appMasterEnv.UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH="$udf_python_dependencies_archive_path" \
 --conf spark.executorEnv.UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH="$udf_python_dependencies_archive_path" \
 --conf spark.driver.extraClassPath=${logging_jar:-} \
 --conf spark.executor.extraClassPath=${logging_jar:-} \
 --conf spark.hadoop.yarn.timeline-service.enabled=false \
 --conf spark.hadoop.yarn.client.failover-proxy-provider=org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider \
 --conf spark.shuffle.service.name=spark_shuffle_320 --conf spark.shuffle.service.port=7557 \
 --conf spark.eventLog.enabled=true \
 --conf spark.eventLog.dir=hdfs:///spark2-history/ \
 --conf spark.history.fs.logDirectory=hdfs:///spark2-history/ \
 --conf spark.history.kerberos.enabled=true \
 --conf spark.history.kerberos.keytab=/etc/security/keytabs/spark.service.keytab \
 --conf spark.history.kerberos.principal=spark/_HOST@VGT.VITO.BE \
 --conf spark.history.provider=org.apache.spark.deploy.history.FsHistoryProvider \
 --conf spark.history.store.path=/var/lib/spark2/shs_db \
 --conf spark.yarn.historyServer.address=epod-ha.vgt.vito.be:18481 \
 --conf spark.archives=${archives} \
 --conf spark.extraListeners=org.openeo.sparklisteners.LogErrorSparkListener,org.openeo.sparklisteners.BatchJobProgressListener \
 --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
 --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=5242880 \
 --files "${files}" \
 --py-files "${pyfiles}" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --conf spark.yarn.tags=openeo \
 --jars "${extensions},local:///opt/geotrellis-dependencies-static.jar" \
 --name "${jobName}" \
 "${main_py_file}" "$(basename "${processGraphFile}")" "${outputDir}" "${outputFileName}" "${metadataFileName}" "${apiVersion}" "${dependencies}" "${userId}" "${maxSoftErrorsRatio}" "${sentinelHubClientAlias}"
