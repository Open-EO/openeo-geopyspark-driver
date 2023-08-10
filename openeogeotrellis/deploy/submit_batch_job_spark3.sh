#!/bin/sh -e

if [ "$#" -lt 7 ]; then
    >&2 echo "Usage: $0 <job name> <process graph input file> <results output file> <user log file> <principal> <key tab file> <OpenEO user> [api version] [driver memory] [executor memory]"
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
userLogFileName=$5
metadataFileName=$6
principal=$7
keyTab=$8
proxyUser=$9
apiVersion=${10}
drivermemory=${11-22G}
executormemory=${12-4G}
executormemoryoverhead=${13-3G}
drivercores=${14-14}
executorcores=${15-2}
drivermemoryoverhead=${16-8G}
queue=${17-default}
profile=${18-false}
dependencies=${19-"[]"}
pyfiles=${20}
maxexecutors=${21-500}
userId=${22}
batchJobId=${23}
maxSoftErrorsRatio=${24-"0.0"}
taskCpus=${25}
sentinelHubClientAlias=${26}
propertiesFile=${27}
archives=${28}
logging_threshold=${29}
openeo_backend_config=${30}

pysparkPython="/opt/venv/bin/python"

kinit -kt ${keyTab} ${principal} || true

export HDP_VERSION=3.1.4.0-315
export SPARK_HOME=/opt/spark3_4_0
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_SUBMIT_OPTS="-Dlog4j2.configurationFile=file:${sparkSubmitLog4jConfigurationFile}"
export LD_LIBRARY_PATH="/opt/venv/lib64"

export PYTHONPATH="/opt/venv/lib64/python3.8/site-packages:/opt/venv/lib/python3.8/site-packages:/opt/tensorflow/python38/2.8.0:/usr/lib/python3.8/site-packages:/usr/lib64/python3.8/site-packages"

extensions="geotrellis-extensions-static.jar"
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

main_py_file="/opt/venv/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py"

sparkDriverJavaOptions="-Dscala.concurrent.context.maxThreads=2 -Dpixels.treshold=100000000\
 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/projects/OpenEO/$(date +%s).hprof\
 -Dlog4j2.configurationFile=file:/opt/venv/openeo-geopyspark-driver/batch_job_log4j2.xml\
 -Dhdp.version=3.1.4.0-315\
 -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
 -Dopeneo.logging.threshold=$logging_threshold"

sparkExecutorJavaOptions="-Dlog4j2.configurationFile=file:/opt/venv/openeo-geopyspark-driver/batch_job_log4j2.xml\
 -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
 -Dscala.concurrent.context.numThreads=8 -Djava.library.path=/opt/venv/lib/python3.8/site-packages/jep\
 -Dopeneo.logging.threshold=$logging_threshold"

ipa_request='{"id": 0, "method": "user_find", "params": [["'${proxyUser}'"], {"all": false, "no_members": true, "sizelimit": 40000, "whoami": false}]}'
ipa_response=$(curl --negotiate -u : --insecure -X POST https://ipa01.vgt.vito.be/ipa/session/json   -H 'Content-Type: application/json' -H 'referer: https://ipa01.vgt.vito.be/ipa'  -d "${ipa_request}")
echo "${ipa_response}"
ipa_user_count=$(echo "${ipa_response}" | python3 -c 'import json,sys;obj=json.load(sys.stdin);print(obj["result"]["count"])')
if [ "${ipa_user_count}" != "0" ]; then
  run_as="--proxy-user ${proxyUser}"
else
  run_as="--principal ${principal} --keytab ${keyTab}"
fi

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
 --conf spark.excludeOnFailure.enabled=true \
 --conf spark.speculation=true \
 --conf spark.speculation.interval=5000ms \
 --conf spark.speculation.multiplier=8 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --conf spark.dynamicAllocation.maxExecutors=${maxexecutors} \
 --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$pysparkPython" \
 --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.LD_LIBRARY_PATH=/opt/venv/lib64 \
 --conf spark.executorEnv.PATH=/opt/venv/bin:$PATH \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=/opt/venv/lib64 \
 --conf spark.yarn.appMasterEnv.JAVA_HOME=${JAVA_HOME} \
 --conf spark.executorEnv.JAVA_HOME=${JAVA_HOME} \
 --conf spark.yarn.appMasterEnv.BATCH_JOBS_ZOOKEEPER_ROOT_PATH=${BATCH_JOBS_ZOOKEEPER_ROOT_PATH} \
 --conf spark.yarn.appMasterEnv.OPENEO_USER_ID=${userId} \
 --conf spark.yarn.appMasterEnv.OPENEO_BATCH_JOB_ID=${batchJobId} \
 --conf spark.yarn.appMasterEnv.OPENEO_LOGGING_THRESHOLD=${logging_threshold} \
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
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/data/MTDA:/data/MTDA:ro,/data/projects/OpenEO:/data/projects/OpenEO:rw,/data/MEP:/data/MEP:ro,/data/users:/data/users:rw,/tmp_epod:/tmp_epod:rw,/opt/tensorflow:/opt/tensorflow:ro,/data/worldcereal_data:/data/worldcereal_data:ro \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/data/MTDA:/data/MTDA:ro,/data/projects/OpenEO:/data/projects/OpenEO:rw,/data/MEP:/data/MEP:ro,/data/users:/data/users:rw,/tmp_epod:/tmp_epod:rw,/opt/tensorflow:/opt/tensorflow:ro,/data/worldcereal_data:/data/worldcereal_data:ro \
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
 --conf spark.extraListeners=org.openeo.sparklisteners.LogErrorSparkListener \
 --files "${files}" \
 --py-files "${pyfiles}" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --conf spark.yarn.tags=openeo \
 --jars "${extensions}","${backend_assembly}" \
 --name "${jobName}" \
 "${main_py_file}" "$(basename "${processGraphFile}")" "${outputDir}" "${outputFileName}" "${userLogFileName}" "${metadataFileName}" "${apiVersion}" "${dependencies}" "${userId}" "${maxSoftErrorsRatio}" "${sentinelHubClientAlias}"
