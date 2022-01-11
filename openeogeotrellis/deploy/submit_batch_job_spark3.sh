#!/bin/sh -e

if [ -z "${OPENEO_VENV_ZIP}" ]; then
    >&2 echo "Environment variable OPENEO_VENV_ZIP is not set, falling back to default.\n"
    OPENEO_VENV_ZIP=https://artifactory.vgt.vito.be/auxdata-public/openeo/venv36.zip
fi

if [ "$#" -lt 7 ]; then
    >&2 echo "Usage: $0 <job name> <process graph input file> <results output file> <user log file> <principal> <key tab file> <OpenEO user> [api version] [driver memory] [executor memory]"
    exit 1
fi

sparkSubmitLog4jConfigurationFile="venv/submit_batch_job_log4j.properties"

if [ ! -f ${sparkSubmitLog4jConfigurationFile} ]; then
    sparkSubmitLog4jConfigurationFile='scripts/submit_batch_job_log4j.properties'
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

pysparkPython="venv/bin/python"

kinit -kt ${keyTab} ${principal} || true

export HDP_VERSION=3.1.4.0-315
export SPARK_HOME=/opt/spark3_2_0
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_SUBMIT_OPTS="-Dlog4j.configuration=file:${sparkSubmitLog4jConfigurationFile}"
export LD_LIBRARY_PATH="venv/lib64"

export PYTHONPATH="venv/lib64/python3.8/site-packages:venv/lib/python3.8/site-packages"

extensions=$(ls geotrellis-extensions-*.jar)
backend_assembly=$(ls geotrellis-backend-assembly-*.jar) || true
if [ -z "${backend_assembly}" ]; then
   backend_assembly=https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo_2.12.jar
fi
logging_jar=$(ls openeo-logging-*.jar) || true

files="layercatalog.json,${processGraphFile}"
if [ -n "${logging_jar}" ]; then
  files="${files},${logging_jar}"
fi
if [ -f "client.conf" ]; then
  files="${files},client.conf"
fi

openeo_zip="$(basename "${OPENEO_VENV_ZIP}")"

if [ ! -f "${openeo_zip}" ]; then
  echo "Downloading ${OPENEO_VENV_ZIP}"
  curl --retry 3 --connect-timeout 60 -C - -O "${OPENEO_VENV_ZIP}"
fi

main_py_file='venv/lib/python3.8/site-packages/openeogeotrellis/deploy/batch_job.py'

sparkDriverJavaOptions="-Dscala.concurrent.context.maxThreads=2 -Dpixels.treshold=100000000\
 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/projects/OpenEO/$(date +%s).hprof\
 -Dlog4j.debug=true -Dlog4j.configuration=file:venv/batch_job_log4j.properties\
 -Dhdp.version=3.1.4.0-315\
 -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService"

sparkExecutorJavaOptions="-Dlog4j.debug=true -Dlog4j.configuration=file:venv/batch_job_log4j.properties\
 -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
 -Dscala.concurrent.context.numThreads=8 -Djava.library.path=venv/lib/python3.8/site-packages/jep"

image=${YARN_CONTAINER_RUNTIME_DOCKER_IMAGE:-"vito-docker-private.artifactory.vgt.vito.be/python38-hadoop:latest"}

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
 --conf spark.executor.extraJavaOptions="${sparkExecutorJavaOptions}" \
 --conf spark.python.profile=$profile \
 --conf spark.kryoserializer.buffer.max=1G \
 --conf spark.kryo.classesToRegister=org.openeo.geotrellis.layers.BandCompositeRasterSource,geotrellis.raster.RasterRegion,geotrellis.raster.geotiff.GeoTiffResampleRasterSource,geotrellis.raster.RasterSource,geotrellis.raster.SourceName,geotrellis.raster.geotiff.GeoTiffPath \
 --conf spark.rpc.message.maxSize=200 \
 --conf spark.rdd.compress=true \
 --conf spark.driver.cores=${drivercores} \
 --conf spark.executor.cores=${executorcores} \
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
 --conf spark.executorEnv.LD_LIBRARY_PATH=venv/lib64 \
 --conf spark.executorEnv.PATH=venv/bin:$PATH \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=venv/lib64 \
 --conf spark.yarn.appMasterEnv.JAVA_HOME=${JAVA_HOME} \
 --conf spark.executorEnv.JAVA_HOME=${JAVA_HOME} \
 --conf spark.yarn.appMasterEnv.BATCH_JOBS_ZOOKEEPER_ROOT_PATH=${BATCH_JOBS_ZOOKEEPER_ROOT_PATH} \
 --conf spark.yarn.appMasterEnv.OPENEO_USER_ID=${userId} \
 --conf spark.yarn.appMasterEnv.OPENEO_BATCH_JOB_ID=${batchJobId} \
 --conf spark.executorEnv.AWS_REGION=${AWS_REGION} --conf spark.yarn.appMasterEnv.AWS_REGION=${AWS_REGION} \
 --conf spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} --conf spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
 --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --conf spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
 --conf spark.executorEnv.OPENEO_USER_ID=${userId} \
 --conf spark.executorEnv.OPENEO_BATCH_JOB_ID=${batchJobId} \
 --conf spark.dynamicAllocation.shuffleTracking.enabled=false --conf spark.dynamicAllocation.enabled=true \
 --conf spark.shuffle.service.enabled=true \
 --conf spark.ui.view.acls.groups=vito \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/data/MTDA:/data/MTDA:ro,/data/projects/OpenEO:/data/projects/OpenEO:rw,/data/MEP:/data/MEP:ro,/data/users:/data/users:rw,/tmp_epod:/tmp_epod:rw \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${image} \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=${image} \
 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/var/lib/sss/pubconf/krb5.include.d:/var/lib/sss/pubconf/krb5.include.d:ro,/var/lib/sss/pipes:/var/lib/sss/pipes:rw,/usr/hdp/current/:/usr/hdp/current/:ro,/etc/hadoop/conf/:/etc/hadoop/conf/:ro,/etc/krb5.conf:/etc/krb5.conf:ro,/data/MTDA:/data/MTDA:ro,/data/projects/OpenEO:/data/projects/OpenEO:rw,/data/MEP:/data/MEP:ro,/data/users:/data/users:rw,/tmp_epod:/tmp_epod:rw \
 --conf spark.driver.extraClassPath=${logging_jar:-} \
 --conf spark.executor.extraClassPath=${logging_jar:-} \
 --conf spark.hadoop.yarn.timeline-service.enabled=false \
 --conf spark.hadoop.yarn.client.failover-proxy-provider=org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider \
 --conf spark.shuffle.service.name=spark_shuffle_320 --conf spark.shuffle.service.port=7557 \
 --files "${files}" \
 --py-files "${pyfiles}" \
 --archives "${openeo_zip}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --conf spark.yarn.tags=openeo \
 --jars "${extensions}","${backend_assembly}" \
 --name "${jobName}" \
 "${main_py_file}" "$(basename "${processGraphFile}")" "${outputDir}" "${outputFileName}" "${userLogFileName}" "${metadataFileName}" "${apiVersion}" "${dependencies}" "${proxyUser}"
