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
openEoUser=$9
apiVersion=${10}
drivermemory=${11-22G}
executormemory=${12-4G}
executormemoryoverhead=${13-2G}
drivercores=${14-14}
executorcores=${15-2}
drivermemoryoverhead=${16-8G}
queue=${17-default}
profile=${18-false}
dependencies=${19-"no_dependencies"}

pysparkPython="venv/bin/python"

kinit -kt ${keyTab} ${principal} || true

export HDP_VERSION=3.1.4.0-315
export SPARK_HOME=/usr/hdp/$HDP_VERSION/spark2
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_SUBMIT_OPTS="-Dlog4j.configuration=file:${sparkSubmitLog4jConfigurationFile}"
export LD_LIBRARY_PATH="venv/lib64"

export PYTHONPATH="venv/lib64/python3.6/site-packages:venv/lib/python3.6/site-packages"

extensions=$(ls geotrellis-extensions-*.jar)
backend_assembly=$(ls geotrellis-backend-assembly-*.jar) || true
if [ ! -f ${backend_assembly} ]; then
   backend_assembly=https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo.jar
fi

pyfiles="--py-files cropsar*.whl"
if [ -f __pyfiles__/custom_processes.py ]; then
   pyfiles=${pyfiles},__pyfiles__/custom_processes.py
fi

main_py_file='venv/lib64/python3.6/site-packages/openeogeotrellis/deploy/batch_job.py'

sparkDriverJavaOptions="-Dscala.concurrent.context.maxThreads=2\
 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/data/projects/OpenEO/$(date +%s).hprof\
 -Dlog4j.debug=true -Dlog4j.configuration=file:venv/batch_job_log4j.properties"

if PYTHONPATH= ipa -v user-find --login "${openEoUser}"; then
  run_as="--proxy-user ${openEoUser}"
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
 --conf spark.python.profile=$profile \
 --conf spark.kryoserializer.buffer.max=512m \
 --conf spark.rpc.message.maxSize=200 \
 --conf spark.rdd.compress=true \
 --conf spark.driver.cores=${drivercores} \
 --conf spark.executor.cores=${executorcores} \
 --conf spark.driver.maxResultSize=5g \
 --conf spark.driver.memoryOverhead=${drivermemoryoverhead} \
 --conf spark.executor.memoryOverhead=${executormemoryoverhead} \
 --conf spark.blacklist.enabled=true \
 --conf spark.speculation=true \
 --conf spark.speculation.interval=5000ms \
 --conf spark.speculation.multiplier=4 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$pysparkPython" \
 --conf spark.executorEnv.LD_LIBRARY_PATH=venv/lib64 \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=venv/lib64 \
 --conf spark.executorEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis --conf spark.yarn.appMasterEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis \
 --conf spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} --conf spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
 --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --conf spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
 --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
 --conf spark.ui.view.acls.groups=vito \
 --files layercatalog.json,"${processGraphFile}" ${pyfiles} \
 --archives "${OPENEO_VENV_ZIP}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars "${extensions}","${backend_assembly}" \
 --name "${jobName}" \
 "${main_py_file}" "$(basename "${processGraphFile}")" "${outputDir}" "${outputFileName}" "${userLogFileName}" "${metadataFileName}" "${apiVersion}" "${dependencies}"
