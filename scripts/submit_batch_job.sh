#!/bin/sh -e

if [ -z "${OPENEO_VENV_ZIP}" ]; then
    >&2 echo "Environment variable OPENEO_VENV_ZIP is not set"
    exit 1
fi

if [ "$#" -ne 5 ]; then
    >&2 echo "Usage: $0 <job name> <process graph input file> <results output file> <principal> <key tab file>"
    exit 1
fi

export LOG4J_CONFIGURATION_FILE="./log4j.properties"

if [ ! -f ${LOG4J_CONFIGURATION_FILE} ]; then
    >&2 echo "${LOG4J_CONFIGURATION_FILE} is missing"
    exit 1
fi

jobName=$1
processGraphFile=$2
outputFile=$3
principal=$4
keyTab=$5

kinit -kt ${keyTab} ${principal}

export SPARK_HOME=/usr/hdp/current/spark2-client
export PYSPARK_PYTHON="./python"
export PATH="$SPARK_HOME/bin:$PATH"
export SPARK_SUBMIT_OPTS="-Dlog4j.configuration=file:${LOG4J_CONFIGURATION_FILE}"

extensions=$(ls GeoPySparkExtensions-*.jar)
backend_assembly=$(ls geotrellis-backend-assembly-*.jar)

spark-submit \
 --master yarn --deploy-mode cluster \
 --principal ${principal} --keytab ${keyTab} \
 --conf spark.executor.memory=8G \
 --conf spark.speculation=true \
 --conf spark.speculation.quantile=0.4 --conf spark.speculation.multiplier=1.1 \
 --conf spark.dynamicAllocation.minExecutors=20 \
 --conf "spark.yarn.appMasterEnv.SPARK_HOME=$SPARK_HOME" --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
 --files python,$(ls typing-*-none-any.whl),layercatalog.json,"${processGraphFile}" \
 --archives "${OPENEO_VENV_ZIP}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars "${extensions}","${backend_assembly}" \
 --name "${jobName}" batch_job.py "$(basename "${processGraphFile}")" "${outputFile}"
