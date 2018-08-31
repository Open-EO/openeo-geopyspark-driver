#!/bin/sh -e

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <job name> <process graph input file> <results output file> <principal> <key tab file>"
    exit 1
fi

jobName=$1
processGraphFile=$2
outputFile=$3
principal=$4
keyTab=$5

kinit -kt ${keyTab} ${principal}

export HDP_VERSION=2.5.3.0-37
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client
export PYSPARK_PYTHON="/usr/bin/python3.5"

extensions=$(ls GeoPySparkExtensions-*.jar)
backend_assembly=$(ls geotrellis-backend-assembly-*.jar)

spark-submit \
 --master yarn --deploy-mode cluster \
 --principal ${principal} --keytab ${keyTab} \
 --conf spark.executor.memory=8G \
 --conf spark.speculation=true \
 --conf spark.speculation.quantile=0.4 --conf spark.speculation.multiplier=1.1 \
 --conf spark.dynamicAllocation.minExecutors=20 \
 --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/hdp/current/spark2-client --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.5 --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.5 \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.executor.extraClassPath="$(basename "${backend_assembly}")" \
 --driver-class-path "$(basename "${backend_assembly}")" \
 --files "${backend_assembly}",layercatalog.json,"${processGraphFile}" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars "${extensions}" \
 --py-files $(ls openeo_api-*.egg),$(ls openeo_driver*.egg),$(ls openeo_geopyspark*.egg),libs.zip \
 --name "${jobName}" batch_job.py "$(basename "${processGraphFile}")" "${outputFile}"
