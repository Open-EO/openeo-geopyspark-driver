#!/usr/bin/env bash

set -exo pipefail

jobName="OpenEO-GeoPySpark"
pysparkPython="venv/bin/python"

export HDP_VERSION=3.1.0.0-78
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/${HDP_VERSION}/spark2
export LD_LIBRARY_PATH="venv/lib64"

export PYTHONPATH="venv/lib64/python3.6/site-packages:venv/lib/python3.6/site-packages"

hdfsVenvZip=https://artifactory.vgt.vito.be/auxdata-public/openeo/venv36.zip
extensions=https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/1.3.0-SNAPSHOT/geotrellis-extensions-1.3.0-SNAPSHOT.jar
backend_assembly=https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.2-openeo.jar

echo "Found backend assembly: ${backend_assembly}"


spark-submit \
 --master yarn --deploy-mode cluster \
 --queue default \
 --name ${jobName} \
 --principal mep_tsviewer@VGT.VITO.BE --keytab mep_tsviewer.keytab \
 --driver-memory 10G \
 --executor-memory 5G \
 --conf spark.driver.memoryOverhead=3g \
 --conf spark.executor.memoryOverhead=2g \
 --conf spark.driver.maxResultSize=2g \
 --conf spark.speculation=true \
 --conf spark.speculation.quantile=0.4 --conf spark.speculation.multiplier=1.1 \
 --conf spark.dynamicAllocation.minExecutors=5 --conf spark.dynamicAllocation.maxExecutors=30 \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
 --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.LD_LIBRARY_PATH=venv/lib64  \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=venv/lib64  \
 --conf spark.yarn.appMasterEnv.OPENEO_VENV_ZIP=${hdfsVenvZip} \
 --conf spark.executorEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis --conf spark.yarn.appMasterEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis \
 --conf spark.yarn.appMasterEnv.WMTS_BASE_URL_PATTERN=http://openeo.vgt.vito.be/openeo/services/%s \
 --conf spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} --conf spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
 --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --conf spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
 --files scripts/log4j.properties,layercatalog.json \
 --archives "${hdfsVenvZip}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars ${extensions},${backend_assembly} \
 openeogeotrellis/deploy/probav-mep.py
