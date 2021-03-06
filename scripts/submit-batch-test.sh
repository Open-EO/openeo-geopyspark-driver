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
extensions=https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.2.0-SNAPSHOT/geotrellis-extensions-2.2.0-SNAPSHOT.jar
extensions=geotrellis-extensions-2.2.0-SNAPSHOT.jar
backend_assembly=https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo.jar

echo "Found backend assembly: ${backend_assembly}"
graph=$1

spark-submit \
 --master yarn --deploy-mode cluster \
 --queue default \
 --name ${jobName} \
 --driver-memory 16G \
 --executor-memory 4G \
 --conf spark.driver.cores=18 \
 --driver-java-options "-Dscala.concurrent.context.maxThreads=2" \
 --principal mep_tsviewer@VGT.VITO.BE --keytab mep_tsviewer.keytab \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 --conf spark.kryo.classesToRegister=org.openeo.geotrellisaccumulo.SerializableConfiguration \
 --conf spark.kryoserializer.buffer.max=512m \
 --conf spark.rpc.message.maxSize=200 \
 --conf spark.rdd.compress=true \
 --conf spark.driver.memoryOverhead=1g \
 --conf spark.executor.memoryOverhead=2g \
 --conf spark.driver.maxResultSize=3g \
 --conf spark.dynamicAllocation.minExecutors=5 --conf spark.dynamicAllocation.maxExecutors=200 \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
 --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.LD_LIBRARY_PATH=venv/lib64:/tmp_epod/gdal \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=venv/lib64:/tmp_epod/gdal \
 --conf spark.executorEnv.PROJ_LIB=/tmp_epod/gdal/data \
 --conf spark.yarn.appMasterEnv.PROJ_LIB=/tmp_epod/gdal/data \
 --conf spark.yarn.appMasterEnv.OPENEO_VENV_ZIP=${hdfsVenvZip} \
 --conf spark.executorEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis --conf spark.yarn.appMasterEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis \
 --conf spark.yarn.appMasterEnv.WMTS_BASE_URL_PATTERN=http://openeo.vgt.vito.be/openeo/services/%s \
 --conf spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} --conf spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
 --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --conf spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
 --files scripts/log4j.properties,layercatalog.json,"${graph}" \
 --archives "${hdfsVenvZip}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars ${extensions},${backend_assembly} \
 openeogeotrellis/deploy/batch_job.py "$(basename "${graph}")" result.out 0.4.0
