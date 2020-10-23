#!/usr/bin/env bash

set -exo pipefail

jobName="OpenEO huge vector file benchmark"
pysparkPython="venv/bin/python"

export HDP_VERSION=3.1.0.0-78
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/${HDP_VERSION}/spark2
export LD_LIBRARY_PATH="venv/lib64"
export PYTHONPATH="venv/lib64/python3.6/site-packages:venv/lib/python3.6/site-packages"

hdfsVenvZip=https://artifactory.vgt.vito.be/auxdata-public/openeo/venv36.zip
extensions=https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/1.4.0-SNAPSHOT/geotrellis-extensions-1.4.0-SNAPSHOT.jar
backend_assembly=https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo.jar

script_dir=$(dirname "$0")
base_dir="${script_dir}/.."

spark-submit \
 --master yarn --deploy-mode cluster \
 --name "${jobName}" \
 --principal mep_tsviewer@VGT.VITO.BE --keytab mep_tsviewer.keytab \
 --driver-memory 6G \
 --executor-memory 4G \
 --conf spark.driver.memoryOverhead=1g \
 --conf spark.executor.memoryOverhead=2g \
 --conf spark.speculation=false \
 --num-executors 20 \
 --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64 --conf spark.executorEnv.XDG_DATA_DIRS=/opt/rh/rh-python35/root/usr/share \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64 --conf spark.yarn.appMasterEnv.XDG_DATA_DIRS=/opt/rh/rh-python35/root/usr/share \
 --conf spark.executorEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis --conf spark.yarn.appMasterEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis \
 --conf spark.driver.extraJavaOptions="-Dpixels.treshold=0" \
 --files "${base_dir}/scripts/log4j.properties","${base_dir}/layercatalog.json" \
 --archives "${hdfsVenvZip}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars ${extensions},${backend_assembly} \
 "${script_dir}/benchmark.py" "$@"
