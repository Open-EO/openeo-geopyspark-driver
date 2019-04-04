#!/usr/bin/env bash

set -exo pipefail

jobName="OpenEO-GeoPySpark"
pysparkPython="venv/bin/python"

export HDP_VERSION=3.0.0.0-1634
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/${HDP_VERSION}/spark2
export LD_LIBRARY_PATH="/opt/rh/rh-python35/root/usr/lib64"
export PYTHONPATH=""

pushd venv/
zip -r ../venv.zip *
popd

hdfsVenvDir=${jobName}

hadoop fs -mkdir -p ${hdfsVenvDir}
hadoop fs -put -f venv.zip ${hdfsVenvDir}

hdfsVenvZip=hdfs:/user/$(hadoop fs -stat %u ${hdfsVenvDir}/venv.zip)/${hdfsVenvDir}/venv.zip

extensions=$(ls -1 geotrellis-extensions-*.jar | tail -n 1)
backend_assembly=$(find venv -name 'geotrellis-backend-assembly-*.jar' | tail -n 1)

spark-submit \
 --master yarn --deploy-mode cluster \
 --queue default \
 --name ${jobName} \
 --principal mep_tsviewer@VGT.VITO.BE --keytab mep_tsviewer.keytab \
 --driver-memory 10G \
 --executor-memory 5G \
 --conf spark.driver.memoryOverhead=3g \
 --conf spark.driver.maxResultSize=2g \
 --conf spark.speculation=true \
 --conf spark.speculation.quantile=0.4 --conf spark.speculation.multiplier=1.1 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
 --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.PYSPARK_PYTHON=${pysparkPython} \
 --conf spark.executorEnv.LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64 --conf spark.executorEnv.XDG_DATA_DIRS=/opt/rh/rh-python35/root/usr/share \
 --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64 --conf spark.yarn.appMasterEnv.XDG_DATA_DIRS=/opt/rh/rh-python35/root/usr/share \
 --conf spark.yarn.appMasterEnv.OPENEO_VENV_ZIP=${hdfsVenvZip} \
 --conf spark.executorEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis --conf spark.yarn.appMasterEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis \
 --files $(ls typing-*-none-any.whl),scripts/log4j.properties,layercatalog.json,scripts/submit_batch_job.sh,openeogeotrellis/deploy/batch_job.py \
 --archives "${hdfsVenvZip}#venv" \
 --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
 --jars ${extensions},${backend_assembly} \
 openeogeotrellis/deploy/probav-mep.py
