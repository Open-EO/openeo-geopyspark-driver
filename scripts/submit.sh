#!/usr/bin/env bash
cd ../openeo-python-client
python3.5 setup.py bdist_egg
cd ../openeo-python-driver
python3.5 setup.py bdist_egg
cd dependencies
#spark only supports zipped dependencies
#tar xzOf MarkupSafe-1.0.tar.gz | zip MarkupSafe-1.0.zip $(tar tf MarkupSafe-1.0.tar.gz)
cd ../../openeo-geopyspark-driver
python3.5 setup.py bdist_egg
# pip download  -d dependencies -r requirements.txt
#export HDP_VERSION=2.5.3.0-37
#export HDP_VERSION=2.6.5.0-292
export HDP_VERSION=3.0.0.0-1634
export SPARK_MAJOR_VERSION=2
#export SPARK_HOME=/usr/hdp/current/spark2-client
#export SPARK_HOME=/usr/hdp/2.6.5.0-292/spark2
export SPARK_HOME=/usr/hdp/3.0.0.0-1634/spark2
export PYSPARK_PYTHON="/opt/rh/rh-python35/root/usr/bin/python3.5"
export LD_LIBRARY_PATH="/opt/rh/rh-python35/root/usr/lib64"
export PYTHONPATH=""
/usr/hdp/3.0.0.0-1634/spark2/bin/spark-submit --principal mep_tsviewer@VGT.VITO.BE --keytab mep_tsviewer.keytab --driver-memory 10G --executor-memory 5G --queue default \
 --conf spark.driver.memoryOverhead=3g \
 --conf spark.driver.maxResultSize=2g \
 --conf spark.speculation=true \
 --conf spark.speculation.quantile=0.4 --conf spark.speculation.multiplier=1.1 \
 --conf spark.dynamicAllocation.minExecutors=5 \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.executor.extraClassPath=geotrellis-extensions-1.0.0.jar:geotrellis-backend-assembly-0.4.2.jar \
 --driver-class-path py4j-0.10.7.jar:geotrellis-extensions-1.0.0.jar:geotrellis-backend-assembly-0.4.2.jar \
 --files /usr/hdp/3.0.0.0-1634/spark2/jars/py4j-0.10.7.jar,scripts/log4j.properties,layercatalog.json,jars/geotrellis-extensions-1.0.0.jar,dependencies/geopyspark/jars/geotrellis-backend-assembly-0.4.2.jar --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/rh/rh-python35/root/usr/bin/python3.5 \
  --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/opt/rh/rh-python35/root/usr/bin/python3.5 --conf spark.executorEnv.PYSPARK_PYTHON=/opt/rh/rh-python35/root/usr/bin/python3.5 \
  --conf spark.executorEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis --conf spark.yarn.appMasterEnv.DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis \
  --conf spark.executorEnv.LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64 --conf spark.executorEnv.XDG_DATA_DIRS=/opt/rh/rh-python35/root/usr/share \
  --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64 --conf spark.yarn.appMasterEnv.XDG_DATA_DIRS=/opt/rh/rh-python35/root/usr/share \
  --py-files dependencies/openeo_udf-0.0.post0.dev51+gb7bc661.dirty-py2.py3-none-any.whl,dependencies/colortools-0.1.2-py3.5.egg,dependencies/cloudpickle-0.5.2-py2.py3-none-any.whl,dependencies/geopyspark-0.4.2-py3-none-any.whl,../openeo-python-client/dist/openeo_api-0.0.1-py3.5.egg,../openeo-python-driver/dependencies/itsdangerous-0.24.zip,../openeo-python-driver/dependencies/MarkupSafe-1.0.zip,../openeo-python-driver/dependencies/Jinja2-2.10-py2.py3-none-any.whl,../openeo-python-driver/dependencies/Werkzeug-0.14.1-py2.py3-none-any.whl,../openeo-python-driver/dependencies/Flask-0.12.2-py2.py3-none-any.whl,dist/openeo_geopyspark-0.0.0-py3.5.egg,../openeo-python-driver/dist/openeo_driver-0.0.0-py3.5.egg,dependencies/gunicorn-19.7.1-py2.py3-none-any.whl,dependencies/kazoo-2.4.0-py2.py3-none-any.whl,dependencies/six-1.11.0-py2.py3-none-any.whl,dependencies/aiohttp-1.3.5-cp35-cp35m-manylinux1_x86_64.whl,dependencies/async_timeout-2.0.0-py3-none-any.whl,dependencies/chardet-3.0.4-py2.py3-none-any.whl,dependencies/idna-2.6-py2.py3-none-any.whl,dependencies/multidict-4.0.0-cp35-cp35m-manylinux1_x86_64.whl,dependencies/yarl-0.9.8-cp35-cp35m-manylinux1_x86_64.whl --master yarn --deploy-mode cluster --name OpenEO-GeoPySpark openeogeotrellis/deploy/probav-mep.py
