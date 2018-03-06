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
export HDP_VERSION=2.5.3.0-37
export SPARK_MAJOR_VERSION=2
export SPARK_HOME=/usr/hdp/current/spark2-client
export PYSPARK_PYTHON="/usr/bin/python3.5"
spark-submit --principal mep_tsviewer@VGT.VITO.BE --keytab mep_tsviewer.keytab --driver-memory 8G --executor-memory 4G --queue default \
 --conf spark.speculation=true \
 --conf spark.speculation.quantile=0.4 --conf spark.speculation.multiplier=1.1 \
 --conf spark.dynamicAllocation.minExecutors=30 \
 --conf spark.locality.wait=300ms --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true --conf spark.executor.extraClassPath=geotrellis-backend-assembly-0.3.0.jar \
 --driver-class-path geotrellis-backend-assembly-0.3.0.jar \
 --files /home/driesj/pythonworkspace/geopyspark/geopyspark/jars/geotrellis-backend-assembly-0.3.0.jar --conf spark.hadoop.security.authentication=kerberos --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/hdp/current/spark2-client  --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ \
  --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.5 --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.5 \
  --py-files dependencies/cloudpickle-0.5.2-py2.py3-none-any.whl,dependencies/geopyspark-0.3.0+openeo1-py3.6.egg,../openeo-python-client/dist/openeo_api-0.0.1-py3.5.egg,../openeo-python-driver/dependencies/itsdangerous-0.24.zip,../openeo-python-driver/dependencies/MarkupSafe-1.0.zip,../openeo-python-driver/dependencies/Jinja2-2.10-py2.py3-none-any.whl,../openeo-python-driver/dependencies/Werkzeug-0.14.1-py2.py3-none-any.whl,../openeo-python-driver/dependencies/Flask-0.12.2-py2.py3-none-any.whl,dist/openeo_geopyspark-0.0.0-py3.5.egg,../openeo-python-driver/dist/openeo_driver-0.0.0-py3.5.egg,dependencies/gunicorn-19.7.1-py2.py3-none-any.whl,dependencies/kazoo-2.4.0-py2.py3-none-any.whl,dependencies/six-1.11.0-py2.py3-none-any.whl,dependencies/aiohttp-1.3.5-cp35-cp35m-manylinux1_x86_64.whl,dependencies/async_timeout-2.0.0-py3-none-any.whl,dependencies/chardet-3.0.4-py2.py3-none-any.whl,dependencies/idna-2.6-py2.py3-none-any.whl,dependencies/multidict-4.0.0-cp35-cp35m-manylinux1_x86_64.whl,dependencies/yarl-0.9.8-cp35-cp35m-manylinux1_x86_64.whl --master yarn --deploy-mode cluster --name OpenEO-GeoPySpark openeogeotrellis/deploy/probav-mep.py
