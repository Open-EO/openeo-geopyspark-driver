#!/usr/bin/env bash
cd ../openeo-client-api
python setup.py bdist_egg
cd ../openeo-python-driver
python setup.py bdist_egg
cd dependencies
#spark only supports zipped dependencies
#tar xzOf MarkupSafe-1.0.tar.gz | zip MarkupSafe-1.0.zip $(tar tf MarkupSafe-1.0.tar.gz)
cd ../../openeo-geopyspark-driver
python setup.py bdist_egg
# pip download  -d dependencies -r requirements.txt

spark-submit --queue lowlatency --conf spark.yarn.appMasterEnv.PYTHON_EGG_CACHE=./ --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/bin/python3.5 --conf spark.executorEnv.PYSPARK_PYTHON=/bin/python3.5 --py-files ../geopyspark/dist/geopyspark-0.2.2-py3.6.egg,../openeo-client-api/dist/openeo_api-0.0.1-py3.6.egg,../openeo-python-driver/dependencies/itsdangerous-0.24.zip,../openeo-python-driver/dependencies/MarkupSafe-1.0.zip,../openeo-python-driver/dependencies/Jinja2-2.10-py2.py3-none-any.whl,../openeo-python-driver/dependencies/Werkzeug-0.14.1-py2.py3-none-any.whl,../openeo-python-driver/dependencies/Flask-0.12.2-py2.py3-none-any.whl,dist/openeo_geopyspark-0.0.0-py3.6.egg,../openeo-python-driver/dist/openeo_driver-0.0.0-py3.6.egg,dependencies/gunicorn-19.7.1-py2.py3-none-any.whl --master yarn --deploy-mode cluster --name OpenEO-GeoPySpark openeogeotrellis/deploy/probav-mep.py