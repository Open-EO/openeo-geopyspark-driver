#!/usr/bin/env bash

set -exo pipefail

export LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64:${LD_LIBRARY_PATH}
unset PYTHONPATH

python3.5 -m venv venv
source venv/bin/activate

pip install --upgrade --force-reinstall pip
pip download typing==3.6.6
pip download Fiona==1.7.13 && pip install Fiona-1.7.13-cp35-cp35m-manylinux1_x86_64.whl

cd ../openeo-python-client
pip install travis-sphinx==2.1.0 "sphinx<1.7"
pip install -r requirements-dev.txt
pip install -r requirements.txt
python setup.py install bdist_egg

cd ../openeo-python-driver
pip install -r requirements-dev.txt
pip install -r requirements.txt
python setup.py install bdist_egg

cd ../openeo-geopyspark-driver
pip install $(cat requirements.txt | tr '\n' ' ' | sed -e 's/openeo-api==0.0.1/openeo-api/') --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple
SPARK_HOME=$(find_spark_home.py) geopyspark install-jar
mkdir -p jars && curl -sSf https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/1.0.0-SNAPSHOT/geotrellis-extensions-1.0.0-SNAPSHOT.jar -o jars/geotrellis-extensions-1.0.0-SNAPSHOT.jar
python setup.py install bdist_egg

mvn dependency:copy -Dartifact=org.openeo:geotrellis-extensions:1.0.0-SNAPSHOT -DoutputDirectory=.
