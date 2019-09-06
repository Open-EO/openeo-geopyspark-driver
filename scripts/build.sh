#!/usr/bin/env bash

set -exo pipefail

export LD_LIBRARY_PATH=/opt/rh/rh-python35/root/usr/lib64:${LD_LIBRARY_PATH}
unset PYTHONPATH

python3.7 -m venv ../install/venv
source ../install/venv/bin/activate

pip install --upgrade --force-reinstall pip
pip download typing==3.6.6
pip download Fiona==1.7.13 && pip install Fiona-1.7.13-cp37-cp37m-manylinux1_x86_64.whl

cd ../openeo-python-client
rm -rf dist/*
pip install travis-sphinx==2.1.0 "sphinx<1.7"
pip install -r requirements-dev.txt
pip install -r requirements.txt
python setup.py install bdist_egg
cp dist/* ../install/


cd ../openeo-python-driver
rm -rf dist/*
pip install -r requirements-dev.txt
pip install -r requirements.txt
python setup.py install bdist_egg
cp dist/* ../install/

cd ../openeo-geopyspark-driver
rm -rf dist/*
rm -rf jars/*
pip install $(cat requirements.txt | tr '\n' ' ' | sed -e 's/openeo-api==0.0.1/openeo-api/') --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple
SPARK_HOME=$(find_spark_home.py) geopyspark install-jar
#mkdir -p jars && mvn dependency:copy -Dartifact=org.openeo:geotrellis-extensions:1.1.0-SNAPSHOT -DoutputDirectory=jars
mkdir -p jars && mvn dependency:get -DremoteRepositories=https://artifactory.vgt.vito.be/libs-snapshot-public/  -Dartifact=org.openeo:geotrellis-extensions:1.1.0-SNAPSHOT -Ddest=./jars
python setup.py install bdist_egg
cp dist/* ../install/
cp jars/* ../install/
