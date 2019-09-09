#!/usr/bin/env bash

#set -exo pipefail

unset PYTHONPATH

INSTALL_DEPS="0"

INSTALLDIR=$(cd ../install ; pwd)
echo $INSTALLDIR

python3.7 -m venv $INSTALLDIR/venv
. $INSTALLDIR/venv/bin/activate

if [ "$INSTALL_DEPS" == "1" ] ; then
  pip install --upgrade --force-reinstall pip
  pip download typing==3.6.6
  pip download Fiona==1.7.13 && pip install Fiona-1.7.13-cp37-cp37m-manylinux1_x86_64.whl
fi

cd ../openeo-python-client
rm -rf dist/*
if [ "$INSTALL_DEPS" == "1" ] ; then
  pip install travis-sphinx==2.1.0 "sphinx<1.7"
  pip install -r requirements-dev.txt
  pip install -r requirements.txt
fi 
python3 setup.py install bdist_egg
cp dist/* $INSTALLDIR/

cd ../openeo-python-driver
rm -rf dist/*
if [ "$INSTALL_DEPS" == "1" ] ; then
  pip install -r requirements-dev.txt
  pip install -r requirements.txt
fi
python3 setup.py install bdist_egg
cp dist/* $INSTALLDIR/

cd ../openeo-geopyspark-driver
rm -rf dist/*
#mkdir -p jars 
#rm -rf jars/*
if [ "$INSTALL_DEPS" == "1" ] ; then
  pip install $(cat requirements.txt | tr '\n' ' ' | sed -e 's/openeo-api==0.0.1/openeo-api/') --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple
  SPARK_HOME=$(find_spark_home.py) geopyspark install-jar
  #mkdir -p jars && mvn dependency:copy -Dartifact=org.openeo:geotrellis-extensions:1.1.0-SNAPSHOT -DoutputDirectory=jars
  #mvn dependency:get -DremoteRepositories=https://artifactory.vgt.vito.be/libs-snapshot-public/  -Dartifact=org.openeo:geotrellis-extensions:1.1.0-SNAPSHOT -Ddest=./jars
  #mvn dependency:get -DremoteRepositories=https://artifactory.vgt.vito.be/auxdata-public/  -Dartifact=org.openeo:ggeotrellis-backend-assembly-0.4.2 -Ddest=./jars
  curl https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/1.2.0-SNAPSHOT/geotrellis-extensions-1.2.0-SNAPSHOT.jar -O
  curl https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.2.jar -O
fi
python3 setup.py install bdist_egg
cp dist/* $INSTALLDIR/
cp ./*.jar $INSTALLDIR/
cp layercatalog.json $INSTALLDIR/
mkdir -p $INSTALLDIR/scripts
cp scripts/*.sh $INSTALLDIR/scripts/
cp scripts/*.properties $INSTALLDIR/scripts/
mkdir -p $INSTALLDIR/openeogeotrellis/deploy
cp openeogeotrellis/deploy/*.py $INSTALLDIR/openeogeotrellis/deploy/

echo "INSTALL SUCCESSFUL"

