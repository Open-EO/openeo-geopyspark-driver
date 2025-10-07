#!/bin/sh
set -eux
pwd
#TODO: remove temporary switch to spark-vito-4_0_1

rm -f /opt/geotrellis-dependencies-static.jar

yum remove -y spark-bin
yum install -y hdf5 spark-vito-4_0_1
ln -sf /opt/spark4_0_1 /usr/local/spark
mkdir /eodata
chown jenkins /eodata
runuser jenkins -c '
set -eux
source ./env.sh
python3 scripts/get-jars.py --force-download --python-version ${PYTHON_VERSION:-3.8} jars
'

mkdir -p pytest-tmp
chown jenkins pytest-tmp
