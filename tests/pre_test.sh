#!/bin/sh
set -eux
pwd

rm -f /opt/geotrellis-dependencies-static.jar

yum remove -y hdf5
yum install -y hdf5
mkdir /eodata
chown jenkins /eodata
runuser jenkins -c '
set -eux
source ./env.sh
python3 scripts/get-jars.py --force-download --python-version ${PYTHON_VERSION:-3.8} jars
'

mkdir -p pytest-tmp
chown jenkins pytest-tmp
