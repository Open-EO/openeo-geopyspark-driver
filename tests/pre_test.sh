#!/bin/sh
set -eux
pwd

yum install -y hdf5
mkdir /eodata
chown jenkins /eodata
runuser jenkins -c '
source ./env.sh
python3 scripts/get-jars.py --force-download --python-version ${PYTHON_VERSION:-3.8} jars
'

mkdir -p pytest-tmp
chown jenkins pytest-tmp
