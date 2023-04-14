#!/bin/sh
set -eux
pwd

yum install -y hdf5
mkdir /eodata
chown jenkins /eodata
runuser jenkins -c '
python3 scripts/get-jars.py --force-download jars
'

mkdir -p pytest-tmp
chown jenkins pytest-tmp
