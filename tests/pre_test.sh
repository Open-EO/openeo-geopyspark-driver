#!/bin/sh
yum install -y hdf5
mkdir /eodata
chown jenkins /eodata
runuser jenkins -c '
python3 scripts/get-jars.py --force-download jars
'