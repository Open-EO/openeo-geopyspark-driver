#!/bin/sh
yum install -y hdf5
runuser jenkins -c '
python3 scripts/get-jars.py --force-download jars
'