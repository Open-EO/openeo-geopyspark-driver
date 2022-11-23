#!/bin/sh
yum install -y hdf5
runuser jenkins -c '
./scripts/get-jars.py --force-download jars
'