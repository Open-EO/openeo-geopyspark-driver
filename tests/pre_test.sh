#!/bin/sh
yum info hdf5
yum install -y hdf5
runuser jenkins -c '
mkdir -p jars
cd jars
curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-extensions-2.0.0_2.12-SNAPSHOT.jar
curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo_2.12.jar
cd ..'