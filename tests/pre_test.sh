#!/bin/sh
yum install -y hdf5
runuser jenkins -c '
mkdir -p jars
cd jars
rm *.jar
curl -L -O -C - https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.2.0-SNAPSHOT/geotrellis-extensions-2.2.0-SNAPSHOT.jar
curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.7-openeo.jar
cd ..'