#!/bin/sh
yum install -y hdf5
runuser jenkins -c '
mkdir -p jars
cd jars
rm *.jar
curl -f -L -O -C - https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.3.0_2.12-SNAPSHOT/geotrellis-extensions-2.3.0_2.12-SNAPSHOT.jar
curl -f -L -O -C - https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/openeo-logging/2.3.0_2.12-SNAPSHOT/openeo-logging-2.3.0_2.12-SNAPSHOT.jar
curl -f -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo_2.12.jar
cd ..'