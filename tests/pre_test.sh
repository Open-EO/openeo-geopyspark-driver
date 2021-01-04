#!/bin/sh
yum install -y hdf5
runuser jenkins -c '
mkdir -p jars
cd jars
if [ $(lsb_release -rs) = "8.2.2004" ]; then
  curl -L -O -C - https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.0.0_2.12-SNAPSHOT/geotrellis-extensions-2.0.0_2.12-SNAPSHOT.jar
  curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo_2.12.jar
else
 curl -L -O -C - https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.0.0-SNAPSHOT/geotrellis-extensions-2.0.0-SNAPSHOT.jar
 curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo.jar
fi
cd ..'