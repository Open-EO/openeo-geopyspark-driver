#!/bin/sh
runuser jenkins -c '
mkdir -p jars
cd jars
curl -L -O -C - https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/1.4.0-SNAPSHOT/geotrellis-extensions-1.4.0-SNAPSHOT.jar
curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.5-openeo.jar
cd ..'