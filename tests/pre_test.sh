#!/bin/sh
runuser jenkins -c '
mkdir -p jars
cd jars
curl -L -O -C - https://artifactory.vgt.vito.be/libs-release-public/org/openeo/geotrellis-extensions/1.1.0/geotrellis-extensions-1.1.0.jar
curl -L -O -C - https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.2.jar
cd ..'