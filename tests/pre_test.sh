#!/bin/sh
runuser jenkins -c '
mkdir -p jars
cd jars
curl -L -O -C - https://artifactory.vgt.vito.be/libs-release-public/org/openeo/geotrellis-extensions/1.1.0/geotrellis-extensions-1.1.0.jar
curl -L -O -C - https://github.com/locationtech-labs/geopyspark/releases/download/v0.4.2/geotrellis-backend-assembly-0.4.2.jar
cd ..'