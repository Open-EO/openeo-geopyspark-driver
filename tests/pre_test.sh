#!/bin/sh
mkdir -p jars
export jar="geotrellis-backend-assembly-0.4.2.jar"
cd jars
wget -nc https://artifactory.vgt.vito.be/libs-release-public/org/openeo/geotrellis-extensions/1.1.0/geotrellis-extensions-1.1.0.jar
wget -nc https://github.com/locationtech-labs/geopyspark/releases/download/v0.4.2/$jar
cd ..