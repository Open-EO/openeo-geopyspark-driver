#!/bin/sh
mkdir -p jars
mvn dependency:copy -Dartifact=org.openeo:geotrellis-extensions:1.1.0-SNAPSHOT -DoutputDirectory=jars
jar = "geotrellis-backend-assembly-0.4.2.jar"
curl -L https://github.com/locationtech-labs/geopyspark/releases/download/v0.4.2/$jar -o jars/$jar