#!/usr/bin/env bash

set -eo pipefail

source venv/bin/activate

export SPARK_HOME="$(find_spark_home.py)"
export OPENEO_CATALOG_FILES="layercatalog.json"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"
export AWS_SECRET_ACCESS_KEY="!!!"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export OPENEO_VENV_ZIP="https://artifactory.vgt.vito.be/auxdata-public/openeo/dev/openeo-20210614-268.zip"

classpath="geotrellis-extensions-2.2.0-SNAPSHOT.jar:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"

python -m openeogeotrellis.job_tracker --py4j-classpath "$classpath"
