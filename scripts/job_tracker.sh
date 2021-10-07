#!/usr/bin/env bash

set -eo pipefail

export SPARK_HOME="$(venv/bin/find_spark_home.py)"
export OPENEO_CATALOG_FILES="layercatalog.json"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"
export AWS_SECRET_ACCESS_KEY="!!!"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export OPENEO_VENV_ZIP="https://artifactory.vgt.vito.be/auxdata-public/openeo/openeo-20210805-412.zip"

extensions="$(bash geotrellis-extensions-jar.sh)"
classpath="$extensions:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"

venv/bin/python -m openeogeotrellis.job_tracker --py4j-classpath "$classpath"
