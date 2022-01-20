#!/usr/bin/env bash

set -eo pipefail

export SPARK_HOME="$(venv/bin/find_spark_home.py)"
export OPENEO_CATALOG_FILES="layercatalog.json"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"  # TODO: pass as sensitive parameters from Nifi instead
export AWS_SECRET_ACCESS_KEY="!!!"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export OPENEO_VENV_ZIP="https://artifactory.vgt.vito.be/auxdata-public/openeo/openeo-20211110-69.zip"

if [ "$#" -lt 1 ]; then
    >&2 echo "Usage: $0 <task JSON>"
    exit 1
fi

task_json="$1"

extensions="$(bash geotrellis-extensions-jar.sh)"
classpath="$extensions:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"

venv/bin/python -m openeogeotrellis.async_task --py4j-classpath "$classpath" --task "$task_json"
