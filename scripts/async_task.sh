#!/usr/bin/env bash

set -eo pipefail

export SPARK_HOME="$(venv/bin/find_spark_home.py)"
export OPENEO_CATALOG_FILES="layercatalog.json"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"
export AWS_SECRET_ACCESS_KEY="!!!"

if [ "$#" -lt 1 ]; then
    >&2 echo "Usage: $0 <task JSON>"
    exit 1
fi

task_json="$1"

extensions="$(bash geotrellis-extensions-jar.sh)"
classpath="$extensions:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"

venv/bin/python -m openeogeotrellis.async_task --py4j-classpath "$classpath" --task "$task_json" 2>&1
