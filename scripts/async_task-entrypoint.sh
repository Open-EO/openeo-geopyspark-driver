#!/usr/bin/env bash

# Called from async_task-docker.sh to do the actual work in a Docker container.

set -eo pipefail

if [ "$#" -lt 4 ]; then
    >&2 echo "Usage: $0 <task JSON> <Hadoop classpath> <AWS access key ID> <AWS secret access key>"
    exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
    >&2 echo "Environment variable SPARK_HOME is not set"
    exit 1
fi

task_json=$1
hadoop_classpath=$2
aws_access_key_id=$3
aws_secret_access_key=$4

export PYTHONPATH="/opt/venv/lib64/python3.8/site-packages"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID=$aws_access_key_id
export AWS_SECRET_ACCESS_KEY=$aws_secret_access_key

classpath="geotrellis-extensions-static.jar:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':'):$hadoop_classpath"

python3 -m openeogeotrellis.async_task --py4j-classpath "$classpath" --py4j-jarpath "venv/share/py4j/py4j0.10.9.2.jar" --task "$task_json"

