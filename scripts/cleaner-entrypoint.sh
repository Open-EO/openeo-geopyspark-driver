#!/usr/bin/env bash

set -eo pipefail

if [ "$#" -lt 4 ]; then
    >&2 echo "Usage: $0 <batch jobs Zookeeper root path> <Hadoop classpath> <AWS access key ID> <AWS secret access key>"
    exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
    >&2 echo "Environment variable SPARK_HOME is not set"
    exit 1
fi

batch_jobs_zookeeper_root_path=$1
hadoop_classpath=$2
aws_access_key_id=$3
aws_secret_access_key=$4

export PYTHONPATH="/opt/venv/lib64/python3.8/site-packages"
export BATCH_JOBS_ZOOKEEPER_ROOT_PATH=$batch_jobs_zookeeper_root_path
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID=$aws_access_key_id
export AWS_SECRET_ACCESS_KEY=$aws_secret_access_key

classpath="geotrellis-extensions-static.jar:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':'):$hadoop_classpath"
py4j_jarpath="$(find venv/share/py4j -name 'py4j*.jar')"

python3 -m openeogeotrellis.cleaner --py4j-classpath "$classpath" --py4j-jarpath "$py4j_jarpath" 2>&1
