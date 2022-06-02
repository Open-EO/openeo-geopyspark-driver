#!/usr/bin/env bash

# Called from cleaner-docker.sh to do the actual work in a Docker container.

set -eo pipefail

if [ -z "${BATCH_JOBS_ZOOKEEPER_ROOT_PATH}" ]; then
    >&2 echo "Environment variable BATCH_JOBS_ZOOKEEPER_ROOT_PATH is not set"
    exit 1
fi

if [ -z "${AWS_ACCESS_KEY_ID}" ]; then
    >&2 echo "Environment variable AWS_ACCESS_KEY_ID is not set"
    exit 1
fi

if [ -z "${AWS_SECRET_ACCESS_KEY}" ]; then
    >&2 echo "Environment variable AWS_SECRET_ACCESS_KEY is not set"
    exit 1
fi

if [ -z "${HADOOP_CLASSPATH}" ]; then
    >&2 echo "Environment variable HADOOP_CLASSPATH is not set"
    exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
    >&2 echo "Environment variable SPARK_HOME is not set"
    exit 1
fi

export PYTHONPATH="/opt/venv/lib64/python3.8/site-packages"
export AWS_REGION="eu-central-1"

classpath="geotrellis-extensions-static.jar:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':'):$HADOOP_CLASSPATH"

python3 -m openeogeotrellis.cleaner --py4j-classpath "$classpath" --py4j-jarpath "venv/share/py4j/py4j0.10.9.2.jar" 2>&1
