#!/usr/bin/env bash

set -eo pipefail

if [ "$#" -lt 6 ]; then
    >&2 echo "Usage: $0 <task JSON> <batch jobs Zookeeper root path> <Hadoop classpath> <AWS access key ID> <AWS secret access key> <batch job Docker image>"
    exit 1
fi

if [ -z "${SPARK_HOME}" ]; then
    >&2 echo "Environment variable SPARK_HOME is not set"
    exit 1
fi

if [ -z "${HADOOP_CONF_DIR}" ]; then
    >&2 echo "Environment variable HADOOP_CONF_DIR is not set"
    exit 1
fi

task_json=$1
batch_jobs_zookeeper_root_path=$2
aws_access_key_id=$4
aws_secret_access_key=$5
batch_job_docker_image=$6
keytab="openeo.keytab"

export OPENEO_CATALOG_FILES="layercatalog.json"
export BATCH_JOBS_ZOOKEEPER_ROOT_PATH=$batch_jobs_zookeeper_root_path
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID=$aws_access_key_id
export AWS_SECRET_ACCESS_KEY=$aws_secret_access_key
export OPENEO_SPARK_SUBMIT_PY_FILES="$(python3 /opt/get-py-files.py)"
export YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=$batch_job_docker_image
export PYARROW_IGNORE_TIMEZONE=1

kinit -kt $keytab openeo@VGT.VITO.BE

classpath="geotrellis-dependencies-static.jar:geotrellis-extensions-static.jar:openeo-logging-static.jar:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"
py4j_jarpath="$(find venv/share/py4j -name 'py4j*.jar')"

/opt/venv/bin/python -m openeogeotrellis.async_task --py4j-classpath "$classpath" --py4j-jarpath "$py4j_jarpath" --keytab "$keytab" --task "$task_json"
