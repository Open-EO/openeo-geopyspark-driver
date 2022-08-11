#!/usr/bin/env bash

# run with: scl enable rh-python38 -- bash async_task.sh <task JSON, base 64 encoded>
# note: scl enable strips double quotes in command arguments (https://bugzilla.redhat.com/show_bug.cgi?id=1248418)
# note: set BATCH_JOBS_ZOOKEEPER_ROOT_PATH, if necessary

set -eo pipefail

if [ "$#" -lt 1 ]; then
    >&2 echo "Usage: $0 <task JSON, base 64 encoded>"
    exit 1
fi

task_json_base64="$1"
task_json=$(base64 -d - <<< $task_json_base64)

unalias python 2> /dev/null || true

pyfiles=$(python openeo-deploy/mep/get-py-files.py)

export SPARK_HOME="/opt/spark3_2_0"
export PYTHONPATH="venv/lib/python3.8/site-packages:${SPARK_HOME}/python"
export OPENEO_CATALOG_FILES="layercatalog.json"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"  # TODO: pass as sensitive parameters from Nifi instead
export AWS_SECRET_ACCESS_KEY="!!!"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export OPENEO_VENV_ZIP="https://artifactory.vgt.vito.be/auxdata-public/openeo/dev/openeo-venv38-20220512-194.zip"
export OPENEO_SPARK_SUBMIT_PY_FILES="$pyfiles"
export PYSPARK_PYTHON="$(which python)"
export YARN_CONTAINER_RUNTIME_DOCKER_IMAGE="vito-docker-private.artifactory.vgt.vito.be/python38-hadoop:latest"

extensions_jar="$(bash geotrellis-extensions-jar.sh)"
logging_jar="$(bash openeo-logging-jar.sh)"
classpath="$extensions_jar:$logging_jar:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':'):$(hadoop classpath)"
py4j_jarpath="$(find venv/share/py4j -name 'py4j*.jar')"

export KRB5CCNAME=/tmp/krb5cc_openeo
kinit -kt openeo-deploy/mep/openeo.keytab openeo@VGT.VITO.BE

python -m openeogeotrellis.async_task --py4j-classpath "$classpath" --py4j-jarpath "$py4j_jarpath" --task "$task_json"
