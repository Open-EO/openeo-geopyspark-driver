#!/usr/bin/env bash

# run with: scl enable rh-python38 -- bash async_task.sh <task JSON, base 64 encoded>
# note: scl enable strips double quotes in command arguments (https://bugzilla.redhat.com/show_bug.cgi?id=1248418)

set -eo pipefail

if [ "$#" -lt 1 ]; then
    >&2 echo "Usage: $0 <task JSON, base 64 encoded>"
    exit 1
fi

task_json_base64="$1"
task_json=$(base64 -d - <<< $task_json_base64)

unalias python 2> /dev/null || true

pyfiles=$(python openeo-deploy/mep/get-py-files.py)

export PYTHONPATH="venv/lib/python3.8/site-packages"
export SPARK_HOME="$(python venv/bin/find_spark_home.py)"
export OPENEO_CATALOG_FILES="layercatalog.json"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"  # TODO: pass as sensitive parameters from Nifi instead
export AWS_SECRET_ACCESS_KEY="!!!"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export OPENEO_VENV_ZIP="https://artifactory.vgt.vito.be/auxdata-public/openeo/openeo-venv38-20220112-220.zip"
export OPENEO_SPARK_SUBMIT_PY_FILES="$pyfiles"

extensions="$(bash geotrellis-extensions-jar.sh)"
classpath="$extensions:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"

python -m openeogeotrellis.async_task --py4j-classpath "$classpath" --py4j-jarpath "venv/share/py4j/py4j0.10.9.2.jar" --task "$task_json"
