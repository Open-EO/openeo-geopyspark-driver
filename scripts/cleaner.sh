#!/usr/bin/env bash

# run with: scl enable rh-python38 -- bash cleaner.sh
# note: set BATCH_JOBS_ZOOKEEPER_ROOT_PATH, if necessary

set -eo pipefail

unalias python 2> /dev/null || true

export SPARK_HOME="/opt/spark3_2_0"
export PYTHONPATH="venv/lib/python3.8/site-packages:${SPARK_HOME}/python"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"  # TODO: pass as sensitive parameters from Nifi instead
export AWS_SECRET_ACCESS_KEY="!!!"

extensions="$(bash geotrellis-extensions-jar.sh)"
classpath="$extensions:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':'):$(hadoop classpath)"
py4j_jarpath="$(find venv/share/py4j -name 'py4j*.jar')"

python -m openeogeotrellis.cleaner --py4j-classpath "$classpath" --py4j-jarpath "$py4j_jarpath" 2>&1
