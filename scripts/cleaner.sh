#!/usr/bin/env bash

# run with: scl enable rh-python38 -- bash cleaner.sh

set -eo pipefail

unalias python 2> /dev/null || true

export PYTHONPATH="venv/lib/python3.8/site-packages"
export SPARK_HOME="$(python venv/bin/find_spark_home.py)"
export AWS_REGION="eu-central-1"
export AWS_ACCESS_KEY_ID="???"  # TODO: pass as sensitive parameters from Nifi instead
export AWS_SECRET_ACCESS_KEY="!!!"

extensions="$(bash geotrellis-extensions-jar.sh)"
classpath="$extensions:$(find $SPARK_HOME/jars -name '*.jar' | tr '\n' ':')"

python -m openeogeotrellis.cleaner --py4j-classpath "$classpath" --py4j-jarpath "venv/share/py4j/py4j0.10.9.2.jar" 2>&1
