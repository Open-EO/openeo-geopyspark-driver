#!/usr/bin/env bash

# Nightly cleaner script for deployments in k8s.

set -eo pipefail

# TODO: fail fast for missing environment variables

# TODO: wasn't there a function in py4j to find this jar?
py4j_jarpath="$(find venv/share/py4j -name 'py4j*.jar')"
classpath="/opt/geotrellis-extensions-static.jar:$(find "$SPARK_HOME/jars" -name '*.jar' | tr '\n' ':')"

# TODO: drop --user
# TODO: fix/drop --min-age
# TODO: drop --dry-run
/opt/venv/bin/python -m openeogeotrellis.cleaner \
--py4j-jarpath "$py4j_jarpath" \
--py4j-classpath "$classpath" \
--user df7ea45d-ecc4-453f-8af9-de8cfb1058b1 \
--min-age 7 \
--dry-run
