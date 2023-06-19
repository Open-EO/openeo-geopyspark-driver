#!/usr/bin/env bash

# Nightly cleaner script for deployments in k8s.

set -eo pipefail

fail_if_not_set () {
  local -n envars=$1

  for envar in "${envars[@]}"; do
    if [ -z "${!envar}" ]; then
      >&2 echo "Environment variable ${envar} is not set"
      exit 1
    fi
  done
}

expected_envars=(
"SPARK_HOME"
"ZOOKEEPERNODES"
"BATCH_JOBS_ZOOKEEPER_ROOT_PATH"
"SWIFT_URL"
"AWS_ACCESS_KEY_ID"
"AWS_SECRET_ACCESS_KEY"
)
fail_if_not_set expected_envars

export KUBE="true"

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
