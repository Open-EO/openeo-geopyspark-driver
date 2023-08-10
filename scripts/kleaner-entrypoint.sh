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

classpath="/opt/geotrellis-extensions-static.jar:$(find "$SPARK_HOME/jars" -name '*.jar' | tr '\n' ':')"

/opt/venv/bin/python -m openeogeotrellis.cleaner \
--py4j-classpath "$classpath" \
--min-age 90
