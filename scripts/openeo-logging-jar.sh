#!/usr/bin/env bash

set -eo pipefail

logging_jars=($(ls -1 openeo-logging-*.jar))

if [[ ${#logging_jars[@]} -gt 1 ]]; then
  >&2 echo "Found multiple openeo-logging jars - cannot decide which one to use!"
  exit 1
else
  echo "${logging_jars[0]}"
fi
