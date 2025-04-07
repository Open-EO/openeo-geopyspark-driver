#!/usr/bin/env bash

set -eo pipefail

extensions_jars=($(ls -1 geotrellis-extensions-*.jar))

if [[ ${#extensions_jars[@]} -gt 1 ]]; then
  >&2 echo "Found multiple geotrellis-extensions jars - cannot decide which one to use!"
  exit 1
else
  echo "${extensions_jars[0]}"
fi
