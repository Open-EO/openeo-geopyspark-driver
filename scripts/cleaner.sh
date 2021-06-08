#!/usr/bin/env bash

set -eo pipefail

source venv36/bin/activate

export SPARK_HOME=$(find_spark_home.py)
export OPENEO_CATALOG_FILES="venv36/layercatalog.json"

python -m openeogeotrellis.cleaner 2>&1
