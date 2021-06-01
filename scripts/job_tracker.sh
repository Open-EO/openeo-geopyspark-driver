#!/usr/bin/env bash

set -eo pipefail

source venv36/bin/activate

export SPARK_HOME=$(find_spark_home.py)
export OPENEO_CATALOG_FILES="venv36/layercatalog.json"
export AWS_ACCESS_KEY_ID="???"
export AWS_SECRET_ACCESS_KEY="!!!"

python -m openeogeotrellis.job_tracker 2>&1
