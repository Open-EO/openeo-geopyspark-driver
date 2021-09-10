#!/usr/bin/env bash

set -eo pipefail

export SPARK_HOME=$(venv/bin/find_spark_home.py)
export OPENEO_CATALOG_FILES="layercatalog.json"

venv/bin/python -m openeogeotrellis.cleaner 2>&1
