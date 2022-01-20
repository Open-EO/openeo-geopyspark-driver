#!/usr/bin/env bash

# run with: scl enable rh-python38 -- bash job_tracker.sh

set -eo pipefail

unalias python 2> /dev/null || true

export PYTHONPATH="venv/lib/python3.8/site-packages"
export SPARK_HOME="$(python venv/bin/find_spark_home.py)"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

python -m openeogeotrellis.job_tracker
