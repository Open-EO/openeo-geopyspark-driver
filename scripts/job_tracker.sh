#!/usr/bin/env bash

set -eo pipefail

export SPARK_HOME="$(venv/bin/find_spark_home.py)"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

venv/bin/python -m openeogeotrellis.job_tracker
