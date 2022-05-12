#!/usr/bin/env bash

# run with: scl enable rh-python38 -- bash job_tracker.sh
# note: set BATCH_JOBS_ZOOKEEPER_ROOT_PATH and ASYNC_TASK_HANDLER_ENV, if necessary

set -eo pipefail

unalias python 2> /dev/null || true

export SPARK_HOME="/opt/spark3_2_0"
export PYTHONPATH="venv/lib/python3.8/site-packages:${SPARK_HOME}/python"
export HADOOP_CONF_DIR="/etc/hadoop/conf"

export KRB5CCNAME=/tmp/krb5cc_openeo
kinit -kt openeo-deploy/mep/openeo.keytab openeo@VGT.VITO.BE

python -m openeogeotrellis.job_tracker
