#!/usr/bin/env bash

# Called from job_tracker-docker.sh to do the actual work in a Docker container.

set -eo pipefail

if [ "$#" -lt 2 ]; then
    >&2 echo "Usage: $0 <batch jobs Zookeeper root path> <async task handler environment>"
    exit 1
fi

batch_jobs_zookeeper_root_path=$1
async_task_handler_env=$2

kinit -kt openeo.keytab openeo@VGT.VITO.BE

export PYTHONPATH="/opt/venv/lib64/python3.8/site-packages"
export BATCH_JOBS_ZOOKEEPER_ROOT_PATH=$batch_jobs_zookeeper_root_path
export ASYNC_TASK_HANDLER_ENV=$async_task_handler_env
export PYARROW_IGNORE_TIMEZONE=1

python3 -m openeogeotrellis.job_tracker
