#!/usr/bin/env bash

set -eo pipefail

if [ "$#" -lt 2 ]; then
    >&2 echo "Usage: $0 <batch jobs Zookeeper root path> <async task handler environment> [run ID]"
    exit 1
fi

# Example values: "/openeo/jobs" (for prod), "/openeo/dev/jobs" (for dev), ...
batch_jobs_zookeeper_root_path=$1
# Example values: "prod", "dev", "integrationtests"
deploy_env=$2
async_task_handler_env=$2
run_id=$3

if [ -n "${run_id}" ]; then
  run_id_arg="--run-id $run_id"
else
  run_id_arg=""
fi

# Note: this `./openeo.keytab` is expected to be mounted in docker container by NiFi processor.
keytab="openeo.keytab"

kinit -kt $keytab openeo@VGT.VITO.BE

export BATCH_JOBS_ZOOKEEPER_ROOT_PATH=$batch_jobs_zookeeper_root_path
export ASYNC_TASK_HANDLER_ENV=$async_task_handler_env
export PYARROW_IGNORE_TIMEZONE=1
export OPENEO_ENV="$deploy_env"

/opt/venv/bin/python -m openeogeotrellis.job_tracker_v2 \
  --app-cluster yarn \
  --zk-job-registry-root-path="$batch_jobs_zookeeper_root_path" \
  --keytab "$keytab" \
  --rotating-log logs/job_tracker_python_"$deploy_env".log \
  $run_id_arg
