#!/usr/bin/env bash

# Called from clustered Nifi to run the job tracker in a Docker container, for example:
# bash job_tracker-docker.sh vito-docker-private-dev.artifactory.vgt.vito.be/openeo-yarn:20220602-239 /openeo/dev/jobs dev

set -eo pipefail

if [ "$#" -lt 3 ]; then
    >&2 echo "Usage: $0 <Docker image> <batch jobs Zookeeper root path> <async task handler environment>"
    exit 1
fi

docker_image=$1
batch_jobs_zookeeper_root_path=$2
async_task_handler_env=$3

docker pull $docker_image

docker run \
--rm \
--entrypoint bash \
-v /usr/hdp/:/usr/hdp/:ro \
-v /etc/hadoop/conf/:/etc/hadoop/conf/:ro \
-v /data/projects/OpenEO/:/data/projects/OpenEO/:ro \
--mount type=bind,source=/tmp/job_tracker-entrypoint.sh,target=/opt/job_tracker-entrypoint.sh,readonly \
--mount type=bind,source=/tmp/openeo.keytab,target=/opt/openeo.keytab,readonly \
--mount type=bind,source=/etc/krb5.conf,target=/etc/krb5.conf,readonly \
--mount type=bind,source=/usr/bin/yarn,target=/usr/bin/yarn,readonly \
--user root \
$docker_image \
/opt/job_tracker-entrypoint.sh $batch_jobs_zookeeper_root_path $async_task_handler_env

