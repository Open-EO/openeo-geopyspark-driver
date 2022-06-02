#!/usr/bin/env bash

# Called from clustered Nifi to run the nightly cleaner in a Docker container, for example:
# AWS_ACCESS_KEY_ID='???' AWS_SECRET_ACCESS_KEY='!!!' BATCH_JOBS_ZOOKEEPER_ROOT_PATH=/openeo/dev/jobs bash cleaner-docker.sh

set -eo pipefail

# TODO: take and pass on regular arguments instead of environment variables?

# TODO: drop the version
# TODO: take the image name from Nifi instead (allows us to take a particular version + support prod image)
docker_image="vito-docker-private-dev.artifactory.vgt.vito.be/openeo-yarn:20220602-238"

docker pull $docker_image

docker run \
--rm \
--entrypoint bash \
--env BATCH_JOBS_ZOOKEEPER_ROOT_PATH=${BATCH_JOBS_ZOOKEEPER_ROOT_PATH} \
--env AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
--env AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
--env HADOOP_CLASSPATH=$(hadoop classpath) \
-v /usr/hdp/:/usr/hdp/:ro \
-v /data/projects/OpenEO/:/data/projects/OpenEO/:rw \
--mount type=bind,source=/tmp/cleaner-entrypoint.sh,target=/opt/cleaner-entrypoint.sh,readonly \
$docker_image \
/opt/cleaner-entrypoint.sh
