#!/usr/bin/env bash

# Called from clustered Nifi to run the nightly cleaner in a Docker container, for example:
# bash cleaner-docker.sh vito-docker-private-dev.artifactory.vgt.vito.be/openeo-yarn /openeo/dev/jobs '???' '!!!'

set -eo pipefail

if [ "$#" -lt 4 ]; then
    >&2 echo "Usage: $0 <Docker image> <batch jobs Zookeeper root path> <AWS access key ID> <AWS secret access key>"
    exit 1
fi

docker_image=$1
batch_jobs_zookeeper_root_path=$2
aws_access_key_id=$3
aws_secret_access_key=$4

docker pull $docker_image

docker run \
--rm \
--entrypoint bash \
-v /usr/hdp/:/usr/hdp/:ro \
-v /data/projects/OpenEO/:/data/projects/OpenEO/:rw \
-v /tmp_epod/openeo_assembled/:/tmp_epod/openeo_assembled/:rw \
-v /tmp_epod/openeo_collecting/:/tmp_epod/openeo_collecting/:rw \
--mount type=bind,source=/tmp/cleaner-entrypoint.sh,target=/opt/cleaner-entrypoint.sh,readonly \
$docker_image \
/opt/cleaner-entrypoint.sh "$batch_jobs_zookeeper_root_path" "$(hadoop classpath)" "$aws_access_key_id" "$aws_secret_access_key"
