#!/usr/bin/env bash

# Called from clustered Nifi to run an async_task in a Docker container, for example:
# bash async_task-docker.sh vito-docker-private-dev.artifactory.vgt.vito.be/openeo-yarn eyJ0YXNrX2lkIjoiZGVsZXRlX2JhdGNoX3Byb2Nlc3NfZGVwZW5kZW5jeV9zb3VyY2VzIiwiYXJndW1lbnRzIjp7ImJhdGNoX2pvYl9pZCI6ImFiYzEyMyIsInVzZXJfaWQiOiJ2ZGJvc2NoaiIsImRlcGVuZGVuY3lfc291cmNlcyI6WyJzMzovL29wZW5lby1zZW50aW5lbGh1Yi9zb21lX3Vua25vd25fcHJlZml4Il19fQ== '???' '!!!'

set -eo pipefail

if [ "$#" -lt 4 ]; then
    >&2 echo "Usage: $0 <Docker image> <task JSON, base 64 encoded> <AWS access key ID> <AWS secret access key>"
    exit 1
fi

docker_image=$1
task_json_base64=$2
aws_access_key_id=$3
aws_secret_access_key=$4

task_json=$(base64 -d - <<< $task_json_base64)

docker pull $docker_image

docker run \
--rm \
--entrypoint bash \
-v /usr/hdp/:/usr/hdp/:ro \
--mount type=bind,source=/tmp/async_task-entrypoint.sh,target=/opt/async_task-entrypoint.sh,readonly \
$docker_image \
/opt/async_task-entrypoint.sh "$task_json" "$(hadoop classpath)" "$aws_access_key_id" "$aws_secret_access_key"

