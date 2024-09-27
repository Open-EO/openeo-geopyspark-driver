#!/bin/bash

if [ -z "$1" ]; then
    echo "First argument should be the path to the process graph."
    echo "'local_batch_job.sh path/to/process_graph.json'"
    exit 1
fi
if [ ! -f "$1" ]; then
    echo "File not found: $1"
    exit 1
fi

parent_folder="$(dirname "$1")"

# --entrypoint /bin/bash
# Specify user otherwise output files are root
# /etc/passwd to avoid "whoami: cannot find name for user ID"
# Opening a /vsi file with the netCDF driver requires Linux userfaultfd to be available. If running from Docker, --security-opt seccomp=unconfined might be needed.
# mount is used to read process_graph and write results
# Avoid -i, to avoid "the input device is not a TTY"
# --network host can fix internet connection when the host machine is behind a VPN
docker run -t \
    --user "$(id -g):$(id -u)" \
    -v /etc/passwd:/etc/passwd:ro \
    --security-opt seccomp=unconfined \
    -v "$parent_folder":/opt/docker_mount \
    --network host \
    openeo_docker_local
