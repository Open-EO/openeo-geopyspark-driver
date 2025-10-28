#!/bin/bas

set -x

# Determine container runtime
if command -v podman; then
    CONTAINER_CMD="podman"
elif command -v docker; then
    CONTAINER_CMD="docker"
else
    echo "Error: Neither Docker nor Podman is installed."
    exit 1
fi
echo "Using container runtime: $CONTAINER_CMD"


# start the webservice locally, using host network so all ports are available
$CONTAINER_CMD \
  run \
  --rm \
  --network=host \
  --env-file local_service.env  \
  --entrypoint=sh \
  vito-docker.artifactory.vgt.vito.be/openeo-base \
  -c '/opt/venv/bin/python /opt/venv/bin/openeo_local.py'
