#!/bin/bash
set -euxo pipefail

# Sets up a small local Elasticsearch instance + a Filebeat DaemonSet in the local k3d cluster, so that
# batch job driver/executor pod logs get shipped to it -- mirroring (in miniature) how production ships
# pod logs to a central Elasticsearch cluster (see `openeogeotrellis/logs.py`'s `elasticsearch_logs()`,
# which expects log documents with a `job_id` (keyword), `tags` (keyword, containing "openeo"),
# `levelname` (keyword), `@timestamp` (date) and `log.offset` (long, used as a tie-breaker for sorting/
# pagination) -- all of which Filebeat's `container` input + a `decode_json_fields` processor naturally
# provide, given that both the Java (log4j2 `JsonTemplateLayout`) and Python
# (`openeo_driver.util.logging`) sides already write their container stdout as JSON lines.
#
# Run this once, after `setup_calrissian_cwl_k8.sh`. `local.py` (via `LOGGING_ES_HOSTS`, see
# `setup_environment()`) then talks to this Elasticsearch instance directly, for `GET /jobs/{id}/logs`.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

K3D_CLUSTER_NAME="${K3D_CLUSTER_NAME:-$(kubectl config current-context 2>/dev/null | sed -n 's/^k3d-//p')}"
K3D_DOCKER_NETWORK="k3d-${K3D_CLUSTER_NAME}"

# The k3d cluster's nodes are Docker containers on their own bridge network; the network's gateway IP
# is reachable both from the host (where `local.py`/Elasticsearch run) and from inside the cluster
# (where Filebeat runs) -- so it's used here to let Filebeat (in the cluster) ship logs to Elasticsearch
# (published on the host).
ES_HOST_IP="$(docker network inspect "$K3D_DOCKER_NETWORK" --format '{{(index .IPAM.Config 0).Gateway}}')"

# Run a single-node Elasticsearch, with security disabled for simplicity, attached to the k3d network so
# Filebeat can reach it via $ES_HOST_IP, and published on localhost:9200 so `local.py` (on the host) can
# query it directly too.
docker rm -f openeo-local-es >/dev/null 2>&1 || true
docker run -d --name openeo-local-es \
    -p 9200:9200 \
    --network "$K3D_DOCKER_NETWORK" \
    -e discovery.type=single-node \
    -e xpack.security.enabled=false \
    -e xpack.security.http.ssl.enabled=false \
    -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
    docker.elastic.co/elasticsearch/elasticsearch:8.11.1

# Wait for Elasticsearch to come up.
for i in $(seq 1 60); do
    curl -sf "http://localhost:9200" >/dev/null 2>&1 && break
    sleep 2
done
curl -sf "http://localhost:9200" >/dev/null

# Index template: map the fields `elasticsearch_logs()` filters/sorts on as `keyword`/`date`/`long`
# (dynamic mapping would otherwise map `job_id` etc. as analyzed `text`, breaking exact `term` filters).
# Also disable replicas: with only one node, unassigned replica shards would leave the index red/
# unsearchable.
curl -sf -X PUT "http://localhost:9200/_index_template/openeo-logs" -H 'Content-Type: application/json' -d '{
  "index_patterns": ["openeo-*-index-1m*"],
  "template": {
    "settings": {"number_of_replicas": 0},
    "mappings": {
      "properties": {
        "job_id": {"type": "keyword"},
        "user_id": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "levelname": {"type": "keyword"},
        "name": {"type": "keyword"},
        "message": {"type": "text"},
        "@timestamp": {"type": "date"},
        "log": {"properties": {"offset": {"type": "long"}}}
      }
    }
  }
}'

# On a shared/busy host, overall disk usage can easily exceed Elasticsearch's default disk-based
# shard-allocation watermarks (85%/90%/95%), which would otherwise leave every index permanently
# unassigned ("red"), regardless of how little space Elasticsearch itself actually uses.
curl -sf -X PUT "http://localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d '{
  "persistent": {
    "cluster.routing.allocation.disk.threshold_enabled": true,
    "cluster.routing.allocation.disk.watermark.low": "99%",
    "cluster.routing.allocation.disk.watermark.high": "99%",
    "cluster.routing.allocation.disk.watermark.flood_stage": "99.5%"
  }
}'

# Deploy the Filebeat DaemonSet, pointed at Elasticsearch via the gateway IP computed above.
sed "s/172\.19\.0\.1:9200/${ES_HOST_IP}:9200/" "$SCRIPT_DIR/filebeat-daemonset.yaml" | kubectl apply -f -

kubectl -n logging rollout status daemonset/filebeat --timeout=120s
