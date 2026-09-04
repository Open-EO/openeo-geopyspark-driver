#!/bin/bash
# Prepares the local environment/tooling required by ./setup_calrissian_cwl_k8.sh
# and (re)creates a local k3d cluster with the NodePort range it needs.
#
# Usage:
#   ./setup_calrissian_cwl_k8_env.sh [cluster-name]
#
# Run this once (or whenever you need a fresh cluster) before
# ./setup_calrissian_cwl_k8.sh.
set -euxo pipefail

CLUSTER_NAME="${1:-calrissian-demo}"

# --- Install missing CLI tools (kubectl/k3d are assumed present already in
# most dev images; helm/aws-cli/docker-cli are the ones commonly missing). ---
if ! command -v docker >/dev/null 2>&1; then
    apt-get update -qq
    apt-get install -y -qq docker.io
fi

if ! command -v helm >/dev/null 2>&1; then
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 -o /tmp/get_helm.sh
    chmod +x /tmp/get_helm.sh
    /tmp/get_helm.sh
fi

if ! command -v aws >/dev/null 2>&1; then
    tmpdir=$(mktemp -d)
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$tmpdir/awscliv2.zip"
    unzip -q -o "$tmpdir/awscliv2.zip" -d "$tmpdir"
    "$tmpdir/aws/install"
    rm -rf "$tmpdir"
fi

if ! command -v k3d >/dev/null 2>&1; then
    curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
fi

# --- Helm repo required by setup_calrissian_cwl_k8.sh ---
helm repo add yandex-s3 https://yandex-cloud.github.io/k8s-csi-s3/charts
helm repo update yandex-s3

# --- (Re)create the k3d cluster with the NodePort range (30000-30001)
# published, as required by setup_calrissian_cwl_k8.sh. ---
if k3d cluster list "$CLUSTER_NAME" >/dev/null 2>&1; then
    k3d cluster delete "$CLUSTER_NAME"
fi

# `k3d cluster delete` sometimes leaves the cluster's docker network behind
# (e.g. if it was removed/recreated out of band, or a previous run was
# interrupted). Re-using such an orphaned network can leave it with stale
# iptables forwarding rules, silently dropping traffic between the
# serverlb and the k3s API server and making the cluster appear to hang.
# Remove it so docker regenerates the network (and its iptables rules)
# from scratch.
if docker network inspect "k3d-${CLUSTER_NAME}" >/dev/null 2>&1; then
    docker network rm "k3d-${CLUSTER_NAME}" >/dev/null 2>&1 || true
fi

k3d cluster create "$CLUSTER_NAME" -p "30000-30001:30000-30001@server:0"

# If this script itself runs inside a container that shares the host's
# docker socket (e.g. a dev container started with
# `-v /var/run/docker.sock:/var/run/docker.sock`), that container is not
# on the k3d cluster's docker network by default, so it can't reach the
# k3d API server or its NodePort range. Attach it to the cluster network
# so `kubectl`/`aws` calls from inside this container work, matching the
# NODE_IP fallback added to setup_calrissian_cwl_k8.sh.
SELF_CONTAINER_ID="$(cat /etc/hostname 2>/dev/null || true)"
if [ -f /.dockerenv ] && [ -n "$SELF_CONTAINER_ID" ]; then
    docker network connect "k3d-${CLUSTER_NAME}" "$SELF_CONTAINER_ID" 2>/dev/null || true
    # Point kubectl at the serverlb container directly instead of the
    # host-published "0.0.0.0:<port>" address, which isn't reachable from
    # inside this container's own network namespace.
    KUBECONFIG_FILE="${KUBECONFIG:-$HOME/.kube/config}"
    sed -i "s|server: https://0.0.0.0:[0-9]*|server: https://k3d-${CLUSTER_NAME}-serverlb:6443|" "$KUBECONFIG_FILE"
fi

kubectl cluster-info
