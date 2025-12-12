#!/bin/bash
set -euxo pipefail

# minikube delete && minikube start  # clean up previous sessions

NAMESPACE_NAME=calrissian-demo-project
kubectl create namespace "$NAMESPACE_NAME" --dry-run=client -o yaml | kubectl apply -f -
helm install csi-s3 yandex-s3/csi-s3 -n calrissian-demo-project
kubectl apply -f calrissian-local-minio.yaml
kubectl wait -n calrissian-demo-project --for=condition=available --timeout=300s deployment/minio
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin aws --endpoint-url "http://$(minikube ip):30000" s3 mb s3://calrissian
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin aws --endpoint-url "http://$(minikube ip):30000" s3api put-bucket-policy --bucket calrissian --policy '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::calrissian/*"
    }
  ]
}'

# For debugging, run the following:
# open "http://$(minikube ip):30001"
# minikube dashboard &

# Roles and permissions
kubectl \
    --namespace="$NAMESPACE_NAME" \
    create role \
    pod-manager-role \
    --verb=create,patch,delete,list,watch \
    --resource=pods

kubectl \
    --namespace="$NAMESPACE_NAME" \
    create role \
    log-reader-role \
    --verb=get,list \
    --resource=pods/log

kubectl \
    --namespace="$NAMESPACE_NAME" \
    create role \
    job-manager-role \
    --verb=create,list,get,delete \
    --resource=jobs

kubectl \
    --namespace="$NAMESPACE_NAME" \
    create role \
    pvc-reader-role \
    --verb=list,get \
    --resource=persistentvolumeclaims


# And attach roles to the service account in that namespace:

kubectl \
    --namespace="$NAMESPACE_NAME" \
    create rolebinding \
    pod-manager-default-binding \
    --role=pod-manager-role \
    --serviceaccount=${NAMESPACE_NAME}:default

kubectl \
    --namespace="$NAMESPACE_NAME" \
    create rolebinding \
    log-reader-default-binding \
    --role=log-reader-role \
    --serviceaccount=${NAMESPACE_NAME}:default


# Also attach roles to the appropriate service accounts (in other namespaces)

ENV=dev
# ENV=staging
# ENV=prod
OTHER_NAMESPACE_NAME=spark-jobs-$ENV

for SERVICE_ACCOUNT_NAME in openeo batch-jobs
do
    kubectl \
        --namespace="$NAMESPACE_NAME" \
        create rolebinding \
        job-manager-${OTHER_NAMESPACE_NAME}-${SERVICE_ACCOUNT_NAME} \
        --role=job-manager-role \
        --serviceaccount=${OTHER_NAMESPACE_NAME}:${SERVICE_ACCOUNT_NAME}

    kubectl \
        --namespace="$NAMESPACE_NAME" \
        create rolebinding \
        pvc-reader-${OTHER_NAMESPACE_NAME}-${SERVICE_ACCOUNT_NAME} \
        --role=pvc-reader-role \
        --serviceaccount=${OTHER_NAMESPACE_NAME}:${SERVICE_ACCOUNT_NAME}
done

# Create a `StorageClass` `csi-s3-calrissian` for the volumes used by Calrissian.
# For example with `geesefs` from `ru.yandex.s3.csi`:
cat <<EOF | kubectl create -f -
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: csi-s3-calrissian
provisioner: ru.yandex.s3.csi
volumeBindingMode: Immediate
parameters:
  mounter: geesefs
  # you can set mount options here, for example limit memory cache size (recommended)
  options: "--memory-limit 1000 --dir-mode 0777 --file-mode 0666 --no-systemd --fsync-on-close --stat-cache-ttl 0"
  # to use an existing bucket, specify it here:
  bucket: calrissian
  csi.storage.k8s.io/provisioner-secret-name: csi-s3-secret
  csi.storage.k8s.io/provisioner-secret-namespace: csi-s3
  csi.storage.k8s.io/controller-publish-secret-name: csi-s3-secret
  csi.storage.k8s.io/controller-publish-secret-namespace: csi-s3
  csi.storage.k8s.io/node-stage-secret-name: csi-s3-secret
  csi.storage.k8s.io/node-stage-secret-namespace: csi-s3
  csi.storage.k8s.io/node-publish-secret-name: csi-s3-secret
  csi.storage.k8s.io/node-publish-secret-namespace: csi-s3
EOF


# For storage, create the `PersistentVolumeClaim`s for the input, tmp and output volumes.

cat <<EOF | kubectl create -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: calrissian-input-data
  namespace: calrissian-demo-project
spec:
  accessModes:
    - ReadWriteOnce
    - ReadOnlyMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: csi-s3-calrissian
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: calrissian-tmpout
  namespace: calrissian-demo-project
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: csi-s3-calrissian
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: calrissian-output-data
  namespace: calrissian-demo-project
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: csi-s3-calrissian
EOF
