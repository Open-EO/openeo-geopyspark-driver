 # CWL based openEO processing using Calrissian

This document describes how CWL based processing is implemented
in the openEO GeoPySpark driver when running on a Kubernetes cluster.

## Background and terminology

[CWL (Common Workflow Language)](https://www.commonwl.org) is an open standard
for describing how to run (command line) tools and connect them to create workflows.
While CWL and openEO are a bit competing due to this conceptual overlap,
there is demand to run existing or new CWL workflows as part of a larger openEO processing chain.
This is comparable to UDFs which allow to run (Python) scripts as part of an openEO workflow.

[Calrissian](https://duke-gcb.github.io/calrissian/) is a CWL implementation
designed to run inside a Kubernetes cluster.


## Kubernetes setup


Note: this is an extension to the Kubernetes setup discussed
in the [Calrissian cluster configuration docs](https://duke-gcb.github.io/calrissian/cluster-configuration/).

### Namespace

Create a namespace for the Calrissian service:

```bash
NAMESPACE_NAME=calrissian-demo-project
kubectl create namespace "$NAMESPACE_NAME"
```


### Roles and permissions


Create roles:

```bash
kubectl \
    --namespace="$NAMESPACE" \
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
    --verb=create,delete,list \
    --resource=jobs

kubectl \
    --namespace="$NAMESPACE_NAME" \
    create role \
    pvc-reader-role \
    --verb=list,get \
    --resource=persistentvolumeclaims
```

And attach roles to the service account in that namespace:

```bash
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
```

Also attach roles to the appropriate service accounts (in other namespaces)

```bash
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
```

### Storage

Create a `StorageClass` `csi-s3-calrissian` for the volumes used by Calrissian.
For example with `geesefs` from `ru.yandex.s3.csi`:

```yaml
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: csi-s3-calrissian
provisioner: ru.yandex.s3.csi
parameters:
  mounter: geesefs
  # you can set mount options here, for example limit memory cache size (recommended)
  options: "--memory-limit 1000 --dir-mode 0777 --file-mode 0666 --no-systemd"
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
```

Create the `PersistentVolumeClaim`s for the input, tmp and output volumes.
For example, with namespace `calrissian-demo-project` and
storage class `csi-s3-calrissian` as defined above:


```yaml
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
```
