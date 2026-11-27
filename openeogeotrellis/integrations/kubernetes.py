"""
Utilities, helpers, adapters for integration with Kubernetes (K8s)
"""
import base64
import logging
import os
import time
from importlib.resources import files
from typing import List

from jinja2 import Environment, FileSystemLoader

from openeo_driver.jobregistry import JOB_STATUS
from openeo_driver.utils import generate_unique_id


_log = logging.getLogger(__name__)


def ensure_kubernetes_config():
    import kubernetes
    from kubernetes.config.incluster_config import SERVICE_TOKEN_FILENAME

    if os.path.exists(SERVICE_TOKEN_FILENAME):
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config()


def kube_client(api_type):
    from kubernetes import client

    ensure_kubernetes_config()

    if api_type == "CustomObject":
        api_instance = client.CustomObjectsApi()
    elif api_type == "Core":
        api_instance = client.CoreV1Api()
    else:
        raise ValueError(api_type)

    return api_instance

def truncate_job_id_k8s(job_id: str) -> str:
    if job_id.startswith("j-"):
        job_id = job_id[2:]
    return job_id[:10]


def truncate_user_id_k8s(user_id: str) -> str:
    return user_id.split("@")[0][:13]


def k8s_job_name() -> str:
    return generate_unique_id(prefix="a", date_prefix=False)


class K8S_SPARK_APP_STATE:
    # Job states as returned by spark-on-k8s-operator (sparkoperator.k8s.io)
    # Based on https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/22cd4a2c6990df90ab1cb6b0ffbd9d8b76646790/pkg/apis/sparkoperator.k8s.io/v1beta2/types.go#L328-L344
    NEW = ""
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SUBMISSION_FAILED = "SUBMISSION_FAILED"
    PENDING_RERUN = "PENDING_RERUN"
    INVALIDATING = "INVALIDATING"
    SUCCEEDING = "SUCCEEDING"
    FAILING = "FAILING"
    UNKNOWN = "UNKNOWN"


def k8s_state_to_openeo_job_status(state: str) -> str:
    """Map Kubernetes app state to openEO batch job status"""
    if state in {K8S_SPARK_APP_STATE.NEW, K8S_SPARK_APP_STATE.SUBMITTED}:
        job_status = JOB_STATUS.QUEUED
    elif state in {K8S_SPARK_APP_STATE.RUNNING, K8S_SPARK_APP_STATE.SUCCEEDING}:
        job_status = JOB_STATUS.RUNNING
    elif state == K8S_SPARK_APP_STATE.COMPLETED:
        job_status = JOB_STATUS.FINISHED
    elif state in {
        K8S_SPARK_APP_STATE.FAILED,
        K8S_SPARK_APP_STATE.SUBMISSION_FAILED,
        K8S_SPARK_APP_STATE.FAILING,
    }:
        job_status = JOB_STATUS.ERROR
    else:
        _log.warning(f"Unhandled K8s app state mapping {state}")
        # Fallback to minimal status "queued" (once in K8s, batch job status should be at least "queued")
        job_status = JOB_STATUS.QUEUED
    # TODO: is there a kubernetes state for canceled apps?
    return job_status


def k8s_render_manifest_template(template, **kwargs) -> dict:
    """ Load and render a provided kubernetes manifest jinja template with the passed kwargs """
    import yaml

    template_resource = files("openeogeotrellis.deploy").joinpath(template)
    jinja_path = str(template_resource)
    jinja_dir = os.path.dirname(jinja_path)
    jinja_env = Environment(loader=FileSystemLoader(jinja_dir))

    def base64encode(input_str: str) -> str:
        if not isinstance(input_str, str):
            input_str = str(input_str)
        return base64.b64encode(input_str.encode("utf-8")).decode("utf-8")

    jinja_env.filters["b64encode"] = base64encode
    jinja_env.globals["utcnow_epoch"] = time.time  # TODO: remove this deprecated/unused utility
    jinja_env.globals["unix_time"] = time.time
    jinja_template = jinja_env.from_string(template_resource.read_text(encoding="utf-8"))

    rendered = jinja_template.render(**kwargs)
    return yaml.safe_load(rendered)


# Fields of spec.{driver,executor} that are only applied by the spark-operator's mutating pod webhook
# (not by spark-submit), so they do not belong next to a pod template.
_K8S_WEBHOOK_ONLY_POD_FIELDS = (
    "affinity",
    "configMaps",
    "dnsConfig",
    "env",
    "envFrom",
    "envVars",
    "gpu",
    "hostAliases",
    "hostNetwork",
    "initContainers",
    "lifecycle",
    "memoryLimit",
    "nodeSelector",
    "podSecurityContext",
    "ports",
    "priorityClassName",
    "schedulerName",
    "securityContext",
    "shareProcessNamespace",
    "sidecars",
    "terminationGracePeriodSeconds",
    "tolerations",
    "volumeMounts",
)
# Names of the main container in the pod templates, as configured by the spark-operator.
_K8S_POD_TEMPLATE_CONTAINER_NAMES = {"driver": "spark-kubernetes-driver", "executor": "spark-kubernetes-executor"}
# Volumes Spark manages itself (local dirs), the webhook never adds these.
_K8S_SPARK_LOCAL_DIR_VOLUME_PREFIX = "spark-local-dir-"


def k8s_finalize_pod_templates(app: dict) -> List[str]:
    """
    Finalize the pod templates (spec.{driver,executor}.template) of a rendered SparkApplication, in place,
    and return findings about what still relies on the spark-operator's mutating pod webhook or will not work.

    Does nothing when neither the driver nor the executor has a pod template.

    The template base renders all volumes in both pod templates, here each pod only keeps the volumes that its
    containers mount, which mirrors the webhook (``addVolumes``):
    - volume mounts of the main container without a matching volume are dropped (the webhook skips them silently),
    - ``spark-local-dir-*`` volumes and main container mounts are dropped (Spark manages its local dir volumes).
    When ``monitoring.prometheus`` is self-contained (``configFile`` and ``metricsPropertiesFile`` set),
    the ``jmx-exporter`` container port the webhook would otherwise add is added as well.
    """
    spec = app.get("spec") or {}
    roles = [r for r in ("driver", "executor") if isinstance((spec.get(r) or {}).get("template"), dict)]
    if not roles:
        return []

    findings = []
    shared_volumes = spec.pop("volumes", None) or []
    if shared_volumes:
        findings.append(
            "spec.volumes is only applied by the operator webhook, render volumes in the pod templates instead"
        )

    prometheus = (spec.get("monitoring") or {}).get("prometheus")
    self_contained_prometheus = bool(
        prometheus and prometheus.get("configFile") and spec["monitoring"].get("metricsPropertiesFile")
    )
    if prometheus and not self_contained_prometheus:
        findings.append(
            "monitoring.prometheus without both configFile and metricsPropertiesFile:"
            " the operator webhook still has to mount its prometheus configmap"
        )

    for role in ("driver", "executor"):
        role_spec = spec.get(role) or {}
        if role not in roles:
            findings.append(f"spec.{role} has no pod template")
            continue

        left = [f for f in _K8S_WEBHOOK_ONLY_POD_FIELDS if role_spec.get(f) not in (None, [], {})]
        if left:
            findings.append(f"spec.{role} fields only applied by the operator webhook: {', '.join(left)}")

        pod_spec = role_spec["template"].get("spec") or {}
        role_spec["template"]["spec"] = pod_spec
        _k8s_drop_empty(pod_spec)
        containers = pod_spec.get("containers") or []
        expected_name = _K8S_POD_TEMPLATE_CONTAINER_NAMES[role]
        if len(containers) != 1 or containers[0].get("name") != expected_name:
            names = [c.get("name") for c in containers]
            findings.append(f"spec.{role}.template should have exactly one container {expected_name!r}, got {names}")
        if not containers:
            continue
        container = containers[0]
        _k8s_drop_empty(container)
        init_containers = pod_spec.get("initContainers") or []

        # Volumes: those of the pod template, then the shared ones, first occurrence of a name wins.
        available = {}
        for volume in (pod_spec.get("volumes") or []) + shared_volumes:
            name = volume.get("name") or ""
            if name in available:
                if volume != available[name]:
                    findings.append(f"spec.{role}: conflicting definitions of volume {name!r}, using the first")
            elif name and not name.startswith(_K8S_SPARK_LOCAL_DIR_VOLUME_PREFIX):
                available[name] = volume

        mounts = []
        for mount in container.get("volumeMounts") or []:
            name = mount.get("name") or ""
            if name.startswith(_K8S_SPARK_LOCAL_DIR_VOLUME_PREFIX):
                findings.append(
                    f"spec.{role}: dropped mount {mount.get('mountPath')} of Spark local dir volume {name!r}"
                )
            elif name not in available:
                findings.append(f"spec.{role}: dropped mount {mount.get('mountPath')} of undefined volume {name!r}")
            else:
                mounts.append(mount)
        if mounts:
            container["volumeMounts"] = mounts
        else:
            container.pop("volumeMounts", None)

        used = {m.get("name") for m in mounts}
        for init_container in init_containers:
            for mount in init_container.get("volumeMounts") or []:
                name = mount.get("name") or ""
                if name in available:
                    used.add(name)
                elif not name.startswith(_K8S_SPARK_LOCAL_DIR_VOLUME_PREFIX):
                    findings.append(
                        f"spec.{role}: init container {init_container.get('name')!r} mounts undefined volume {name!r}"
                    )
        volumes = [v for n, v in available.items() if n in used]
        if volumes:
            pod_spec["volumes"] = volumes
        else:
            pod_spec.pop("volumes", None)

        mount_paths = [m.get("mountPath") for m in mounts]
        duplicate_paths = sorted({p for p in mount_paths if mount_paths.count(p) > 1})
        if duplicate_paths:
            findings.append(f"spec.{role}: duplicate volume mount paths {duplicate_paths}")
        env_names = [e.get("name") for e in container.get("env") or []]
        duplicate_env = sorted({n for n in env_names if env_names.count(n) > 1})
        if duplicate_env:
            findings.append(f"spec.{role}: duplicate env vars {duplicate_env}")

        if self_contained_prometheus:
            port = prometheus.get("port", 8090)
            ports = container.setdefault("ports", [])
            jmx_ports = [p for p in ports if p.get("name") == "jmx-exporter"]
            if not jmx_ports:
                ports.append({"name": "jmx-exporter", "containerPort": port, "protocol": "TCP"})
            elif jmx_ports[0].get("containerPort") != port:
                findings.append(f"spec.{role}: jmx-exporter port differs from monitoring.prometheus.port {port}")
            metrics_properties = spec["monitoring"]["metricsPropertiesFile"]
            if not any(metrics_properties == p or metrics_properties.startswith(f"{p}/") for p in mount_paths):
                findings.append(f"spec.{role}: metricsPropertiesFile {metrics_properties} is not mounted")

    return findings


def _k8s_drop_empty(d: dict):
    """Drop keys without a value (e.g. a list key that rendered without items)."""
    for key in [k for k, v in d.items() if v is None]:
        del d[key]


def k8s_get_batch_job_cfg_secret_name(spark_app_name: str) -> str:
    """
    Get a secret name for a submitted spark application
    """
    return f"cfg-{spark_app_name}"


def k8s_set_secret_owner_reference(
    api_instance_core,
    *,
    namespace: str,
    secret_name: str,
    owner: dict,
    log: logging.LoggerAdapter = None,
) -> bool:
    """
    Make the given secret owned by `owner` so that Kubernetes garbage collects the secret
    when the owner is deleted.

    This is a best effort operation: secrets are also cleaned up separately based on their
    "created_at" annotation, so failing to set the owner reference is only worth a warning.

    :param api_instance_core: Kubernetes Core API instance
    :param namespace: namespace of the secret (must be the same as the owner's namespace)
    :param secret_name: name of the secret to adopt
    :param owner: the owning object, as returned when creating it (must contain `metadata.uid`)
    :return: whether the owner reference was successfully set
    """
    log = log or _log
    try:
        owner_metadata = owner.get("metadata") or {}
        owner_uid = owner_metadata.get("uid")
        owner_name = owner_metadata.get("name")
        if not (owner_uid and owner_name):
            log.warning(f"Not setting owner reference on secret {secret_name}: no uid/name in owner metadata")
            return False

        owner_reference = {
            "apiVersion": owner.get("apiVersion") or "sparkoperator.k8s.io/v1beta2",
            "kind": owner.get("kind") or "SparkApplication",
            "name": owner_name,
            "uid": owner_uid,
            "controller": False,
            # Note: `blockOwnerDeletion` requires "update" permission on the owner's "finalizers"
            # subresource, which we do not have, so it must be left disabled.
            "blockOwnerDeletion": False,
        }
        # Note: a strategic merge patch on "metadata" only touches the owner references,
        # so labels/annotations (used for the separate secret cleanup) are left untouched.
        api_instance_core.patch_namespaced_secret(
            name=secret_name,
            namespace=namespace,
            body={"metadata": {"ownerReferences": [owner_reference]}},
        )
        log.debug(f"Set owner reference of secret {secret_name} to {owner_name} ({owner_uid})")
        return True
    except Exception as e:
        log.warning(
            f"Failed to set owner reference on secret {secret_name}: {type(e).__name__}: {e}",
            exc_info=True,
        )
        return False
