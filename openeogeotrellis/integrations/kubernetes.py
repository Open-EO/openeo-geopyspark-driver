"""
Utilities, helpers, adapters for integration with Kubernetes (K8s)
"""
import base64
import logging
import os
import pkg_resources
import time

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
    import yaml
    """ Load and render a provided kubernetes manifest jinja template with the passed kwargs """
    # TODO: move away from pkg_resources https://github.com/Open-EO/openeo-geopyspark-driver/issues/954
    jinja_path = pkg_resources.resource_filename(
        "openeogeotrellis.deploy", template
    )
    jinja_dir = os.path.dirname(jinja_path)
    jinja_env = Environment(
        loader=FileSystemLoader(jinja_dir)
    )

    def base64encode(input_str: str) -> str:
        if not isinstance(input_str, str):
            input_str = str(input_str)
        return base64.b64encode(input_str.encode("utf-8")).decode("utf-8")

    jinja_env.filters["b64encode"] = base64encode
    jinja_env.globals["utcnow_epoch"] = time.time  # TODO: remove this deprecated/unused utility
    jinja_env.globals["unix_time"] = time.time
    jinja_template = jinja_env.from_string(open(jinja_path).read())

    rendered = jinja_template.render(**kwargs)

    return yaml.safe_load(rendered)


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
