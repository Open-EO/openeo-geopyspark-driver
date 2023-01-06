"""
Utilities, helpers, adapters for integration with Kubernetes (K8s)
"""
import logging

from openeo_driver.jobregistry import JOB_STATUS

_log = logging.getLogger(__name__)

def kube_client():
    from kubernetes import client, config

    config.load_incluster_config()
    api_instance = client.CustomObjectsApi()
    return api_instance


def truncate_job_id_k8s(job_id: str) -> str:
    if job_id.startswith("j-"):
        job_id = job_id[2:]
    return job_id[:10]


def truncate_user_id_k8s(user_id: str) -> str:
    return user_id.split("@")[0][:20]


def k8s_job_name(job_id: str, user_id: str) -> str:
    user_id_truncated = truncate_user_id_k8s(user_id)
    job_id_truncated = truncate_job_id_k8s(job_id)
    return "job-{id}-{user}".format(id=job_id_truncated, user=user_id_truncated)


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
