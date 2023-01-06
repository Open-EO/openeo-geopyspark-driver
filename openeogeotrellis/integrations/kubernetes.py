"""
Utilities, helpers, adapters for integration with Kubernetes (K8s)
"""


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
