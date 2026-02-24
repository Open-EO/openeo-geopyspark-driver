from typing import Optional

import attrs
import os

from openeo_driver.utils import smart_bool

DEFAULT_NAMESPACE = "calrissian-demo-project"
DEFAULT_INPUT_STAGING_IMAGE = "alpine:3"
# TODO #1007 proper calrissian image? Official one doesn't work due to https://github.com/Duke-GCB/calrissian/issues/124#issuecomment-947008286
DEFAULT_CALRISSIAN_IMAGE = "ghcr.io/duke-gcb/calrissian/calrissian:0.18.1"
DEFAULT_CALRISSIAN_RUNNER_RESOURCE_REQUIREMENTS = dict(
    limits={"memory": "256Mi"}, requests={"cpu": "1m", "memory": "256Mi"}
)

DEFAULT_SECURITY_CONTEXT = dict(run_as_user=1000, fs_group=0, run_as_group=0)
DEFAULT_CALRISSIAN_S3_REGION = None
DEFAULT_CALRISSIAN_S3_BUCKET = "calrissian"
DEFAULT_CALRISSIAN_BASE_ARGUMENTS: tuple = (
    "--debug",
    "--max-ram",
    "128G",  # Seems to be a limit for pods combined across nodes. Testing with higher threshold. # TODO: Use max_executor_or_driver_memory ?
    "--max-cores",
    "1000",
    "--force-docker-pull",
)
if smart_bool(os.environ.get("OPENEO_LOCAL_DEBUGGING", "false")):
    DEFAULT_CALRISSIAN_BASE_ARGUMENTS += ("--leave-container", "--leave-tmpdir")
    # Avoid local error:
    # "OSError: [Errno 18] Invalid cross-device link: '/calrissian/tmpout/xzo916uy' -> '/calrissian/output-data/r-2601261601194b68b7-cal-cwl-607800ce/xzo916uy'"
    DEFAULT_SECURITY_CONTEXT["run_as_user"] = 0


@attrs.frozen(kw_only=True)
class CalrissianConfig:
    """
    Configration container for Calrissian integration.
    Intended as sub-config of GpsBackendConfig
    """

    """
    Kubernetes namespace to use for Calrissian jobs.
    """
    namespace: str = DEFAULT_NAMESPACE

    """
    Docker image to use for the input staging step of the Calrissian workflow.
    Should be a small image providing basic shell utilities (e.g. alpine).
    """
    input_staging_image: str = DEFAULT_INPUT_STAGING_IMAGE

    """
    Docker image providing the Calrissian tool
    """
    calrissian_image: str = DEFAULT_CALRISSIAN_IMAGE

    """
    S3 region associated with `StorageClass` that is used for the `PersistentVolumeClaim`s
    used by the Calrissian jobs.
    """
    s3_region: Optional[str] = DEFAULT_CALRISSIAN_S3_REGION

    """
    S3 bucket associated with `StorageClass` that is used for the `PersistentVolumeClaim`s
    used by the Calrissian jobs.
    """
    s3_bucket: str = DEFAULT_CALRISSIAN_S3_BUCKET

    security_context: dict = attrs.Factory(DEFAULT_SECURITY_CONTEXT.copy)

    """
    The pod that runs the calrissian tool is the runner of the CWL workflow and requires its own resources to drive
    the resources that actually perform the work which are specified . Ideally a default
    """
    runner_resource_requirements: dict = attrs.Factory(DEFAULT_CALRISSIAN_RUNNER_RESOURCE_REQUIREMENTS.copy)
