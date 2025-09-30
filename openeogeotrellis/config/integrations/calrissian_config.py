import attrs

DEFAULT_NAMESPACE = "calrissian-demo-project"
DEFAULT_INPUT_STAGING_IMAGE = "alpine:3"
# TODO #1007 proper calrissian image? Official one doesn't work due to https://github.com/Duke-GCB/calrissian/issues/124#issuecomment-947008286
DEFAULT_CALRISSIAN_IMAGE = "ghcr.io/duke-gcb/calrissian/calrissian:0.17.1"
DEFAULT_SECURITY_CONTEXT = dict(run_as_user=1000, fs_group=0, run_as_group=0)
DEFAULT_CALRISSIAN_S3_BUCKET = "calrissian"
DEFAULT_CALRISSIAN_BASE_ARGUMENTS = (
    "--debug",
    "--max-ram",
    "15G",  # TODO: Use max_executor_or_driver_memory ?
    "--max-cores",
    "4",
    "--force-docker-pull",
)


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
    S3 bucket associated with `StorageClass` that is used for the `PersistentVolumeClaim`s
    used by the Calrissian jobs.
    """
    s3_bucket: str = DEFAULT_CALRISSIAN_S3_BUCKET

    security_context: dict = attrs.Factory(DEFAULT_SECURITY_CONTEXT.copy)
