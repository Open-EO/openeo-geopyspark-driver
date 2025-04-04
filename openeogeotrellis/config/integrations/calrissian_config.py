import attrs

DEFAULT_NAMESPACE = "calrissian-demo-project"
DEFAULT_INPUT_STAGING_IMAGE = "alpine:3"
DEFAULT_CALRISSIAN_IMAGE = "ghcr.io/duke-gcb/calrissian/calrissian:0.17.1"
DEFAULT_SECURITY_CONTEXT = dict(run_as_user=1000, run_as_group=1000)
DEFAULT_CALRISSIAN_S3_BUCKET = "calrissian"
DEFAULT_CALRISSIAN_BASE_ARGUMENTS = (
    "--debug",
    "--max-ram",
    "2G",
    "--max-cores",
    "1",
)


@attrs.frozen(kw_only=True)
class CalrissianConfig:
    """
    Configration container for Calrissian integration.
    Intended as sub-config of GpsBackendConfig
    """

    namespace: str = DEFAULT_NAMESPACE

    # TODO #1009/#1132 revert default to DEFAULT_INPUT_STAGING_IMAGE once custom image is in appropriate configs
    # input_staging_image: str = DEFAULT_INPUT_STAGING_IMAGE
    input_staging_image: str = "registry.stag.warsaw.openeo.dataspace.copernicus.eu/rand/alpine:3"

    # TODO #1007 proper calrissian image? Official one doesn't work due to https://github.com/Duke-GCB/calrissian/issues/124#issuecomment-947008286
    # calrissian_image: str = DEFAULT_CALRISSIAN_IMAGE
    calrissian_image: str = "registry.stag.warsaw.openeo.dataspace.copernicus.eu/rand/calrissian:latest"

    s3_bucket: str = DEFAULT_CALRISSIAN_S3_BUCKET

    security_context: dict = attrs.Factory(DEFAULT_SECURITY_CONTEXT.copy)
