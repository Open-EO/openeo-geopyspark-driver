class EVAL_ENV_KEY:
    VAULT_TOKEN = "vault_token"
    SENTINEL_HUB_CLIENT_ALIAS = "sentinel_hub_client_alias"
    MAX_SOFT_ERRORS_RATIO = "max_soft_errors_ratio"
    DEPENDENCIES = "dependencies"
    PYRAMID_LEVELS = "pyramid_levels"
    REQUIRE_BOUNDS = "require_bounds"
    CORRELATION_ID = "correlation_id"
    USER = "user"
    ALLOW_EMPTY_CUBES = "allow_empty_cubes"
    DO_EXTENT_CHECK = "do_extent_check"
    PARAMETERS = "parameters"
    OPENEO_API_VERSION = "openeo_api_version"
    GLOBAL_EXTENT = "global_extent"
    JOB_DIR = "job_dir"
    STAC_API_FILTER_BY_GEOMETRY = "stac_api_filter_by_geometry"


WHITELIST = [
    EVAL_ENV_KEY.VAULT_TOKEN,
    EVAL_ENV_KEY.SENTINEL_HUB_CLIENT_ALIAS,
    EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO,
    EVAL_ENV_KEY.DEPENDENCIES,
    EVAL_ENV_KEY.PYRAMID_LEVELS,
    EVAL_ENV_KEY.REQUIRE_BOUNDS,
    EVAL_ENV_KEY.CORRELATION_ID,
    EVAL_ENV_KEY.USER,
    EVAL_ENV_KEY.ALLOW_EMPTY_CUBES,
    EVAL_ENV_KEY.DO_EXTENT_CHECK,
    EVAL_ENV_KEY.PARAMETERS,
    EVAL_ENV_KEY.OPENEO_API_VERSION,
    EVAL_ENV_KEY.GLOBAL_EXTENT,
    EVAL_ENV_KEY.JOB_DIR,
    # TODO: this linking/allow-listing of job options and eval env keys feels quite cumbersome
    EVAL_ENV_KEY.STAC_API_FILTER_BY_GEOMETRY,
]


JOB_OPTION_LOG_LEVEL = "log_level"
JOB_OPTION_LOGGING_THRESHOLD = "logging-threshold"  # Deprecated in favor of JOB_OPTION_LOG_LEVEL


STAC_API_FILTER_BY_GEOMETRY_DEFAULT = True

# ".invalid" is a reserved TLD, so it can't collide with a real STAC URL.
DUMMY_STAC_URL = "https://dummy.invalid/"
