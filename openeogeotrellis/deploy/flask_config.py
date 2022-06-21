import itertools
from pathlib import Path

from openeogeotrellis import get_backend_version
from openeogeotrellis.deploy import build_gps_backend_deploy_metadata

OPENEO_TITLE = "GeoPySpark based openEO backend"
OPENEO_DESCRIPTION = OPENEO_TITLE

OPENEO_BACKEND_VERSION = get_backend_version()
OPENEO_BACKEND_DEPLOY_METADATA = build_gps_backend_deploy_metadata(
    packages=[
        "openeo",
        "openeo_driver",
        "openeo-geopyspark",
        "geopyspark",
    ],
    jar_paths=itertools.chain(
        # Assumes app is launched from project root:
        Path("jars").glob("geotrellis-backend-assembly*.jar"),
        Path("jars").glob("geotrellis-extensions*.jar"),
    )
)
backend_version = get_backend_version()

MAX_CONTENT_LENGTH = 1024 * 1024  # bytes
