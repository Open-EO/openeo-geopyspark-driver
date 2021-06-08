from openeo_driver.server import build_backend_deploy_metadata
from openeogeotrellis import get_backend_version

OPENEO_TITLE = "VITO Remote Sensing openEO API"
OPENEO_DESCRIPTION = """
    OpenEO API to the VITO Remote Sensing product catalog and processing services
    (using GeoPySpark driver).
"""

OPENEO_BACKEND_VERSION = get_backend_version()
OPENEO_BACKEND_DEPLOY_METADATA = build_backend_deploy_metadata(
    packages=[
        "openeo",
        "openeo_driver",
        "openeo-geopyspark",
        "openeo_udf",
        "geopyspark",
    ]
    # TODO: add version info about geotrellis-extensions jar?
),
backend_version = get_backend_version(),

MAX_CONTENT_LENGTH = 1024 * 1024  # bytes
