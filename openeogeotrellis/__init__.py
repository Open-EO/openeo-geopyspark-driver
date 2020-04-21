import logging

from openeogeotrellis._version import __version__
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.GeotrellisCatalogImageCollection import GeotrellisCatalogImageCollection
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.utils import kerberos

logger = logging.getLogger("openeo")
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s - THREAD: %(threadName)s - %(name)s] : %(message)s")

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_formatter)
logger.addHandler( log_stream_handler )


def get_backend_version() -> str:
    return __version__







# Late import to avoid circular dependency issues.
# TODO avoid this. Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/12
from openeogeotrellis.backend import get_openeo_backend_implementation

