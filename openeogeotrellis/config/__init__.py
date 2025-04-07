from typing import Callable

from openeogeotrellis.config.config import GpsBackendConfig
from openeogeotrellis.config.load import gps_config_getter

# Function-like config getter
get_backend_config: Callable[..., GpsBackendConfig] = gps_config_getter
