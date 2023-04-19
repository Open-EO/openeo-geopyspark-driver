import importlib.resources
import logging
import os
from typing import List, Union, Optional

import attrs

from openeo_driver.config.load import load_from_py_file
from openeo_driver.users.oidc import OidcProvider

_log = logging.getLogger(__name__)


class ConfigException(ValueError):
    pass


@attrs.frozen
class GpsBackendConfig:
    """
    Configuration for GeoPySpark backend.

    Meant to gradually replace ConfigParams (Open-EO/openeo-geopyspark-driver#285)
    """

    # TODO: move base class implementation to openeo-python-driver?

    # identifier for this config
    id: Optional[str] = None

    oidc_providers: List[OidcProvider] = attrs.Factory(list)


# Global config (lazy-load cache)
_gps_backend_config: Union[GpsBackendConfig, None] = None


def gps_backend_config(force_reload: bool = False) -> GpsBackendConfig:
    """Get GpsBackendConfig (lazy loaded + cached)."""
    global _gps_backend_config

    if _gps_backend_config is None or force_reload:
        with importlib.resources.path(
            "openeogeotrellis.config", "default.py"
        ) as default_config:
            config_path = os.environ.get(
                "OPENEO_BACKEND_CONFIG",
                default_config,
            )
        _gps_backend_config = load_from_py_file(
            config_path, expected_class=GpsBackendConfig
        )

    return _gps_backend_config


def flush_gps_backend_config():
    """Reset GpsBackendConfig cache (mainly for tests)"""
    global _gps_backend_config
    _gps_backend_config = None
