import importlib.resources
import logging
import os
from pathlib import Path
from typing import List, Union, Optional

import attrs
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

    @staticmethod
    def from_py_file(
        path: Union[str, Path], config_variable: str = "config"
    ) -> "GpsBackendConfig":
        """Load config from a Python file."""
        path = Path(path)
        _log.info(f"Loading GpsBackendConfig from Python file {path}")
        # Based on flask's Config.from_pyfile
        with path.open(mode="rb") as f:
            code = compile(f.read(), path, "exec")
        globals = {"__file__": str(path)}
        exec(code, globals)
        try:
            config = globals[config_variable]
        except KeyError:
            raise ConfigException(
                f"No variable {config_variable!r} found in config file {path}"
            )
        if not isinstance(config, GpsBackendConfig):
            raise ConfigException(
                f"Expected {GpsBackendConfig.__name__} but got {type(config).__name__}"
            )
        return config


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
        _gps_backend_config = GpsBackendConfig.from_py_file(config_path)

    return _gps_backend_config


def flush_gps_backend_config():
    """Reset GpsBackendConfig cache (mainly for tests)"""
    global _gps_backend_config
    _gps_backend_config = None
