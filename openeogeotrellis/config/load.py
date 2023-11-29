from pathlib import Path
from typing import ContextManager

from openeo_driver.config.load import ConfigGetter, importlib_resources
from openeogeotrellis.config.config import GpsBackendConfig


class GpsConfigGetter(ConfigGetter):
    # TODO: does this have to be a subclass, or can we just use an instance?
    OPENEO_BACKEND_CONFIG = "OPENEO_BACKEND_CONFIG"
    expected_class = GpsBackendConfig

    def __call__(self, force_reload: bool = False, *, show_stack: bool = True) -> GpsBackendConfig:
        return self.get(force_reload=force_reload, show_stack=show_stack)

    def _default_config(self) -> ContextManager[Path]:
        return importlib_resources.as_file(importlib_resources.files("openeogeotrellis.config") / "default.py")


# Singleton getter.
gps_config_getter = GpsConfigGetter()
