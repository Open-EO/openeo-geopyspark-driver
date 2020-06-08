from openeo_driver.backend import OpenEoBackendImplementation
from openeogeotrellis._version import __version__


def get_backend_version() -> str:
    return __version__


def get_openeo_backend_implementation() -> OpenEoBackendImplementation:
    # Local import to avoid heavy import chain
    from openeogeotrellis.backend import GeoPySparkBackendImplementation
    return GeoPySparkBackendImplementation()
