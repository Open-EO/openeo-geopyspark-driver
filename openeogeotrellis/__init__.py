from openeogeotrellis._version import __version__


def get_backend_version() -> str:
    try:
        import importlib.metadata
        # also see issue #138 (distribution name is "openeo-geopyspark")
        return importlib.metadata.version("openeo-geopyspark")
    except Exception:
        # Fallback
        return __version__
