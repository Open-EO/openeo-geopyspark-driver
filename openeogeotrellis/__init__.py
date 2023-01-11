import os

from openeogeotrellis._version import __version__


def get_backend_version() -> str:
    try:
        import importlib.metadata
        # also see issue #138 (distribution name is "openeo-geopyspark")
        return importlib.metadata.version("openeo-geopyspark")
    except Exception:
        # Fallback
        return __version__


# Make sure `pyspark/pandas/__init__.py` does not trigger
# a "PYARROW_IGNORE_TIMEZONE" warning and related side-effects.
# Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/306
os.environ["PYARROW_IGNORE_TIMEZONE"] = os.environ.get("PYARROW_IGNORE_TIMEZONE", "1")
