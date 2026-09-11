"""
Python wrapper around the Scala HealpixDatacube.

Processes are auto-discovered from the Scala side via HealpixProcessRegistry.
Python-specific overrides can be registered via @healpix_override decorator.

By default, adding a new @HealpixProcess-annotated method in Scala requires
NO Python changes — it is auto-discovered at runtime.
"""
import logging
import pathlib
from typing import Optional, Callable, Dict, List, Union

from openeo_driver.datacube import DriverDataCube
from openeo_driver.datastructs import StacAsset
from openeogeotrellis.utils import get_jvm
from openeogeotrellis.geopysparkdatacube import callsite

_log = logging.getLogger(__name__)

# Registry of Python-side overrides.  Key = openEO process id.
_python_overrides: Dict[str, Callable] = {}


def healpix_override(process_id: str):
    """
    Decorator to register a Python override for a specific openEO process.

    The decorated function receives ``(cube: HealpixDataCube, **kwargs)`` and
    must return a ``HealpixDataCube`` (or other result).

    Example::

        @healpix_override("apply")
        def _apply(cube, process=None, context=None, env=None):
            # custom Python logic, e.g. UDF handling
            ...
    """
    def decorator(func):
        _python_overrides[process_id] = func
        return func
    return decorator


class HealpixDataCube(DriverDataCube):
    """
    Python wrapper around the Scala ``HealpixDatacube``.

    Process dispatch:

    1. On init the Scala ``HealpixProcessRegistry`` is queried for all
       ``@HealpixProcess``-annotated methods.  This builds a local catalogue
       (class-level, loaded once per JVM).
    2. When an openEO process is invoked (e.g. ``cube.apply(...)``):

       a. If a Python override is registered via ``@healpix_override`` → call it.
       b. Otherwise delegate to the Scala method via ``HealpixProcessRegistry.invoke``.

    3. Adding a new process in Scala (with ``@HealpixProcess``) requires
       **no** Python changes — it is auto-discovered.
    """

    # Class-level cache — loaded once per JVM lifetime.
    _scala_processes: Optional[Dict[str, dict]] = None

    def __init__(self, scala_cube, metadata=None):
        """
        :param scala_cube: py4j JVM reference to a ``HealpixDatacube`` instance.
        :param metadata:   openEO cube metadata (optional).
        """
        super().__init__(metadata=metadata)
        self._jvm_cube = scala_cube
        if HealpixDataCube._scala_processes is None:
            HealpixDataCube._load_scala_processes()

    # ---- Scala process discovery -----------------------------------------

    @classmethod
    def _load_scala_processes(cls):
        """Query the Scala ``HealpixProcessRegistry`` for supported processes."""
        jvm = get_jvm()
        registry = jvm.org.openeo.geotrellishealpix.HealpixProcessRegistry
        # listProcesses() returns java.util.List[java.util.Map] — py4j
        # converts these to Python list[dict] automatically.
        processes = list(registry.listProcesses())
        cls._scala_processes = {p["id"]: dict(p) for p in processes}
        _log.info(
            "Loaded %d HealpixDatacube processes from Scala: %s",
            len(cls._scala_processes), list(cls._scala_processes.keys()),
        )

    # ---- Properties ------------------------------------------------------

    @property
    def nside(self) -> int:
        return self._jvm_cube.nside()

    @property
    def jvm_cube(self):
        return self._jvm_cube

    # ---- Result wrapping -------------------------------------------------

    def _wrap_result(self, jvm_result) -> "HealpixDataCube":
        """Wrap a Scala HealpixDatacube JVM reference back into Python."""
        return HealpixDataCube(jvm_result, metadata=self.metadata)

    # ---- Generic process dispatch ----------------------------------------

    def _invoke_scala(self, process_id: str, **kwargs):
        """
        Invoke a Scala-side process by openEO process id.

        Arguments are passed as a plain Python dict — py4j converts it to
        ``java.util.HashMap`` automatically.
        """
        desc = self._scala_processes.get(process_id)
        if desc is None:
            raise NotImplementedError(
                f"Process '{process_id}' not found in Scala HealpixProcessRegistry. "
                f"Available: {list(self._scala_processes.keys())}"
            )

        jvm = get_jvm()
        registry = jvm.org.openeo.geotrellishealpix.HealpixProcessRegistry

        # Build args map keyed by Scala parameter names.
        args = {}
        for param in desc["params"]:
            name = param["name"]
            if name in kwargs:
                args[name] = kwargs[name]

        result = registry.invoke(self._jvm_cube, process_id, args)

        if desc["returns"] == "datacube":
            return self._wrap_result(result)
        return result

    def _dispatch(self, process_id: str, **kwargs):
        """
        Dispatch an openEO process:

        1. Python override if registered via ``@healpix_override``.
        2. Scala method via ``HealpixProcessRegistry``.
        """
        if process_id in _python_overrides:
            _log.debug("Using Python override for '%s'", process_id)
            return _python_overrides[process_id](self, **kwargs)

        if process_id in (self._scala_processes or {}):
            _log.debug("Delegating '%s' to Scala", process_id)
            return self._invoke_scala(process_id, **kwargs)

        raise NotImplementedError(
            f"Process '{process_id}' has no Python override and is not "
            f"registered in Scala HealpixProcessRegistry."
        )

    # ---- openEO DriverDataCube interface methods -------------------------
    # These map the DriverDataCube API to the generic dispatch mechanism.

    @callsite
    def apply(self, process, *, context=None, env=None) -> "HealpixDataCube":
        return self._dispatch("apply", process=process, context=context, env=env)

    @callsite
    def filter_temporal(self, start, end) -> "HealpixDataCube":
        return self._dispatch("filter_temporal", start=start, end=end)

    @callsite
    def filter_bbox(self, west, east, north, south, crs=None, **kwargs) -> "HealpixDataCube":
        return self._dispatch(
            "filter_bbox",
            west=west, east=east, north=north, south=south, crs=crs,
        )

    @callsite
    def filter_bands(self, bands) -> "HealpixDataCube":
        return self._dispatch("filter_bands", bandNames=bands)

    @callsite
    def apply_dimension(self, process, *, dimension, target_dimension=None,
                        context=None, env=None) -> "HealpixDataCube":
        return self._dispatch(
            "apply_dimension",
            process=process, dimension=dimension,
            target_dimension=target_dimension, context=context,
        )

    @callsite
    def reduce_dimension(self, reducer, *, dimension, context=None,
                         env=None) -> "HealpixDataCube":
        return self._dispatch(
            "reduce_dimension",
            reducer=reducer, dimension=dimension, context=context,
        )

    @callsite
    def aggregate_temporal(self, intervals, reducer, labels=None, *,
                           dimension=None, context=None) -> "HealpixDataCube":
        return self._dispatch(
            "aggregate_temporal",
            intervals=intervals, reducer=reducer, labels=labels,
        )

    def write_assets(self, filename: Union[str, pathlib.Path], format: str, format_options: dict = None) -> Dict[ str, StacAsset]:
        jvm = get_jvm()
        #TODO make work for sync requests, do not use /tmp directly
        out_dir =  "/tmp/data.zarr"
        jvm.org.openeo.geotrellishealpix.HealpixZarrWriter.write(self._jvm_cube, str(out_dir), 2)

        assets = dict(zarr_data=StacAsset(href=str(out_dir), type="application/x-zarr"))
        item = {
            "id": "myhealpix_cube",
            "properties": {},

            "assets": assets,
        }
        return {item["id"]: item}


    def save_result(self, filename, format, format_options=None):
        result = self.write_assets(filename, format, format_options)
        assets = [asset for item in result.values() for asset in item["assets"].values()]
        return assets[0]["href"]

    def resample_spatial(self, target_crs, layout, extent, band_indices=None):
        """Resample to a regular grid — returns a GeoTrellis RDD (not a HealpixDataCube)."""
        return self._dispatch(
            "resample_spatial",
            targetCRS=target_crs, layout=layout, extent=extent,
            bandIndices=band_indices or [0],
        )

    # ---- Inspection API --------------------------------------------------

    def list_supported_processes(self) -> List[dict]:
        """
        Return the list of processes supported by this datacube, in a format
        close to the openEO process description.

        Includes both Scala-discovered and Python-override processes.
        """
        result = []
        all_ids = set(self._scala_processes or {}) | set(_python_overrides)
        for pid in sorted(all_ids):
            entry = {
                "id": pid,
                "implementation": "python" if pid in _python_overrides else "scala",
            }
            if pid in (self._scala_processes or {}):
                desc = self._scala_processes[pid]
                entry["description"] = desc.get("description", "")
                entry["parameters"] = [
                    {"name": p["name"], "schema": {"type": p["type"]}}
                    for p in desc.get("params", [])
                ]
                entry["returns"] = desc.get("returns", "datacube")
            result.append(entry)
        return result


# ---- Default Python overrides for processes needing Python-side logic ----

@healpix_override("apply")
def _apply_override(cube: HealpixDataCube, process=None, context=None, env=None):
    """
    The 'apply' process needs Python-side handling for UDF detection,
    then delegates the actual computation to Scala.
    """
    from openeogeotrellis.backend import GeoPySparkBackendImplementation
    from openeogeotrellis.processgraphvisiting import (
        GeotrellisTileProcessGraphVisitor,
        SingleNodeUDFProcessGraphVisitor,
    )

    if isinstance(process, dict):
        process = GeoPySparkBackendImplementation.accept_process_graph(
            process,
            default_input_parameter="data",
            default_input_datatype="float64",
        )

    if isinstance(process, GeotrellisTileProcessGraphVisitor):
        # Delegate to Scala applyProcess(builder, context)
        jvm_context = context if isinstance(context, dict) else {}
        result = cube.jvm_cube.applyProcess(process.builder, jvm_context)
        return cube._wrap_result(result)

    if isinstance(process, SingleNodeUDFProcessGraphVisitor):
        raise NotImplementedError("UDF apply on HealpixDataCube not yet supported")

    raise NotImplementedError(f"Unsupported apply process type: {type(process)}")


@healpix_override("filter_temporal")
def _filter_temporal_override(cube: HealpixDataCube, start=None, end=None):
    """
    Filter temporal with metadata update.
    """
    result = cube._invoke_scala("filter_temporal", start=str(start), end=str(end))
    if result.metadata:
        result.metadata = result.metadata.filter_temporal(start, end)
    return result


@healpix_override("filter_bands")
def _filter_bands_override(cube: HealpixDataCube, bandNames=None):
    """
    Filter bands with metadata update.
    """
    import javabridge  # noqa: F401 — ensure java list conversion
    result = cube._invoke_scala("filter_bands", bandNames=list(bandNames))
    if result.metadata and hasattr(result.metadata, "filter_bands"):
        result.metadata = result.metadata.filter_bands(bandNames)
    return result

