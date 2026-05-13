import importlib
import logging
import os
import re
import socket
import sys
import zipfile
from os import PathLike
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from openeo_driver.config.load import exec_py_file
from openeo_driver.server import build_backend_deploy_metadata

import openeogeotrellis


_log = logging.getLogger(__name__)


def load_custom_processes(*, path: Optional[Path] = None, logger=_log, _name="custom_processes"):
    """Try loading optional `custom_processes` module"""
    # TODO: use backend_config instead of env var?
    path = path or os.environ.get("OPENEO_CUSTOM_PROCESSES")
    if path:
        # Directly load custom processes from OPENEO_CUSTOM_PROCESSES
        logger.debug(f"load_custom_processes: trying exec loading {path!r}")
        try:
            exec_py_file(path)
            logger.info(f"load_custom_processes: exec loaded {path!r}")
        except Exception as e:
            logger.error(f"load_custom_processes: failed to exec load {path!r}: {e!r}")
    else:
        # Fallback on legacy import-based approach (requires "custom_processes" module to be in PYTHONPATH)
        # TODO: Deprecate/remove this in longer term
        try:
            logger.debug("Trying to load {n!r} with PYTHONPATH {p!r}".format(n=_name, p=sys.path))
            custom_processes = importlib.import_module(_name)
            logger.debug("Loaded {n!r}: {p!r}".format(n=_name, p=custom_processes.__file__))
            return custom_processes
        except ImportError as e:
            logger.debug("{n!r} not loaded: {e!r}.".format(n=_name, e=e))


def get_socket() -> (str, int):
    local_ip = socket.gethostbyname(socket.gethostname())

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
        tcp.bind(('', 0))
        _, port = tcp.getsockname()

    return local_ip, port


def get_jar_version_info(path: PathLike, na_value="n/a") -> str:
    """Extract version info from jar's MANIFEST.MF"""
    try:
        with zipfile.ZipFile(path) as z:
            with z.open("META-INF/MANIFEST.MF") as m:
                manifest_data = m.read().decode("utf8")
        # Parse as dict
        manifest_data = dict(
            m.group("key", "value")
            for m in re.finditer(r"^(?P<key>[\w-]+):\s*(?P<value>.*?)\s*$", manifest_data, flags=re.MULTILINE)
        )
        version = [manifest_data.get("Implementation-Version"), manifest_data.get("SCM-Revision")]
        return " ".join(v for v in version if v) or na_value
    except Exception:
        _log.warning("Failed to extract jar version info", exc_info=True)
        return na_value


def get_jar_versions(paths: Iterable[PathLike]) -> Dict[str, str]:
    """Build dict describing jar versions"""
    versions = {}
    for path in [Path(p) for p in paths]:
        name = re.match(r"[a-zA-Z_-]*[a-zA-Z]", path.name).group(0)
        # Drop suffix because of "PR-123" style versioning (instead of more classic "1.2.3-SNAPSHOT" style)
        if name.endswith("-PR"):
            name = name[:-3]
        versions[name] = get_jar_version_info(path)
    return versions


def find_geotrellis_jars(
    extra_search_locations: Optional[List[Path]] = None, from_cwd: bool = True, from_project_root: bool = True
) -> List[Path]:
    """Find paths to geotrellis-backend-assembly and geotrellis-extensions jars"""
    search_locations = extra_search_locations or []
    if from_cwd:
        search_locations.append(Path("jars"))
    if from_project_root:
        search_locations.append(
            Path(openeogeotrellis.__file__).parent.parent / "jars",
        )
    _log.debug(f"Looking for geotrellis jars in {search_locations=}")

    jars = []
    for search_location in search_locations:
        jars.extend(search_location.glob("geotrellis-extensions-*.jar"))
        if jars:
            break
    _log.debug(f"Found geotrellis jars: {jars}")
    return jars


def patch_sar_backscatter_spec(backend_implementation) -> None:
    """
    Apply deployment-specific overrides to the sar_backscatter process spec.

    This backend uses Orfeo Toolbox for sar_backscatter, so we mark the process
    as non-experimental and restrict the coefficient parameter to the supported
    ``sigma0-ellipsoid`` option.

    See https://github.com/Open-EO/openeo-python-driver/issues/242 – a more
    generic override mechanism does not yet exist, so we do it here.
    """
    processes = backend_implementation.processing.get_process_registry("1.2.0")
    backscatter_spec = processes.get_spec("sar_backscatter")
    backscatter_spec["experimental"] = False
    backscatter_spec["description"] = (
        backscatter_spec["description"]
        + """
    \n\n ## Backend notes \n\n The implementation in this backend is based on Orfeo Toolbox.
    """
    )
    parameters = {p["name"]: p for p in backscatter_spec["parameters"]}
    parameters["coefficient"]["default"] = "sigma0-ellipsoid"
    parameters["coefficient"]["description"] = (
        "Select the radiometric correction coefficient. The following options are available:\n\n"
        "* `sigma0-ellipsoid`: ground area computed with ellipsoid earth model\n"
    )
    parameters["coefficient"]["schema"] = [
        {"type": "string", "enum": ["sigma0-ellipsoid"]},
        {"title": "Non-normalized backscatter", "type": "null"},
    ]
    backscatter_spec["links"].append(
        {
            "rel": "about",
            "href": "https://www.orfeo-toolbox.org/CookBook/Applications/app_SARCalibration.html",
            "title": "Orfeo toolbox backscatter processor.",
        }
    )


def build_gps_backend_deploy_metadata(packages: List[str], jar_paths: Iterable[PathLike] = ()) -> dict:
    """Build version metadata dict describing python packages and jar files"""
    metadata = build_backend_deploy_metadata(packages=packages)
    metadata["versions"].update(get_jar_versions(paths=jar_paths))
    return metadata
