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


def load_custom_processes(logger=_log, _name="custom_processes"):
    """Try loading optional `custom_processes` module"""
    if path := os.environ.get("OPENEO_CUSTOM_PROCESSES"):
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
    paths = [Path(p) for p in paths]
    return {
        re.match("[a-zA-Z_-]*[a-zA-Z]", p.name).group(0): get_jar_version_info(p)
        for p in paths
    }


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


def build_gps_backend_deploy_metadata(packages: List[str], jar_paths: Iterable[PathLike] = ()) -> dict:
    """Build version metadata dict describing python packages and jar files"""
    metadata = build_backend_deploy_metadata(packages=packages)
    metadata["versions"].update(get_jar_versions(paths=jar_paths))
    return metadata
