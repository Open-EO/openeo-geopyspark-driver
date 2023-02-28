import importlib
import logging
import os
import re
import socket
import sys
import zipfile
from os import PathLike
from pathlib import Path
from typing import Dict, Iterable, List

from kazoo.client import KazooClient

from openeo_driver.server import build_backend_deploy_metadata
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.traefik import Traefik

_log = logging.getLogger(__name__)


def load_custom_processes(logger=_log, _name="custom_processes"):
    """Try loading optional `custom_processes` module"""
    try:
        logger.info("Trying to load {n!r} with PYTHONPATH {p!r}".format(n=_name, p=sys.path))
        custom_processes = importlib.import_module(_name)
        logger.info("Loaded {n!r}: {p!r}".format(n=_name, p=custom_processes.__file__))
        return custom_processes
    except ImportError as e:
        logger.info('{n!r} not loaded: {e!r}.'.format(n=_name, e=e))


def get_socket() -> (str, int):
    local_ip = socket.gethostbyname(socket.gethostname())

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp:
        tcp.bind(('', 0))
        _, port = tcp.getsockname()

    return local_ip, port


def update_zookeeper(cluster_id: str, rule: str, host: str, port: int, health_check: str = None) -> None:
    server_id = os.environ.get("OPENEO_TRAEFIK_SERVER_ID", host)

    zk = KazooClient(hosts=','.join(ConfigParams().zookeepernodes))
    zk.start()
    try:
        Traefik(zk).add_load_balanced_server(
            cluster_id=cluster_id,
            server_id=server_id,
            host=host, port=port,
            rule=rule,
            health_check=health_check
        )
    finally:
        zk.stop()
        zk.close()


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


def build_gps_backend_deploy_metadata(packages: List[str], jar_paths: Iterable[PathLike] = ()) -> dict:
    """Build version metadata dict describing python packages and jar files"""
    metadata = build_backend_deploy_metadata(packages=packages)
    metadata["versions"].update(get_jar_versions(paths=jar_paths))
    return metadata
