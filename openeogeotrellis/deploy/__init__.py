import sys
import importlib
import logging
import socket
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


def update_zookeeper(host: str, port: int, env: str) -> None:
    from kazoo.client import KazooClient
    from openeogeotrellis.configparams import ConfigParams

    cluster_id = 'openeo-' + env
    zk = KazooClient(hosts=','.join(ConfigParams().zookeepernodes))
    zk.start()

    try:
        Traefik(zk).add_load_balanced_server(cluster_id=cluster_id, server_id="0", host=host, port=port,
                                             environment=env)
    finally:
        zk.stop()
        zk.close()
