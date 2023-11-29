"""
Generic command line tool to set (configuration) values in Zookeeper.

Usage instructions:

1.  Make sure to point to the config file for the target deployment,
    so that correct zookeeper hosts (and other settings) are picked up automatically.

    - through env var OPENEO_BACKEND_CONFIG:

            export OPENEO_BACKEND_CONFIG=tests/backend_config.py

    - or by passing the --config argument

            python zookeeper_set.py --config tests/backend_config.py ...

    Some deploy environments use dynamic ZooKeeper host configs (e.g. in Kubernetes context),
    in which case naively loading the config file is not enough to properly obtain the correct ZooKeeper hosts.
    It is possible to explicitly specify/override the ZooKeeper hosts to use through the `--zk-hosts` argument:

        python zookeeper_set.py --config tests/backend_config.py --zk-hosts zk1:2181,zk2:2181 ...

2.  Set the desired ZooKeeper path (absolute or relative) and value, e.g.

        python zookeeper_set.py --config tests/backend_config.py config/users/testuser/concurrent_pod_limit 5

    Use `--dry-run` to see what would happen without actually doing anything.
"""
import argparse
import logging
import os
from pathlib import Path
from typing import Union
import json
from kazoo.exceptions import NodeExistsError

from openeo_driver.util.logging import setup_logging
from openeogeotrellis.config.load import GpsConfigGetter
from openeogeotrellis.utils import zk_client
from openeogeotrellis.config import get_backend_config

_log = logging.getLogger(Path(__file__).name)


def zk_set(hosts: str, path: str, value: Union[bytes, str, dict, int, float, list]):
    if isinstance(value, str):
        value = value.encode("utf-8")
    elif isinstance(value, (dict, list, int, float)):
        value = json.dumps(value).encode("utf-8")

    with zk_client(hosts, timeout=5) as zk:
        try:
            zk.create(path, value, makepath=True)
            _log.info(f"Created new ZK node {path=} with {value=}")
        except NodeExistsError:
            orig, stat = zk.get(path)
            _log.info(f"Overwriting existing ZK node {path=} ({orig=}) with {value=}")
            zk.set(path, value, version=stat.version)


def main():
    setup_logging()

    # Handle command line interface
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        help="""Zookeeper path to set.
            Can be given relative to configured zookeeper root (e.g. 'config/users/testuser/concurrent_pod_limit')"
            or absolutely to ignore the configured zookeeper root (e.g. '/openeo/config/users/testuser/concurrent_pod_limit').
        """,
    )
    parser.add_argument("value", help="Value to set.")

    parser.add_argument(
        "--config",
        help=f"Path to `GpsBackendConfig` config file to be used instead of what is set up through env var {GpsConfigGetter.OPENEO_BACKEND_CONFIG}.",
    )
    parser.add_argument(
        "--zk-hosts",
        help="Optional comma separated list of zookeeper hosts to use instead of configured value.",
    )
    parser.add_argument("--dry-run", help="Dry run mode.", action="store_true")
    args = parser.parse_args()

    if args.config:
        # TODO: cleaner way of getting config from concreate path iso env var
        os.environ[GpsConfigGetter.OPENEO_BACKEND_CONFIG] = args.config
    config = get_backend_config(show_stack=False)

    zookeeper_hosts: str = args.zk_hosts or ",".join(config.zookeeper_hosts)
    _log.info(f"Using {zookeeper_hosts=}")

    path = args.path
    if not path.startswith("/"):
        path = f"{config.zookeeper_root_path}/{path}"
    value = args.value

    _log.info(f"Setting {path=}: {value=}")
    if args.dry_run:
        _log.info("Dry run mode: not actually setting anything")
    else:
        zk_set(zookeeper_hosts, path, value)


if __name__ == "__main__":
    main()
