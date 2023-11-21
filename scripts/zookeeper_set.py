import argparse

from kazoo.exceptions import NodeExistsError
from openeogeotrellis.utils import zk_client


def zk_set(hosts: str, path: str, value: bytes):
    with zk_client(hosts) as zk:
        try:
            zk.create(path, value, makepath=True)
        except NodeExistsError:
            _, stat = zk.get(path)
            zk.set(path, value, version = stat.version)


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--servers", help = "Comma separated list of zookeeper hosts/servers")
parser.add_argument("-p", "--path", default = "/openeo/config/users/testuser/concurrent_pod_limit", help = "Path to the node")
parser.add_argument("-v", "--value", help = "Value to set")
args = parser.parse_args()
value_encoded = args.value.encode()
zk_set(args.hosts, args.path, value_encoded)
