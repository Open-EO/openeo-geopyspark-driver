import logging
import os
import uuid

from kazoo.client import KazooClient
from typing import Optional

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams

_log = logging.getLogger(__name__)


class Traefik:
    def __init__(self, zk: KazooClient, prefix: str = "traefik"):
        self._zk = zk
        self._prefix = prefix

    def proxy_service(self, service_id, host, port) -> None:
        """
        Routes requests to a dynamically created secondary service.

        Creates a service, a server and a router rule specifically for this service.

        :param service_id: the secondary services's unique ID, typically a UUID
        :param host: the host the service runs on, typically myself
        :param port: the port the service runs on, typically random
        """

        priority = 200  # matches first

        tservice_id = self._tservice_id(service_id)
        middleware_id = self._middleware_id(service_id)
        router_id = self._router_id(service_id)

        regexes = [f"/openeo/services/{service_id}", f"/openeo/{{version}}/services/{service_id}"]
        path_prefixes = [f"`{path_prefix}`" for path_prefix in regexes]
        match_specific_service = f"PathPrefix({','.join(path_prefixes)})"

        self._create_tservice_server(tservice_id=tservice_id, server_id="server1", host=host, port=port)
        self._create_middleware_strip_prefix_regexes(middleware_id, *regexes)
        self._create_router_rule(router_id, tservice_id, match_specific_service, priority, middleware_id)

        self._trigger_configuration_update()

    def add_load_balanced_server(self, cluster_id, server_id, host, port, rule, health_check: Optional[str]) -> None:
        """
        Adds an HTTP server to a particular load-balanced cluster (a "service" in Traefik parlance). Requests will be
        routed to this cluster if they match the specified router rule.

        Creates a service, a router rule that routes to this service and a server to be part of this service. More
        correctly, it will update existing services/routers/servers because they are all identified by particular IDs.

        :param cluster_id: uniquely identifies the load-balanced cluster of servers, e.g. "openeo-prod"
        :param server_id: uniquely identifies the server, e.g. "epod-openeo-1.vgt.vito.be"
        :param host: hostname or IP of the HTTP server, e.g. "192.168.207.60"
        :param port: port of the HTTP server, e.g. 43845
        :param rule: the router rule, e.g. "Host(`openeo.vito.be`)"
        :param health_check: path to perform a health check, e.g. "/openeo/1.0/health"
        """

        self._create_tservice_server(tservice_id=cluster_id, server_id=server_id, host=host, port=port)
        if health_check:
            self._setup_load_balancer_health_check(tservice_id=cluster_id, path=health_check)
        self._create_router_rule(router_id=cluster_id, tservice_id=cluster_id, rule=rule, priority=100)

        self._trigger_configuration_update()

    def unproxy_service(self, *service_ids) -> None:
        """
        Removes routes to dynamically created services.
        """

        for service_id in service_ids:
            router_key = self._router_key(self._router_id(service_id))
            middleware_key = self._middleware_key(self._middleware_id(service_id))
            tservice_key = self._tservice_key(self._tservice_id(service_id))

            self._zk.delete(router_key, recursive=True)
            self._zk.delete(middleware_key, recursive=True)
            self._zk.delete(tservice_key, recursive=True)

        # prevents "KV connection error: middlewares cannot be a standalone element"
        middlewares_key = f"/{self._prefix}/http/middlewares"
        if not self._zk.get_children(middlewares_key):
            self._zk.delete(middlewares_key)

        self._trigger_configuration_update()

    def _create_tservice_server(self, tservice_id, server_id, host, port) -> None:
        tservice_key = self._tservice_key(tservice_id)
        url_key = f"{tservice_key}/loadBalancer/servers/{server_id}/url"
        url = f"http://{host}:{port}"
        _log.info(f"Create server {url_key}: {url}")
        self._zk_merge(url_key, url.encode())

    def _setup_load_balancer_health_check(self, tservice_id: str, path: str):
        tservice_key = self._tservice_key(tservice_id)
        _log.info(f"Setup loadBalancer healthCheck for {tservice_key}")
        self._zk_merge(f"{tservice_key}/loadBalancer/healthCheck/path", path.encode())
        self._zk_merge(f"{tservice_key}/loadBalancer/healthCheck/interval", b"60s")
        # TODO: very liberal timeout for now
        self._zk_merge(f"{tservice_key}/loadBalancer/healthCheck/timeout", b"20s")

    def _create_router_rule(self, router_id, tservice_id, rule, priority: int, *middleware_ids):
        router_key = self._router_key(router_id)
        _log.info(f"Create router rule for {router_key}")

        self._zk_merge(f"{router_key}/entrypoints", b"web")
        self._zk_merge(f"{router_key}/service", tservice_id.encode())
        self._zk_merge(f"{router_key}/priority", str(priority).encode())
        self._zk_merge(f"{router_key}/rule", rule.encode())

        for i, middleware_id in enumerate(middleware_ids):
            self._zk_merge(f"{router_key}/middlewares/{i}", middleware_id.encode())

    def _create_middleware_strip_prefix_regexes(self, middleware_id, *regexes):
        middleware_key = self._middleware_key(middleware_id)

        for i, pattern in enumerate(regexes):
            self._zk_merge(f"{middleware_key}/stripPrefixRegex/regex/{i}", pattern.encode())

    def _zk_merge(self, path, value):
        self._zk.ensure_path(path)
        self._zk.set(path, value)

    def _trigger_configuration_update(self):
        # https://github.com/containous/traefik/issues/2068 but seems to work with any child node of /traefik
        random_child_node = f"/{self._prefix}/{uuid.uuid4()}"
        self._zk.create(random_child_node)
        self._zk.delete(random_child_node)

    def _tservice_id(self, service_id):
        return f"service{service_id}"

    def _tservice_key(self, tservice_id):
        return f"/{self._prefix}/http/services/{tservice_id}"

    def _router_id(self, service_id):
        return f"router{service_id}"

    def _router_key(self, router_id):
        return f"/{self._prefix}/http/routers/{router_id}"

    def _middleware_id(self, service_id):
        return f"middleware{service_id}"

    def _middleware_key(self, middleware_id):
        return f"/{self._prefix}/http/middlewares/{middleware_id}"


def update_zookeeper(cluster_id: str, rule: str, host: str, port: int, health_check: str = None) -> None:
    # TODO: get this from GpsBackendConfig instead
    server_id = os.environ.get("OPENEO_TRAEFIK_SERVER_ID", host)

    zk = KazooClient(hosts=",".join(get_backend_config().zookeeper_nodes))
    zk.start()
    try:
        Traefik(zk).add_load_balanced_server(
            cluster_id=cluster_id, server_id=server_id, host=host, port=port, rule=rule, health_check=health_check
        )
    finally:
        zk.stop()
        zk.close()
