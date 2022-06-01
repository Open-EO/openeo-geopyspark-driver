import logging
import uuid

from kazoo.client import KazooClient

_log = logging.getLogger(__name__)


class Traefik:

    def __init__(self, zk: KazooClient, prefix: str = "traefik"):
        self._zk = zk
        self._prefix = prefix

    def proxy_service(self, service_id, host, port) -> None:
        """
        Routes requests to a dynamically created service.

        Creates a backend, a backend server and a frontend rule specifically for this service.

        :param service_id: the services's unique ID, typically a UUID
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

        self._create_tservice_server(tservice_id=tservice_id, server_id='server1', host=host, port=port)
        self._create_middleware_strip_prefix_regexes(middleware_id, *regexes)
        self._create_router_rule(router_id, tservice_id, match_specific_service, priority, middleware_id)

        self._trigger_configuration_update()

    def add_load_balanced_server(self, cluster_id, server_id, host, port, environment) -> None:
        """
        Adds a server to a particular load-balanced cluster.

        Always creates a backend server; the backend and frontend are created only if they don't already exist.

        :param cluster_id: identifies the cluster, e.g. "openeo-prod" or "0"
        :param server_id: uniquely identifies the server, e.g. a UUID or even "192.168.207.194:40661"
        :param host: hostname or IP of the server
        :param port: the port the service runs on
        """

        if environment == 'prod':
            match_openeo = "Host(`openeo.vgt.vito.be`,`openeo.vito.be`) && " \
                           "PathPrefix(`/openeo`,`/.well-known/openeo`)"
        else:
            match_openeo = "Host(`openeo-dev.vgt.vito.be`,`openeo-dev.vito.be`) && " \
                           "PathPrefix(`/openeo`,`/.well-known/openeo`)"

        self._create_tservice_server(tservice_id=cluster_id, server_id=server_id, host=host, port=port)
        self._setup_load_balancer_health_check(tservice_id=cluster_id)
        self._create_router_rule(router_id=cluster_id, tservice_id=cluster_id, matcher=match_openeo, priority=100)

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
        _log.info(f"Create service {url_key}: {url}")
        self._zk_merge(url_key, url.encode())

    def _setup_load_balancer_health_check(self, tservice_id: str):
        tservice_key = self._tservice_key(tservice_id)
        _log.info(f"Setup loadBalancer healthCheck for {tservice_key}")
        self._zk_merge(f"{tservice_key}/loadBalancer/healthCheck/path", b"/openeo/1.0/health?from=TraefikLoadBalancer")
        self._zk_merge(f"{tservice_key}/loadBalancer/healthCheck/interval", b"60s")
        # TODO: very liberal timeout for now
        self._zk_merge(f"{tservice_key}/loadBalancer/healthCheck/timeout", b"20s")

    def _create_router_rule(self, router_id, tservice_id, matcher, priority: int, *middleware_ids):
        router_key = self._router_key(router_id)
        _log.info(f"Create router rule for {router_key}")

        self._zk_merge(f"{router_key}/entrypoints", b"web")
        self._zk_merge(f"{router_key}/service", tservice_id.encode())
        self._zk_merge(f"{router_key}/priority", str(priority).encode())
        self._zk_merge(f"{router_key}/rule", matcher.encode())

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
