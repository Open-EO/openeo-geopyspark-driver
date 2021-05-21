from kazoo.client import KazooClient
import uuid


class Traefik:
    # TODO: split in a TraefikV1 and a TraefikV2 implementation?
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
        backend_id = self._backend_id(service_id)
        frontend_id = self._frontend_id(service_id)
        match_specific_service = "PathPrefixStripRegex: " \
                                 f"/openeo/services/{service_id},/openeo/{{version}}/services/{service_id}"

        # FIXME: support Traefik v2, add a middleware to replace PathPrefixStripRegex, see
        #  https://doc.traefik.io/traefik/migration/v1-to-v2/#strip-and-rewrite-path-prefixes

        self._create_backend_server(backend_id=backend_id, server_id='server1', host=host, port=port)
        self._create_frontend_rule(frontend_id, backend_id, match_specific_service, priority=200)  # matches first
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

        def add_load_balanced_server_v1():
            # TODO: a Path:/.well-known/openeo matcher is a better fit but that seems to require an additional
            #   "test" node
            if environment == 'dev':
                match_openeo = "Host: openeo-dev.vgt.vito.be,openeo-dev.vito.be;" \
                               "PathPrefix: /openeo,/.well-known/openeo"
            else:
                match_openeo = "Host: openeo.vgt.vito.be,openeo.vito.be;" \
                               "PathPrefix: /openeo,/.well-known/openeo"

            self._create_backend_server(backend_id=cluster_id, server_id=server_id, host=host, port=port)
            self._create_frontend_rule(frontend_id=cluster_id, backend_id=cluster_id, matcher=match_openeo,
                                       priority=100)

        def add_load_balanced_server_v2():
            if environment == 'dev':
                match_openeo = "Host(`openeo-dev.vgt.vito.be`,`openeo-dev.vito.be`) && " \
                               "PathPrefix(`/openeo`,`/.well-known/openeo`)"
            else:
                match_openeo = "Host(`openeo.vgt.vito.be`,`openeo.vito.be`) && " \
                               "PathPrefix(`/openeo`,`/.well-known/openeo`)"

            self._create_tservice_server(tservice_id=cluster_id, server_id=server_id, host=host, port=port)
            self._create_router_rule(router_id=cluster_id, tservice_id=cluster_id, matcher=match_openeo, priority=100)

        add_load_balanced_server_v1()
        add_load_balanced_server_v2()
        self._trigger_configuration_update()

    def unproxy_service(self, *service_ids) -> None:
        """
        Removes routes to dynamically created services.
        """

        # FIXME: support Traefik v2
        for service_id in service_ids:
            frontend_key = self._frontend_key(self._frontend_id(service_id))
            backend_key = self._backend_key(self._backend_id(service_id))

            self._zk.delete(frontend_key, recursive=True)
            self._zk.delete(backend_key, recursive=True)

        self._trigger_configuration_update()

    def _create_backend_server(self, backend_id, server_id, host, port) -> None:
        backend_key = self._backend_key(backend_id)
        url_key = f"{backend_key}/servers/{server_id}/url"
        url = f"http://{host}:{port}"
        self._zk_merge(url_key, url.encode())

    def _create_frontend_rule(self, frontend_id, backend_id, matcher, priority: int):
        frontend_key = self._frontend_key(frontend_id)

        self._zk_merge(f"{frontend_key}/entrypoints", b"web")
        self._zk_merge(f"{frontend_key}/backend", backend_id.encode())
        self._zk_merge(f"{frontend_key}/priority", str(priority).encode())
        self._zk_merge(f"{frontend_key}/routes/test/rule", matcher.encode())

    def _create_tservice_server(self, tservice_id, server_id, host, port) -> None:
        backend_key = self._tservice_key(tservice_id)
        url_key = f"{backend_key}/loadbalancer/servers/{server_id}/url"
        url = f"http://{host}:{port}"
        self._zk_merge(url_key, url.encode())

    def _create_router_rule(self, router_id, tservice_id, matcher, priority: int):
        frontend_key = self._router_key(router_id)

        self._zk_merge(f"{frontend_key}/entrypoints", b"web")
        self._zk_merge(f"{frontend_key}/service", tservice_id.encode())
        self._zk_merge(f"{frontend_key}/priority", str(priority).encode())
        self._zk_merge(f"{frontend_key}/rule", matcher.encode())

    def _zk_merge(self, path, value):
        self._zk.ensure_path(path)
        self._zk.set(path, value)

    def _trigger_configuration_update(self):
        # https://github.com/containous/traefik/issues/2068 but seems to work with any child node of /traefik
        random_child_node = f"/{self._prefix}/{uuid.uuid4()}"
        self._zk.create(random_child_node)
        self._zk.delete(random_child_node)

    def _backend_id(self, service_id):
        return f"backend{service_id}"

    def _backend_key(self, backend_id):
        return f"/{self._prefix}/backends/{backend_id}"

    def _frontend_id(self, service_id):
        return f"frontend{service_id}"

    def _frontend_key(self, frontend_id) -> str:
        return f"/{self._prefix}/frontends/{frontend_id}"

    def _tservice_key(self, tservice_id):
        return f"/{self._prefix}/http/services/{tservice_id}"

    def _router_key(self, router_id):
        return f"/{self._prefix}/http/routers/{router_id}"
