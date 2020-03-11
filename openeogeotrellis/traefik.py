from kazoo.client import KazooClient


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
        backend_id = "backend%s" % service_id
        frontend_id = "frontend%s" % service_id
        match_specific_service = "PathPrefixStripRegex: /openeo/services/%s,/openeo/{version}/services/%s" \
                                 % (service_id, service_id)

        self._create_backend_server(backend_id, 'server1', host, port)
        self._create_frontend_rule(frontend_id, backend_id, match_specific_service, priority=200)
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

        # FIXME: a Path:/.well-known/openeo matcher is a better fit but that seems to require an additional "test" node
        if environment == 'dev':
            match_openeo = "Host: openeo-dev.vgt.vito.be;PathPrefix: /openeo,/.well-known/openeo"
        else:
            match_openeo = "Host: openeo.vgt.vito.be;PathPrefix: /openeo,/.well-known/openeo"

        self._create_backend_server(cluster_id, server_id, host, port)
        self._create_frontend_rule(cluster_id, cluster_id, match_openeo, priority=100)
        self._trigger_configuration_update()

    def remove(self, service_id):
        # TODO
        pass

    def _create_backend_server(self, backend_id, server_id, host, port) -> None:
        url_key = "/%s/backends/%s/servers/%s/url" % (self._prefix, backend_id, server_id)
        url = "http://%s:%d" % (host, port)
        self._zk_merge(url_key, url.encode())

    def _create_frontend_rule(self, frontend_id, backend_id, match_path, priority: int):
        frontend_key = "/%s/frontends/%s" % (self._prefix, frontend_id)
        test_key = frontend_key + "/routes/test"

        self._zk_merge(frontend_key + "/entrypoints", b"web")
        self._zk_merge(frontend_key + "/backend", backend_id.encode())
        self._zk_merge(frontend_key + "/priority", str(priority).encode())  # higher priority matches first

        self._zk_merge(test_key + "/rule", match_path.encode())

    def _zk_merge(self, path, value):
        self._zk.ensure_path(path)
        self._zk.set(path, value)

    def _trigger_configuration_update(self):
        # https://github.com/containous/traefik/issues/2068
        self._zk.delete("/%s/leader" % self._prefix, recursive=True)
