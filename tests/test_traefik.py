from unittest import mock

from openeogeotrellis.traefik import Traefik


def test_add_load_balanced_server():
    zk = mock.Mock()
    Traefik(zk=zk).add_load_balanced_server(
        cluster_id="openeo-test", server_id="openeo-test-01", host="10.0.0.0", port=123, environment="test",
    )
    zk_sets = {c.args[0]: c.args[1] for c in zk.set.mock_calls}
    assert zk_sets == {
        "/traefik/http/routers/openeo-test/entrypoints": b"web",
        "/traefik/http/routers/openeo-test/service": b"openeo-test",
        "/traefik/http/routers/openeo-test/rule": b"Host(`openeo-dev.vgt.vito.be`,`openeo-dev.vito.be`) && PathPrefix(`/openeo`,`/.well-known/openeo`)",
        "/traefik/http/routers/openeo-test/priority": b"100",
        "/traefik/http/services/openeo-test/loadBalancer/servers/openeo-test-01/url": b"http://10.0.0.0:123",
        "/traefik/http/services/openeo-test/loadBalancer/healthCheck/path": b"/openeo/1.0/health?from=TraefikLoadBalancer",
        "/traefik/http/services/openeo-test/loadBalancer/healthCheck/interval": b"60s",
        "/traefik/http/services/openeo-test/loadBalancer/healthCheck/timeout": b"20s",
    }
