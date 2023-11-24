from openeogeotrellis.integrations.prometheus import Prometheus


def test_endpoint_property():
    prometheus = Prometheus("https://prometheus.example.org")

    assert prometheus.endpoint == "https://prometheus.example.org"
