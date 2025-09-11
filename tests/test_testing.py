import pytest
from kazoo.exceptions import BadVersionError, NoNodeError

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.testing import KazooClientMock, _ZNodeStat, gps_config_overrides


def test_kazoo_mock_basic():
    client = KazooClientMock()
    assert client.dump() == {'/': b''}


def test_kazoo_mock_create_simple():
    client = KazooClientMock()
    client.create('/foo', b'd6t6')
    assert client.dump() == {
        '/': b'',
        '/foo': b'd6t6'
    }


def test_kazoo_mock_create_multiple():
    client = KazooClientMock()
    client.create('/foo', b'd6t6')
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.dump() == {
        '/': b'',
        '/foo': b'd6t6',
        '/bar': b'',
        '/bar/baz': b'b6r',
    }


def test_kazoo_mock_get():
    client = KazooClientMock()
    client.create('/foo', b'd6t6')
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.get('/foo') == (b'd6t6', _ZNodeStat(1))
    assert client.get('/bar') == (b'', _ZNodeStat(1))
    assert client.get('/bar/baz') == (b'b6r', _ZNodeStat(1))


def test_kazoo_mock_set():
    client = KazooClientMock()
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.get('/bar/baz') == (b'b6r', _ZNodeStat(1))
    client.set('/bar/baz', b'x3v')
    assert client.get('/bar/baz') == (b'x3v', _ZNodeStat(2))
    client.set('/bar/baz', b'l0l', version=2)
    assert client.get('/bar/baz') == (b'l0l', _ZNodeStat(3))
    with pytest.raises(BadVersionError):
        client.set('/bar/baz', b'l0l', version=2)


def test_kazoo_mock_delete():
    client = KazooClientMock()
    client.create('/bar/baz', b'b6r', makepath=True)
    assert client.get('/bar/baz') == (b'b6r', _ZNodeStat(1))
    client.delete('/bar/baz')
    with pytest.raises(NoNodeError):
        client.get('/bar/baz')


def test_kazoo_mock_children():
    client = KazooClientMock()
    client.create('/bar/baz', b'b6r', makepath=True)
    client.create('/bar/fii', b'f000', makepath=True)
    assert client.get_children('/') == ['bar']
    assert client.get_children('/bar') == ['baz', 'fii']
    assert client.get_children('/bar/fii') == []


class TestGpsConfigOverrides:
    def test_baseline(self):
        assert get_backend_config().id == "gps-test-dummy"

    def test_context(self):
        assert get_backend_config().id == "gps-test-dummy"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "gps-test-dummy"

    def test_context_nesting(self):
        assert get_backend_config().id == "gps-test-dummy"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
            with gps_config_overrides(id="hello-again"):
                assert get_backend_config().id == "hello-again"
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "gps-test-dummy"

    @pytest.fixture
    def special_stuff(self):
        with gps_config_overrides(id="hello-fixture"):
            yield

    def test_fixture(self, special_stuff):
        assert get_backend_config().id == "hello-fixture"

    def test_fixture_and_context(self, special_stuff):
        assert get_backend_config().id == "hello-fixture"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "hello-fixture"

    @gps_config_overrides(id="hello-decorator")
    def test_decorator(self):
        assert get_backend_config().id == "hello-decorator"

    @gps_config_overrides(id="hello-decorator")
    def test_decorator_and_context(self):
        assert get_backend_config().id == "hello-decorator"
        with gps_config_overrides(id="hello-inline-context"):
            assert get_backend_config().id == "hello-inline-context"
        assert get_backend_config().id == "hello-decorator"

    @gps_config_overrides(id="hello-decorator")
    def test_decorator_vs_fixture(self, special_stuff):
        assert get_backend_config().id == "hello-decorator"
