import textwrap
from pathlib import Path

import attrs
import pytest

from openeogeotrellis.config import (
    GpsBackendConfig,
    get_backend_config,
)

SIMPLE_CONFIG = """
    from openeogeotrellis.config import GpsBackendConfig
    config = GpsBackendConfig()
    """

CUSTOM_CONFIG = """
    import attrs
    from openeogeotrellis.config import GpsBackendConfig

    @attrs.frozen
    class CustomConfig(GpsBackendConfig):
        id: str = "{id}"

    config = CustomConfig()
    """


def get_config_file(
    tmp_path: Path, content: str = SIMPLE_CONFIG, filename: str = "testconfig.py"
) -> Path:
    config_path = tmp_path / filename
    config_path.write_text(textwrap.dedent(content))
    return config_path


class TestGpsBackendConfig:
    def test_all_defaults(self):
        """Test that config can be created without arguments: everything has default value"""
        config = GpsBackendConfig()
        assert isinstance(config, GpsBackendConfig)

    def test_immutability(self):
        config = GpsBackendConfig(id="foo")
        assert config.id == "foo"
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            config.id = "bar"
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            config.oidc_providers = []
        assert config.id == "foo"


class TestGetGpsBackendConfig:
    @pytest.fixture(autouse=True)
    def _flush_get_backend_config(self):
        # Make sure config cached is cleared before and after each test
        get_backend_config.flush()
        yield
        get_backend_config.flush()

    def test_get_backend_config_default(self, tmp_path):
        config = get_backend_config()
        assert isinstance(config, GpsBackendConfig)

    def test_get_backend_config_custom(self, tmp_path, monkeypatch):
        config_path = get_config_file(
            tmp_path=tmp_path, content=CUSTOM_CONFIG.format(id="custom")
        )
        monkeypatch.setenv("OPENEO_BACKEND_CONFIG", str(config_path))
        config = get_backend_config()
        assert isinstance(config, GpsBackendConfig)
        assert type(config).__name__ == "CustomConfig"
        assert config.id == "custom"

    def test_get_backend_config_lazy_cache(self, tmp_path, monkeypatch):
        config_path = get_config_file(
            tmp_path=tmp_path, content=CUSTOM_CONFIG.format(id="lazy+cache")
        )
        monkeypatch.setenv("OPENEO_BACKEND_CONFIG", str(config_path))
        config = get_backend_config()
        assert isinstance(config, GpsBackendConfig)
        assert type(config).__name__ == "CustomConfig"
        assert config.id == "lazy+cache"

        # Second call without changes
        assert get_backend_config() is config

        # Overwrite config file
        config_path = get_config_file(
            tmp_path=tmp_path, content=CUSTOM_CONFIG.format(id="something else")
        )
        monkeypatch.setenv("OPENEO_BACKEND_CONFIG", str(config_path))
        assert get_backend_config() is config

        # Remove config file
        config_path.unlink()
        assert get_backend_config() is config

        # Force reload should fail
        with pytest.raises(FileNotFoundError):
            _ = get_backend_config(force_reload=True)

    def test_default_config(self, monkeypatch):
        monkeypatch.delenv("OPENEO_BACKEND_CONFIG")
        config = get_backend_config()
        assert config.id == "gps-default"


class TestConfigValues:
    def test_zookeeper_hosts_env_var_parsing(self, monkeypatch):
        monkeypatch.setenv("ZOOKEEPERNODES", "z1.test,zk2.test")
        assert GpsBackendConfig().zookeeper_hosts == ["z1.test", "zk2.test"]

        monkeypatch.setenv("ZOOKEEPERNODES", "")
        assert GpsBackendConfig().zookeeper_hosts == []

    def test_zookeeper_root_path(self, monkeypatch):
        """Test slash validation and trimming."""
        config = GpsBackendConfig(zookeeper_root_path="/openeo.test/")
        assert config.zookeeper_root_path == "/openeo.test"

        with pytest.raises(ValueError, match="must match regex"):
            _ = GpsBackendConfig(zookeeper_root_path="openeo/test")
