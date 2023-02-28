import pathlib
import textwrap
from pathlib import Path
import pytest

from openeogeotrellis.config import (
    GpsBackendConfig,
    gps_backend_config,
    flush_gps_backend_config,
)

SIMPLE_CONFIG = """
    from openeogeotrellis.config import GpsBackendConfig
    config = GpsBackendConfig()
    """

CUSTOM_CONFIG = """
    from openeogeotrellis.config import GpsBackendConfig

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
    @pytest.mark.parametrize("path_type", [str, Path])
    def test_from_py_file_defaults(self, tmp_path, path_type):
        config_path = get_config_file(tmp_path=tmp_path, content=SIMPLE_CONFIG)
        config = GpsBackendConfig.from_py_file(path_type(config_path))
        assert isinstance(config, GpsBackendConfig)


class TestGetGpsBackendConfig:
    @pytest.fixture(autouse=True)
    def _flush_gps_backend_config(self):
        # Make sure config cached is cleared before and after each test
        flush_gps_backend_config()
        yield
        flush_gps_backend_config()

    def test_gps_backend_config_default(self, tmp_path):
        config = gps_backend_config()
        assert isinstance(config, GpsBackendConfig)

    def test_gps_backend_config_custom(self, tmp_path, monkeypatch):
        config_path = get_config_file(
            tmp_path=tmp_path, content=CUSTOM_CONFIG.format(id="custom")
        )
        monkeypatch.setenv("OPENEO_BACKEND_CONFIG", str(config_path))
        config = gps_backend_config()
        assert isinstance(config, GpsBackendConfig)
        assert type(config).__name__ == "CustomConfig"
        assert config.id == "custom"

    def test_gps_backend_config_lazy_cache(self, tmp_path, monkeypatch):
        config_path = get_config_file(
            tmp_path=tmp_path, content=CUSTOM_CONFIG.format(id="lazy+cache")
        )
        monkeypatch.setenv("OPENEO_BACKEND_CONFIG", str(config_path))
        config = gps_backend_config()
        assert isinstance(config, GpsBackendConfig)
        assert type(config).__name__ == "CustomConfig"
        assert config.id == "lazy+cache"

        # Second call without changes
        assert gps_backend_config() is config

        # Overwrite config file
        config_path = get_config_file(
            tmp_path=tmp_path, content=CUSTOM_CONFIG.format(id="something else")
        )
        monkeypatch.setenv("OPENEO_BACKEND_CONFIG", str(config_path))
        assert gps_backend_config() is config

        # Remove config file
        config_path.unlink()
        assert gps_backend_config() is config

        # Force reload should fail
        with pytest.raises(FileNotFoundError):
            _ = gps_backend_config(force_reload=True)
