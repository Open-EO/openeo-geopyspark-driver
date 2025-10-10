import logging

import pytest

from openeogeotrellis.config import GpsBackendConfig
from openeogeotrellis.udf import UdfRuntimeSpecified
from openeogeotrellis.udf.udf_runtime_images import UdfRuntimeImageRepository, _ImageData, _UdfRuntimeAndVersion


class TestUdfRuntimeImageRepository:
    def test_get_udf_runtimes_response_empty(self):
        repo = UdfRuntimeImageRepository(images=[])
        assert repo.get_udf_runtimes_response() == {}

    def test_get_udf_runtimes_response_basic(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(
                    image_ref="docker.example/openeo:1.2.3",
                    udf_runtimes=[_UdfRuntimeAndVersion("Python", "3.11")],
                    udf_runtime_libraries={"numpy": "12.34"},
                ),
            ]
        )
        assert repo.get_udf_runtimes_response() == {
            "Python": {
                "title": "Python",
                "type": "language",
                "default": "3.11",
                "versions": {"3.11": {"libraries": {"numpy": {"version": "12.34"}}}},
            }
        }

    def test_get_udf_runtimes_response_default_and_library_merging(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(
                    image_ref="docker.example/openeo:38",
                    udf_runtimes=[
                        _UdfRuntimeAndVersion("Python", "3.8", preference=38),
                        _UdfRuntimeAndVersion("Python", "3", preference=380),
                        _UdfRuntimeAndVersion("Python-Jep", "3.8", preference=3000),
                    ],
                    udf_runtime_libraries={"numpy": "1.2", "pandas": "8.8"},
                ),
                _ImageData(
                    image_ref="docker.example/openeo:311",
                    udf_runtimes=[
                        _UdfRuntimeAndVersion("Python", "3.11", preference=311),
                        _UdfRuntimeAndVersion("Python", "3", preference=3110),
                        _UdfRuntimeAndVersion("Python-Jep", "3.11", preference=0),
                    ],
                    udf_runtime_libraries={"numpy": "1.2", "pandas": "11.11", "xarray": "3011"},
                ),
            ]
        )
        assert repo.get_udf_runtimes_response() == {
            "Python": {
                "title": "Python",
                "type": "language",
                "default": "3",
                "versions": {
                    "3": {
                        "libraries": {
                            "numpy": {"version": "1.2"},
                            "pandas": {"version": "8.8|11.11"},
                            "xarray": {"version": "n/a|3011"},
                        }
                    },
                    "3.8": {
                        "libraries": {
                            "numpy": {"version": "1.2"},
                            "pandas": {"version": "8.8"},
                        }
                    },
                    "3.11": {
                        "libraries": {
                            "numpy": {"version": "1.2"},
                            "pandas": {"version": "11.11"},
                            "xarray": {"version": "3011"},
                        }
                    },
                },
            },
            "Python-Jep": {
                "title": "Python-Jep",
                "type": "language",
                "default": "3.8",
                "versions": {
                    "3.8": {
                        "libraries": {
                            "numpy": {"version": "1.2"},
                            "pandas": {"version": "8.8"},
                        }
                    },
                    "3.11": {
                        "libraries": {
                            "numpy": {"version": "1.2"},
                            "pandas": {"version": "11.11"},
                            "xarray": {"version": "3011"},
                        }
                    },
                },
            }
        }

    def test_get_udf_runtimes_response_from_config_batch_runtime_to_image_python(self):
        config = GpsBackendConfig(
            batch_runtime_to_image={
                "python38": "docker.example/openeo:1.2.8",
                "python311": "docker.example/openeo:1.2.11",
            }
        )
        repo = UdfRuntimeImageRepository.from_config(config=config)
        assert repo.get_udf_runtimes_response() == {
            "Python": {
                "title": "Python",
                "type": "language",
                "default": "3",
                "versions": {
                    "3": {"libraries": {}},
                    "3.11": {"libraries": {}},
                    "3.8": {"libraries": {}},
                },
            },
            "Python-Jep": {
                "title": "Python-Jep",
                "type": "language",
                "default": "3",
                "versions": {
                    "3": {"libraries": {}},
                    "3.11": {"libraries": {}},
                    "3.8": {"libraries": {}},
                },
            },
        }

    def test_get_udf_runtimes_response_from_config_batch_runtime_to_image_generic(self):
        config = GpsBackendConfig(
            batch_runtime_to_image={
                "foo": "docker.example/openeo-foo:1.2.3",
                "bar": "docker.example/openeo-bar:1.3.5",
            }
        )
        repo = UdfRuntimeImageRepository.from_config(config=config)
        assert repo.get_udf_runtimes_response() == {
            "Python": {
                "title": "Python",
                "type": "language",
                "default": "bar",
                "versions": {
                    "foo": {"libraries": {}},
                    "bar": {"libraries": {}},
                },
            },
            "Python-Jep": {
                "title": "Python-Jep",
                "type": "language",
                "default": "bar",
                "versions": {
                    "foo": {"libraries": {}},
                    "bar": {"libraries": {}},
                },
            },
        }

    def test_get_default_image_basic(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(image_ref="docker.test/openeo:1.2.3", preference=123),
                _ImageData(image_ref="docker.test/openeo:3.4.5", preference=345),
                _ImageData(image_ref="docker.test/openeo:5.6.7", preference=-20),
            ]
        )
        assert repo.get_default_image() == "docker.test/openeo:3.4.5"

    def test_get_default_image_from_config_batch_runtime_to_image_python(self):
        config = GpsBackendConfig(
            batch_runtime_to_image={
                "python38": "docker.example/openeo:3.8",
                "python311": "docker.example/openeo:3.11",
            }
        )
        repo = UdfRuntimeImageRepository.from_config(config=config)
        assert repo.get_default_image() == "docker.example/openeo:3.11"

    def test_get_all_image_refs_and_aliases(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(image_ref="docker.test/openeo:3.8", image_aliases=["py38"]),
                _ImageData(image_ref="docker.test/openeo:3.11", image_aliases=["python311", "default"]),
            ]
        )
        assert repo.get_all_image_refs_and_aliases() == {
            "default",
            "docker.test/openeo:3.11",
            "docker.test/openeo:3.8",
            "py38",
            "python311",
        }

    def test_get_image_from_udf_runtimes_empty_from_config_batch_runtime_to_image_python(self):
        """
        Legacy `batch_runtime_to_image` config.
        Empty runtimes should result in default image.
        """
        config = GpsBackendConfig(
            batch_runtime_to_image={
                "python38": "docker.test/openeo:3.8",
                "python311": "docker.test/openeo:3.11",
            }
        )
        repo = UdfRuntimeImageRepository.from_config(config=config)
        assert repo.get_image_from_udf_runtimes(runtimes=[]) == "docker.test/openeo:3.11"

    def test_get_image_from_udf_runtimes_basic(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(
                    image_ref="docker.test/openeo:1.2.3",
                    udf_runtimes=[_UdfRuntimeAndVersion("Python", "3.11")],
                ),
            ]
        )
        for version in ["3.8", "3", None]:
            assert (
                repo.get_image_from_udf_runtimes(runtimes=[UdfRuntimeSpecified(name="Python", version=version)])
                == "docker.test/openeo:1.2.3"
            )

    @pytest.mark.parametrize(
        ["versions_specified", "expected"],
        [
            ([], "docker.test/openeo:3.11"),
            (["3.11"], "docker.test/openeo:3.11"),
            (["3.14"], "docker.test/openeo:3.14-alpha"),
            (["3"], "docker.test/openeo:3.11"),
            ([None], "docker.test/openeo:3.11"),
            (["3", "3.11"], "docker.test/openeo:3.11"),
            ([None, "3", "3.11"], "docker.test/openeo:3.11"),
            (["3", "3.14"], "docker.test/openeo:3.14-alpha"),
            (["3.14", "3"], "docker.test/openeo:3.14-alpha"),
            ([None, "3", "3.14"], "docker.test/openeo:3.14-alpha"),
        ],
    )
    def test_get_image_from_udf_runtimes_resolve(self, versions_specified, expected, caplog):
        caplog.set_level(logging.WARNING)
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(
                    image_ref="docker.test/openeo:3.11",
                    preference=100,
                    udf_runtimes=[
                        _UdfRuntimeAndVersion("Python", "3.11"),
                        _UdfRuntimeAndVersion("Python", "3"),
                    ],
                ),
                _ImageData(
                    image_ref="docker.test/openeo:3.14-alpha",
                    preference=1,
                    udf_runtimes=[
                        _UdfRuntimeAndVersion("Python", "3.14"),
                        _UdfRuntimeAndVersion("Python", "3"),
                    ],
                ),
            ]
        )
        runtimes = [UdfRuntimeSpecified(name="Python", version=v) for v in versions_specified]
        assert repo.get_image_from_udf_runtimes(runtimes=runtimes) == expected

        assert caplog.messages == []

    def test_alias_map_basic(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(image_ref="docker.test/openeo:3.8", image_aliases=["py38"]),
                _ImageData(image_ref="docker.test/openeo:3.11", image_aliases=["python311", "default"]),
            ]
        )
        assert repo._alias_map() == {
            "py38": "docker.test/openeo:3.8",
            "python311": "docker.test/openeo:3.11",
            "default": "docker.test/openeo:3.11",
        }

    def test_alias_map_case_and_preference(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(image_ref="docker.test/foo", image_aliases=["PY38"], preference=100),
                _ImageData(image_ref="docker.test/bar", image_aliases=["py38", "Python3.8"], preference=10),
            ]
        )
        assert repo._alias_map() == {
            "py38": "docker.test/foo",
            "python3.8": "docker.test/bar",
        }

    def test_resolve_image_alias(self):
        repo = UdfRuntimeImageRepository(
            images=[
                _ImageData(image_ref="docker.test/openeo:3.8", image_aliases=["py38"]),
                _ImageData(image_ref="docker.test/openeo:3.11", image_aliases=["python311", "default"]),
            ]
        )
        assert repo.resolve_image_alias(None) is None
        assert repo.resolve_image_alias("py38") == "docker.test/openeo:3.8"
        assert repo.resolve_image_alias("PY38") == "docker.test/openeo:3.8"
        assert repo.resolve_image_alias("python311") == "docker.test/openeo:3.11"
        assert repo.resolve_image_alias("PythOn311") == "docker.test/openeo:3.11"
        assert repo.resolve_image_alias("default") == "docker.test/openeo:3.11"
        assert repo.resolve_image_alias("docker.test/openeo:3.8") == "docker.test/openeo:3.8"
        assert repo.resolve_image_alias("docker.test/openeo:2.7") == "docker.test/openeo:2.7"
