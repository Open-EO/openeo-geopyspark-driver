import logging
import re
import tarfile
import textwrap
import zipfile
from datetime import datetime
from pathlib import Path

import dirty_equals
import pyspark
import pytest
from openeo.udf import StructuredData, UdfData
from openeo_driver.ProcessGraphDeserializer import custom_process_from_process_graph
from openeo_driver.processes import ProcessRegistry
from openeo_driver.util.logging import LOG_HANDLER_FILE_JSON, get_logging_config

from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.config.constants import UDF_DEPENDENCIES_INSTALL_MODE
from openeogeotrellis.deploy.batch_job import run_job
from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.udf import (
    UdfDependencyHandlingFailure,
    assert_running_in_executor,
    build_python_udf_dependencies_archive,
    collect_python_udf_dependencies,
    install_python_udf_dependencies,
    python_udf_dependency_context_from_archive,
    run_udf_code,
    collect_udfs,
    UdfSpecified,
    UdfRuntimeSpecified,
)


def test_assert_running_in_executor_in_driver():
    """In driver: should raise exception"""
    with pytest.raises(RuntimeError, match="Not running in PySpark executor context."):
        assert_running_in_executor()


@pytest.fixture(scope="module")
def spark_context() -> pyspark.SparkContext:
    return pyspark.SparkContext.getOrCreate()


def test_assert_running_in_executor_in_executor(spark_context):
    """In executor: no exception (just return None)"""
    data = spark_context.parallelize([1, 2, 3])
    result = data.map(lambda x: [assert_running_in_executor()] * x).collect()
    assert result == [[None], [None, None], [None, None, None]]


UDF_SQUARES = textwrap.dedent(
    """
    from openeo.udf import UdfData, StructuredData
    def apply_udf_data(data: UdfData):
        xs = data.get_structured_data_list()[0].data
        data.set_structured_data_list([
            StructuredData([x * x for x in xs]),
        ])
    """
)


def test_run_udf_code_in_driver():
    data = UdfData(structured_data_list=[StructuredData([1, 2, 3, 4, 5])])
    with pytest.raises(RuntimeError, match="Not running in PySpark executor context."):
        _ = run_udf_code(code=UDF_SQUARES, data=data)


def test_run_udf_code_in_executor_per_item(spark_context):
    def mapper(x: int):
        data = UdfData(structured_data_list=[StructuredData([x])])
        run_udf_code(code=UDF_SQUARES, data=data)
        return data.get_structured_data_list()[0].data[0]

    rdd = spark_context.parallelize([1, 2, 3, 4, 5])
    result = rdd.map(mapper).collect()
    assert result == [1, 4, 9, 16, 25]


def test_run_udf_code_in_executor_single_udf_data(spark_context):
    data = UdfData(structured_data_list=[StructuredData([1, 2, 3, 4, 5])])
    rdd = spark_context.parallelize([data])
    result = rdd.map(lambda x: run_udf_code(code=UDF_SQUARES, data=x)).collect()
    result = [[l.data for l in r.get_structured_data_list()] for r in result]
    assert result == [[[1, 4, 9, 16, 25]]]


class TestUdfCollection:
    def test_collect_udfs_no_udfs(self):
        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert list(collect_udfs(pg)) == []

    def test_collect_udfs_single_udf(self):
        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "apply1": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "x"},
                                    "runtime": "Python",
                                    "udf": "print('hello world')\n",
                                },
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert list(collect_udfs(pg)) == [
            UdfSpecified(code="print('hello world')\n", runtime=UdfRuntimeSpecified(name="Python", version=None)),
        ]

    def test_collect_udfs_direct_and_remote_udp(self, requests_mock):
        udp_url = "https://udphub.test/apply_foo.udp.json"

        udp = {
            "id": "apply_foo",
            "process_graph": {
                "apply1": {
                    "process_id": "apply",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "process": {
                            "process_graph": {
                                "runudf1": {
                                    "process_id": "run_udf",
                                    "arguments": {
                                        "data": {"from_parameter": "x"},
                                        "udf": "print('Hello 1')",
                                        "runtime": "Python",
                                    },
                                    "result": True,
                                }
                            }
                        },
                    },
                    "result": True,
                }
            },
            "parameters": [
                {"name": "data", "schema": {"type": "object", "subtype": "datacube"}},
            ],
            "returns": {"schema": {"type": "object", "subtype": "datacube"}},
        }
        requests_mock.get(udp_url, json=udp)

        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "applyfoo1": {
                "process_id": "apply_foo",
                "namespace": udp_url,
                "arguments": {"data": {"from_node": "loadcollection1"}},
            },
            "apply2": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "applyfoo1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "x"},
                                    "udf": "print('Hello 2')",
                                    "runtime": "Python",
                                },
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply2"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert sorted(collect_udfs(pg)) == [
            UdfSpecified(code="print('Hello 1')", runtime=UdfRuntimeSpecified(name="Python", version=None)),
            UdfSpecified(code="print('Hello 2')", runtime=UdfRuntimeSpecified(name="Python", version=None)),
        ]

    def test_collect_udfs_from_custom_process(self):
        process_registry = ProcessRegistry()
        process_spec = {
            "id": "custom123",
            "parameters": [
                {"name": "zecube", "schema": {"type": "object", "subtype": "datacube"}},
            ],
            "process_graph": {
                "apply1": {
                    "process_id": "apply",
                    "arguments": {
                        "data": {"from_parameter": "zecube"},
                        "process": {
                            "process_graph": {
                                "runudf1": {
                                    "process_id": "run_udf",
                                    "arguments": {
                                        "data": {"from_parameter": "x"},
                                        "udf": "print('Hello 123')",
                                        "runtime": "Python",
                                    },
                                    "result": True,
                                }
                            }
                        },
                    },
                    "result": True,
                }
            },
        }
        custom_process_from_process_graph(
            process_spec=process_spec, process_registries=[process_registry], namespace="ns123"
        )

        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "custom123": {
                "process_id": "custom123",
                "namespace": "ns123",
                "arguments": {"data": {"from_node": "loadcollection1"}},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "custom123"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert sorted(collect_udfs(pg, process_registry=process_registry)) == [
            UdfSpecified(code="print('Hello 123')", runtime=UdfRuntimeSpecified(name="Python", version=None)),
        ]

    def test_collect_python_udf_dependencies_no_udfs(self):
        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "loadcollection1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == {}

    def test_collect_python_udf_dependencies_no_deps(self):
        udf = textwrap.dedent(
            """
            def foo(x):
            return x + 1
            """
        )
        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "apply1": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {
                                    "data": {"from_parameter": "x"},
                                    "runtime": "Python",
                                    "udf": udf,
                                },
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == {UdfRuntimeSpecified(name="Python", version=None): set()}

    @pytest.mark.parametrize(
        ["run_udf_args", "expected"],
        [
            (
                {
                    "udf": textwrap.dedent(
                        """
                        # /// script
                        # dependencies = [
                        #     "numpy",
                        #     'pandas',
                        # ]
                        # ///
                        def foo(x):
                            return x + 1
                        """
                    ),
                    "runtime": "Python",
                },
                {UdfRuntimeSpecified(name="Python", version=None): {"numpy", "pandas"}},
            ),
            (
                {
                    "udf": textwrap.dedent(
                        """
                        # /// script
                        # dependencies = ["numpy", 'pandas>=1.2.3']
                        # ///
                        def foo(x):
                            return x + 1
                        """
                    ),
                    "runtime": "Python3",
                    "version": "3.1415",
                },
                {UdfRuntimeSpecified(name="Python3", version="3.1415"): {"numpy", "pandas>=1.2.3"}},
            ),
        ],
    )
    def test_collect_python_udf_dependencies_basic(self, run_udf_args, expected):
        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "apply1": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {"data": {"from_parameter": "x"}, **run_udf_args},
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == expected

    def test_collect_python_udf_dependencies_multiple_udfs(self):
        udf1 = textwrap.dedent(
            """
            # /// script
            # dependencies = [
            #     "numpy",
            #     'pandas',
            # ]
            # ///
            def foo(x):
                return x + 1
            """
        )
        udf2 = textwrap.dedent(
            """
            # /// script
            # dependencies = [
            #     "scipy",
            #     'pandas',
            # ]
            # ///
            def foo(x):
                return x + 1
            """
        )
        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "apply1": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {"data": {"from_parameter": "x"}, "udf": udf1, "runtime": "Python"},
                                "result": True,
                            }
                        }
                    },
                },
            },
            "apply2": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "apply1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {"data": {"from_parameter": "x"}, "udf": udf2, "runtime": "Python"},
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply2"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == {
            UdfRuntimeSpecified(name="Python", version=None): {"numpy", "pandas", "scipy"}
        }

    def test_collect_python_udf_dependencies_from_remote_udp(self, requests_mock):
        udp_url = "https://udphub.test/apply_foo.udp.json"
        udf = textwrap.dedent(
            """
            # /// script
            # dependencies = [
            #     "numpy",
            #     'pandas',
            # ]
            # ///
            def foo(x):
                return x + 1
            """
        )
        udp = {
            "id": "apply_foo",
            "process_graph": {
                "apply1": {
                    "process_id": "apply",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "process": {
                            "process_graph": {
                                "runudf1": {
                                    "process_id": "run_udf",
                                    "arguments": {"data": {"from_parameter": "x"}, "udf": udf, "runtime": "Python"},
                                    "result": True,
                                }
                            }
                        },
                    },
                    "result": True,
                }
            },
            "parameters": [
                {"name": "data", "schema": {"type": "object", "subtype": "datacube"}},
            ],
            "returns": {"schema": {"type": "object", "subtype": "datacube"}},
        }
        requests_mock.get(udp_url, json=udp)

        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "applyfoo1": {
                "process_id": "apply_foo",
                "namespace": udp_url,
                "arguments": {"data": {"from_node": "loadcollection1"}},
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "applyfoo1"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == {
            UdfRuntimeSpecified(name="Python", version=None): {"numpy", "pandas"}
        }

    def test_collect_python_udf_dependencies_direct_and_remote_udp(self, requests_mock):
        udp_url = "https://udphub.test/apply_foo.udp.json"
        udf1 = textwrap.dedent(
            """
            # /// script
            # dependencies = ["numpy", 'pandas']
            # ///
            def foo(x):
                return x + 1
            """
        )
        udf2 = textwrap.dedent(
            """
            # /// script
            # dependencies = ["scipy"]
            # ///
            def bar(x):
                return x * 42
            """
        )
        udp = {
            "id": "apply_foo",
            "process_graph": {
                "apply1": {
                    "process_id": "apply",
                    "arguments": {
                        "data": {"from_parameter": "data"},
                        "process": {
                            "process_graph": {
                                "runudf1": {
                                    "process_id": "run_udf",
                                    "arguments": {"data": {"from_parameter": "x"}, "udf": udf1, "runtime": "Python"},
                                    "result": True,
                                }
                            }
                        },
                    },
                    "result": True,
                }
            },
            "parameters": [
                {"name": "data", "schema": {"type": "object", "subtype": "datacube"}},
            ],
            "returns": {"schema": {"type": "object", "subtype": "datacube"}},
        }
        requests_mock.get(udp_url, json=udp)

        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "applyfoo1": {
                "process_id": "apply_foo",
                "namespace": udp_url,
                "arguments": {"data": {"from_node": "loadcollection1"}},
            },
            "apply2": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "applyfoo1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {"data": {"from_parameter": "x"}, "udf": udf2, "runtime": "Python"},
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply2"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == {
            UdfRuntimeSpecified(name="Python", version=None): {"numpy", "pandas", "scipy"}
        }

    def test_collect_python_udf_dependencies_from_remote_udp_resilience(self, requests_mock, caplog):
        caplog.set_level(logging.WARNING)
        udp_url = "https://udphub.test/apply_foo.udp.json"
        requests_mock.get(udp_url, status_code=404, text="nope nope")

        udf2 = textwrap.dedent(
            """
            # /// script
            # dependencies = ["scipy"]
            # ///
            def bar(x):
                return x * 42
            """
        )

        pg = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {"id": "SENTINEL123"},
            },
            "applyfoo1": {
                "process_id": "apply_foo",
                "namespace": udp_url,
                "arguments": {"data": {"from_node": "loadcollection1"}},
            },
            "apply2": {
                "process_id": "apply",
                "arguments": {
                    "data": {"from_node": "applyfoo1"},
                    "process": {
                        "process_graph": {
                            "runudf1": {
                                "process_id": "run_udf",
                                "arguments": {"data": {"from_parameter": "x"}, "udf": udf2, "runtime": "Python"},
                                "result": True,
                            }
                        }
                    },
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "apply2"}, "format": "GTiff", "options": {}},
                "result": True,
            },
        }
        assert collect_python_udf_dependencies(pg) == {UdfRuntimeSpecified(name="Python", version=None): {"scipy"}}

        assert caplog.text == dirty_equals.IsStr(
            regex=r".*collect_udf: skipping failure.*https://udphub.test/apply_foo.udp.json.*ProcessNamespaceInvalid.*",
            regex_flags=re.DOTALL,
        )


class TestInstallPythonUdfDependencies:

    def test_install_python_udf_dependencies_basic(self, tmp_path, dummy_pypi, caplog):
        caplog.set_level("DEBUG")
        install_target = tmp_path / "target"
        assert not install_target.exists()
        install_python_udf_dependencies(["mehh"], target=install_target, index=dummy_pypi)
        assert (install_target / "mehh.py").exists()
        assert "pip install output: Successfully installed mehh-1.2.3" in caplog.text

    @pytest.mark.parametrize("dependency", ["mehh==1.2.3", "mehh>=1.2.3"])
    def test_install_python_udf_dependencies_with_version(self, tmp_path, dummy_pypi, caplog, dependency):
        caplog.set_level("DEBUG")
        install_target = tmp_path / "target"
        assert not install_target.exists()
        install_python_udf_dependencies([dependency], target=install_target, index=dummy_pypi)
        assert (install_target / "mehh.py").exists()
        assert "pip install output: Successfully installed mehh-1.2.3" in caplog.text

    def test_install_python_udf_dependencies_fail(self, tmp_path, dummy_pypi, caplog):
        caplog.set_level("DEBUG")
        install_target = tmp_path / "target"
        with pytest.raises(
            UdfDependencyHandlingFailure, match="pip install of UDF dependencies failed with exit_code=1"
        ):
            install_python_udf_dependencies(["nope-nope"], target=install_target, index=dummy_pypi)
        assert (
            "pip install output: ERROR: Could not find a version that satisfies the requirement nope-nope"
            in caplog.text
        )

    def test_package_python_udf_dependencies_zip_basic(self, tmp_path, dummy_pypi, caplog):
        target = tmp_path / "udf-deps.zip"

        assert not target.exists()
        actual = build_python_udf_dependencies_archive(dependencies=["mehh"], target=target, index=dummy_pypi)
        assert actual == target
        assert target.exists()

        with zipfile.ZipFile(target, "r") as zf:
            assert "mehh.py" in zf.namelist()

        assert re.search(r"Copying .*/archive\.zip \(\d+ bytes\) to .*/udf-deps\.zip", caplog.text)

    @pytest.mark.parametrize("format", ["tar", "gztar"])
    def test_package_python_udf_dependencies_tar_basic(self, tmp_path, dummy_pypi, caplog, format):
        target = tmp_path / "udf-deps.tar"

        assert not target.exists()
        actual = build_python_udf_dependencies_archive(
            dependencies=["mehh"], target=target, format=format, index=dummy_pypi
        )
        assert actual == target
        assert target.exists()

        with tarfile.open(target, "r") as tf:
            assert "./mehh.py" in tf.getnames()

        assert re.search(r"Copying .*/archive\.[.a-z]+ \(\d+ bytes\) to .*/udf-deps\.tar", caplog.text)

    def test_python_udf_dependency_context_from_archive(self, tmp_path, dummy_pypi, caplog, unload_dummy_packages):
        archive_path = tmp_path / "udf-deps.zip"

        build_python_udf_dependencies_archive(dependencies=["mehh"], target=archive_path, index=dummy_pypi)

        with pytest.raises(ImportError, match="No module named 'mehh'"):
            import mehh

        with python_udf_dependency_context_from_archive(archive=archive_path):
            import mehh

            mehh_path = Path(mehh.__file__)
            assert mehh_path.exists()

        assert not mehh_path.exists()

    def test_run_udf_code_with_deps_from_archive(
        self, tmp_path, dummy_pypi, monkeypatch, caplog, unload_dummy_packages
    ):
        """Test automatic unpacking of UDF deps from archive, when using `run_udf_code`."""
        udf_code = textwrap.dedent(
            """
            # /// script
            # dependencies = [
            #     "mehh",
            # ]
            # ///

            from openeo.udf import UdfData, StructuredData
            import mehh

            def apply_udf_data(data: UdfData):
                xs = data.get_structured_data_list()[0].data
                data.set_structured_data_list([
                    StructuredData({
                        "x squared": [x*x for x in xs],
                        "mehh.__file__": mehh.__file__,
                    }),
                ])
            """
        )

        # Create dependency archive
        udf_archive = tmp_path / "udf-depz.zip"
        build_python_udf_dependencies_archive(dependencies=["mehh"], target=udf_archive, format="zip", index=dummy_pypi)

        # Note that we just test with `run_udf_code` in driver (with require_executor_context=False),
        # as monkeypatching of os.environ in the executors would be challenging,
        # and quite a bit of overkill for testing this feature.
        with gps_config_overrides(udf_dependencies_install_mode=UDF_DEPENDENCIES_INSTALL_MODE.ZIP):
            monkeypatch.setenv("UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH", str(udf_archive))
            data = UdfData(structured_data_list=[StructuredData([1, 2, 3, 4, 5])])
            result = run_udf_code(code=udf_code, data=data, require_executor_context=False)

        data = result.get_structured_data_list()[0].data
        assert data["x squared"] == [1, 4, 9, 16, 25]

        # Check that temp UDF dep folder is cleaned up now
        mehh_path = Path(data["mehh.__file__"])
        assert not mehh_path.exists()
        assert f"Cleaning up temporary UDF deps at {mehh_path.parent}" in caplog.text

    def test_run_udf_on_vector_data_cube_with_logging(self, tmp_path):
        custom_message = "custom_message" + str(datetime.now())
        udf = f"""
from openeo.udf import UdfData
import logging
def apply_udf_data(data: UdfData):
    logging.warn({custom_message!r})
    return data
"""

        run_job(
            {
                "process_graph": {
                    "runudf1": {
                        "process_id": "run_udf",
                        "arguments": {"data": [1, 2, 3], "runtime": "Python", "udf": udf},
                        "result": True,
                    }
                }
            },
            output_file=tmp_path / "out",
            metadata_file=tmp_path / JOB_METADATA_FILENAME,
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )
        log_file = get_logging_config()["handlers"][LOG_HANDLER_FILE_JSON]["filename"]
        assert custom_message in log_file.read_text()
