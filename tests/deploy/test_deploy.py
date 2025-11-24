import logging

import dirty_equals
import sys
import textwrap
from io import StringIO
from pathlib import Path
from unittest import mock

import pytest
from openeo_driver.testing import DictSubSet, RegexMatcher
from openeo_driver.utils import EvalEnv

from openeogeotrellis.deploy import (
    build_gps_backend_deploy_metadata,
    find_geotrellis_jars,
    get_jar_version_info,
    get_jar_versions,
    load_custom_processes,
)
from openeogeotrellis.testing import random_name


def _get_logger(level=logging.DEBUG):
    logger = logging.getLogger(__name__)
    stream = StringIO()
    logger.addHandler(logging.StreamHandler(stream))
    logger.setLevel(level=level)
    return logger, stream


def test_load_custom_processes_exec(tmp_path, monkeypatch, api_version, backend_implementation):
    path = tmp_path / "custom_processes.py"

    process_name = random_name(prefix="my_process")
    content = f"""
        from openeo_driver.ProcessGraphDeserializer import custom_process
        @custom_process
        def {process_name}(args, env):
            return 42
    """
    path.write_text(textwrap.dedent(content))

    monkeypatch.setenv("OPENEO_CUSTOM_PROCESSES", str(path))
    logger, stream = _get_logger(level=logging.DEBUG)
    load_custom_processes(logger=logger)

    logs = stream.getvalue()
    assert f"load_custom_processes: trying exec loading {str(path)!r}" in logs
    assert f"load_custom_processes: exec loaded {str(path)!r}" in logs

    process_registry = backend_implementation.processing.get_process_registry(api_version=api_version)
    f = process_registry.get_function(process_name)
    assert f({}, EvalEnv()) == 42


@pytest.mark.parametrize(["content"], [(None,), ("invalid c0ntent!!",)])
def test_load_custom_processes_exec_broken(tmp_path, monkeypatch, content):
    path = tmp_path / "broken.py"
    if content:
        path.write_text(content)

    monkeypatch.setenv("OPENEO_CUSTOM_PROCESSES", str(path))
    logger, stream = _get_logger(level=logging.ERROR)
    load_custom_processes(logger=logger)
    logs = stream.getvalue()
    assert f"load_custom_processes: failed to exec load {str(path)!r}" in logs


def test_load_custom_processes_import_default():
    logger, stream = _get_logger()
    load_custom_processes(logger=logger)
    logs = stream.getvalue()
    assert "Trying to load 'custom_processes'" in logs


def test_load_custom_processes_import_absent(tmp_path):
    logger, stream = _get_logger()
    sys_path = [str(tmp_path)]
    name = random_name(prefix="custom_processes")
    with mock.patch("sys.path", new=sys_path):
        load_custom_processes(logger=logger, _name=name)

    logs = stream.getvalue()
    assert "Trying to load {n!r} with PYTHONPATH {p}".format(n=name, p=sys_path) in logs
    assert '{n!r} not loaded: ModuleNotFoundError("No module named {n!r}"'.format(n=name) in logs


def test_load_custom_processes_import_present(tmp_path, api_version, backend_implementation):
    logger, stream = _get_logger()
    process_name = random_name(prefix="my_process")
    module_name = random_name(prefix="custom_processes")

    path = tmp_path / (module_name + '.py')
    content = f"""
        from openeo_driver.ProcessGraphDeserializer import custom_process
        @custom_process
        def {process_name}(args, env):
            return 42
    """
    path.write_text(textwrap.dedent(content))
    with mock.patch("sys.path", new=[str(tmp_path)] + sys.path):
        load_custom_processes(logger=logger, _name=module_name)

    logs = stream.getvalue()
    assert "Trying to load {n!r} with PYTHONPATH ['{p!s}".format(n=module_name, p=str(tmp_path)) in logs
    assert "Loaded {n!r}: {p!r}".format(n=module_name, p=str(path)) in logs

    process_registry = backend_implementation.processing.get_process_registry(api_version=api_version)
    f = process_registry.get_function(process_name)
    assert f({}, EvalEnv()) == 42


JAR_DIR = Path(__file__).parent.parent.parent / "jars"


@pytest.mark.parametrize(
    ["glob", "expected"],
    [
        ("geotrellis-extensions-*.jar", dirty_equals.IsStr(regex=r"(\d+\.\d+.\d+_\d+\.\d+|PR-\d+).*")),
    ],
)
def test_get_jar_version_info(glob, expected):
    # TODO: run these tests against small dedicated test files instead of the ones downloaded in pre_test.sh? #336
    jar_paths = list(JAR_DIR.glob(glob))
    assert jar_paths
    for path in jar_paths:
        version = get_jar_version_info(path)
        assert version == expected


def test_get_jar_versions():
    paths = JAR_DIR.glob("geotrellis-*.jar")
    versions = get_jar_versions(paths)
    assert versions == DictSubSet(
        {
            "geotrellis-extensions": dirty_equals.IsStr(regex=r"(\d+\.\d+.\d+_\d+\.\d+|PR-\d+).*"),
        }
    )


@pytest.mark.parametrize(
    ["jar_path", "expected"],
    [
        ("jarz/geotrellis-extensions-2.5.1_2.13-SNAPSHOT.jar", "geotrellis-extensions"),
        ("jarz/geotrellis-extensions-PR-123.jar", "geotrellis-extensions"),
    ],
)
def test_get_jar_versions_name_detection(jar_path, expected):
    """Ignore "PR" prefix (`-PR-123`) in jar name-version detection"""
    assert get_jar_versions([Path(jar_path)]) == {expected: "n/a"}


def test_build_gps_backend_deploy_metadata():
    metadata = build_gps_backend_deploy_metadata(
        packages=["openeo", "openeo_driver", "openeo-geopyspark", "geopyspark"],
        jar_paths=JAR_DIR.glob("geotrellis-*.jar")
    )
    assert metadata == DictSubSet(
        {
            "versions": DictSubSet(
                {
                    "openeo": RegexMatcher(r"\d+.\d+.\d+"),
                    "geotrellis-extensions": dirty_equals.IsStr(regex=r"(\d+\.\d+.\d+_\d+\.\d+|PR-\d+).*"),
                }
            )
        }
    )


def test_find_geotrellis_jars_cwd(tmp_path, monkeypatch):
    (tmp_path / "jars").mkdir(parents=True)
    (tmp_path / "jars" / "geotrellis-backend-assembly-1.2.3.jar").touch()
    (tmp_path / "jars" / "geotrellis-extensions-4.5.6.jar").touch()
    monkeypatch.chdir(tmp_path)
    assert find_geotrellis_jars() == [
        Path("jars/geotrellis-extensions-4.5.6.jar"),
    ]


def test_find_geotrellis_jars_extra(tmp_path):
    (tmp_path / "geotrellis-backend-assembly-1.2.3.jar").touch()
    (tmp_path / "geotrellis-extensions-4.5.6.jar").touch()
    assert find_geotrellis_jars(extra_search_locations=[tmp_path]) == [
        tmp_path / "geotrellis-extensions-4.5.6.jar",
    ]
