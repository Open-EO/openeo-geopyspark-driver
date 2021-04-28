import logging
import sys
import textwrap
from io import StringIO
from unittest import mock

from openeo.capabilities import ComparableVersion
from openeo_driver.ProcessGraphDeserializer import get_process_registry
from openeo_driver.utils import EvalEnv
from openeogeotrellis.deploy import load_custom_processes
from openeogeotrellis.testing import random_name


def _get_logger():
    logger = logging.getLogger(__name__)
    stream = StringIO()
    logger.addHandler(logging.StreamHandler(stream))
    logger.setLevel(logging.DEBUG)
    return logger, stream


def test_load_custom_processes_default():
    logger, stream = _get_logger()
    load_custom_processes(logger)
    logs = stream.getvalue()
    assert "Trying to load 'custom_processes'" in logs


def test_load_custom_processes_absent(tmp_path):
    logger, stream = _get_logger()
    sys_path = [str(tmp_path)]
    name = random_name(prefix="custom_processes")
    with mock.patch("sys.path", new=sys_path):
        load_custom_processes(logger, _name=name)

    logs = stream.getvalue()
    assert "Trying to load {n!r} with PYTHONPATH {p}".format(n=name, p=sys_path) in logs
    assert '{n!r} not loaded: ModuleNotFoundError("No module named {n!r}"'.format(n=name) in logs


def test_load_custom_processes_present(tmp_path, api_version):
    logger, stream = _get_logger()
    process_name = random_name(prefix="my_process")
    module_name = random_name(prefix="custom_processes")

    path = tmp_path / (module_name + '.py')
    with path.open("w") as f:
        f.write(textwrap.dedent("""
            from openeo_driver.ProcessGraphDeserializer import custom_process
            @custom_process
            def {p}(args, env):
                return 42
        """.format(p=process_name)))
    with mock.patch("sys.path", new=[str(tmp_path)] + sys.path):
        load_custom_processes(logger, _name=module_name)

    logs = stream.getvalue()
    assert "Trying to load {n!r} with PYTHONPATH ['{p!s}".format(n=module_name, p=str(tmp_path)) in logs
    assert "Loaded {n!r}: {p!r}".format(n=module_name, p=str(path)) in logs

    process_registry = get_process_registry(ComparableVersion(api_version))
    f = process_registry.get_function(process_name)
    assert f({}, EvalEnv()) == 42
