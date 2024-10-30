import collections
import contextlib
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple, Union

import openeo.udf
import pyspark
from openeo.udf.run_code import extract_udf_dependencies
from openeo.util import TimingLogger

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.constants import UDF_DEPENDENCIES_INSTALL_MODE

_log = logging.getLogger(__name__)

# Reusable constant to streamline discoverability and grep-ability of this folder name.
UDF_PYTHON_DEPENDENCIES_FOLDER_NAME = "udf-py-deps.d"
UDF_PYTHON_DEPENDENCIES_ARCHIVE_NAME = "udf-py-deps.zip"


def run_udf_code(code: str, data: openeo.udf.UdfData, require_executor_context: bool = True) -> openeo.udf.UdfData:
    """
    Wrapper around `openeo.udf.run_udf_code` for some additional guardrails and checks
    """
    if require_executor_context:
        code += "\n\n" + textwrap.dedent(
            f"""
            # UDF code tail added by {run_udf_code.__module__}.{run_udf_code.__name__}
            import {assert_running_in_executor.__module__}
            {assert_running_in_executor.__module__}.assert_running_in_executor()
            """
        )

    context = contextlib.nullcontext()

    udf_python_dependencies_archive_path = os.environ.get("UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH")
    install_mode = get_backend_config().udf_dependencies_install_mode
    _log.info(f"run_udf with {install_mode=} {udf_python_dependencies_archive_path=}")
    if install_mode == UDF_DEPENDENCIES_INSTALL_MODE.ZIP:
        if udf_python_dependencies_archive_path and Path(udf_python_dependencies_archive_path).exists():
            context = python_udf_dependency_context_from_archive(archive=udf_python_dependencies_archive_path)
        else:
            # TODO: make this an exception instead of warning?
            _log.warning(
                f"Empty/non-existent UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH {udf_python_dependencies_archive_path}"
            )

    with context:
        return openeo.udf.run_udf_code(code=code, data=data)


def assert_running_in_executor():
    """
    Check that we are running in an executor process, not a driver
    based on `pyspark.SparkContext._assert_on_driver`
    """
    task_context = pyspark.TaskContext.get()
    if task_context is None:
        raise RuntimeError("Not running in PySpark executor context.")


def collect_udfs(process_graph: dict) -> Iterator[Tuple[str, str, Union[str, None]]]:
    """
    Recursively traverse a process graph in flat graph representation and collect UDFs.

    :return: Iterator of (udf, runtime, version) tuples
    """
    for node_id, node in process_graph.items():
        if node["process_id"] == "run_udf":
            yield tuple(node["arguments"].get(k) for k in ["udf", "runtime", "version"])
        for argument_id, argument in node.get("arguments", {}).items():
            if isinstance(argument, dict) and "process_graph" in argument:
                yield from collect_udfs(argument["process_graph"])


def collect_python_udf_dependencies(process_graph: dict) -> Dict[Tuple[str, str], set]:
    """
    Collect dependencies (imports) from Python UDFs in a given process graph,

    :return: Dictionary of dependencies (set) per (runtime, version) tuple
    """
    dependencies = collections.defaultdict(set)
    for udf, runtime, version in collect_udfs(process_graph):
        if runtime.lower().startswith("python"):
            dependencies[(runtime, version)].update(extract_udf_dependencies(udf) or [])

    return dict(dependencies)


def install_python_udf_dependencies(
    dependencies: Iterable[str],
    target: Union[str, Path],
    *,
    retries: int = 2,
    timeout: float = 5,
    index: Optional[str] = None,
):
    """
    Install Python UDF dependencies in a target directory

    :param dependencies: Iterable of dependency package names
    :param target: Directory where to install dependencies
    """
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--target",
        str(target),
        "--progress-bar",
        "off",
        "--disable-pip-version-check",
        "--no-input",
        "--retries",
        str(retries),
        "--timeout",
        str(timeout),
    ]

    index = index or get_backend_config().udf_dependencies_pypi_index
    if index:
        command.extend(["--index", index])

    # TODO: --cache-dir
    command.extend(sorted(set(dependencies)))

    with TimingLogger(title=f"Installing Python UDF dependencies with {command}", logger=_log.info):
        # TODO: by piping stdout and stderr to same pipe, we simplify the work necessary to avoid deadlocks.
        #       However, this means that we lose the ability to distinguish between stdout and stderr.
        process = subprocess.Popen(
            args=command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )
        process.stdin.close()
        with process.stdout:
            for line in iter(process.stdout.readline, ""):
                _log.debug(f"pip install output: {line.rstrip()}")
        exit_code = process.wait()
        _log.info(f"pip install exited with exit code {exit_code}")
        if exit_code != 0:
            raise RuntimeError(f"pip install of UDF dependencies failed with {exit_code=}")



def build_python_udf_dependencies_archive(
    dependencies: Iterable[str],
    *,
    target: Union[str, Path],
    format: str = "zip",
    retries: int = 2,
    timeout: float = 5,
    index: Optional[str] = None,
) -> Path:
    """
    Install Python UDF dependencies in a temp directory
    and package them into an archive file (e.g. a zip or tar file).

    :param dependencies: Iterable of dependency package names
    :param target: path for the target archive file
    :param format: Archive format (e.g. "zip", "tar", "gztar", ... see `shutil.make_archive`)
    """

    with tempfile.TemporaryDirectory(prefix="udfpydeps-pack-") as temp_root:
        temp_root = Path(temp_root)

        # Start with installing the dependencies in a temp directory
        temp_install = temp_root / "packages"
        install_python_udf_dependencies(
            dependencies=dependencies,
            target=temp_install,
            retries=retries,
            timeout=timeout,
            index=index,
        )

        # Put installed packages in a temp archive
        temp_archive_base_name = temp_root / "archive"
        # Archive everything in a ZIP file
        with TimingLogger(
            title=f"Archiving Python UDF dependencies from {temp_install} to {format} archive {temp_archive_base_name}",
            logger=_log.info,
        ):
            temp_archive = shutil.make_archive(base_name=temp_archive_base_name, format=format, root_dir=temp_install)
            temp_archive = Path(temp_archive)

        # Copy the archive to the target location
        target = Path(target)
        _log.info(f"Copying {temp_archive} ({temp_archive.stat().st_size} bytes) to {target}")
        shutil.copy(src=temp_archive, dst=target)

        return target


@contextlib.contextmanager
def python_udf_dependency_context_from_archive(archive: Union[str, Path]):
    """
    Context manager that extracts UDF dependencies from an archive file and adds them to the Python path.
    """
    # TODO: make sure archive does not escape its intended directory (e.g. only support ZIP for now?)
    # TODO: mode to not clean up unpacked archive (for reuse in subsequent calls)?
    #       But how to establish identity then? By hash of archive file?
    archive = Path(archive)
    with tempfile.TemporaryDirectory(prefix="udfpypeps-unpack-") as extra_deps:
        with TimingLogger(title=f"Extracting Python UDF dependencies from {archive} to {extra_deps}", logger=_log.info):
            shutil.unpack_archive(filename=archive, extract_dir=extra_deps)

        extra_deps = str(extra_deps)
        sys.path.append(extra_deps)
        try:
            yield Path(extra_deps)
        finally:
            if extra_deps in sys.path:
                sys.path.remove(extra_deps)
            _log.info(f"Cleaning up temporary UDF deps at {extra_deps}")
