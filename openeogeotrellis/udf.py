import collections
import logging
import pyspark
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Union, Iterator, Tuple, Dict, Iterable, Optional

import openeo.udf
from openeo.udf.run_code import extract_udf_dependencies
from openeo.util import TimingLogger

_log = logging.getLogger(__name__)


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
                _log.info(f"pip install output: {line.rstrip()}")
        exit_code = process.wait()

    _log.info(f"pip install finished with exit code {exit_code}")
    if exit_code != 0:
        raise RuntimeError(f"pip install failed with {exit_code=}")
