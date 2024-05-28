import collections

import textwrap
from typing import Union, Iterator, Tuple, Dict

import openeo.udf
import pyspark

from openeo.udf.run_code import extract_udf_dependencies


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
