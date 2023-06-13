import ast
import dataclasses
import textwrap
from typing import List, Tuple

import openeo.udf
import pyspark


def run_udf_code(code: str, data: openeo.udf.UdfData, require_executor_context: bool = True) -> openeo.udf.UdfData:
    """
    Wrapper around `openeo.udf.run_udf_code` for some additional guardrails and checks
    """
    # TODO: remove this temporary require_executor_context feature flag https://github.com/Open-EO/openeo-geopyspark-driver/issues/404
    if require_executor_context:
        code = wrap_udf_code(code)
    return openeo.udf.run_udf_code(code=code, data=data)


def wrap_udf_code(code: str) -> str:
    """Wrap UDF code with additional guardrails and checks"""
    # TODO: Instead of adding tail, make sure this runs before any user provided code (but that might mess with line numbers in stack traces)
    #       Instead of wrapping the source code: just run `assert_running_in_executor` directly before calling `load_module_from_string`
    tail = textwrap.dedent(
        f"""
        # UDF code tail added by {wrap_udf_code.__module__}.{wrap_udf_code.__name__}
        import {assert_running_in_executor.__module__}
        {assert_running_in_executor.__module__}.assert_running_in_executor()
        """
    )
    # TODO: only add once?
    code += "\n\n" + tail
    return code


def assert_running_in_executor():
    """
    Check that we are running in an executor process, not a driver
    based on `pyspark.SparkContext._assert_on_driver`
    """
    task_context = pyspark.TaskContext.get()
    if task_context is None:
        raise RuntimeError("Not running in PySpark executor context.")


@dataclasses.dataclass(frozen=True)
class UDFFunctionDeclaration:
    """Declaration info of a UDF function: name, argument names, ..."""

    name: str
    arg_names: List[str]


def extract_udf_functions(udf: str) -> List[UDFFunctionDeclaration]:
    """
    Extract defined UDF functions (without executing the code) in given UDF code

    :return: list of tuples (function_name, list of argument names)

    """
    # TODO: include type annotations?
    # TODO: move this to openeo.udf when API has settled.
    functions = []
    module = ast.parse(udf)
    for x in module.body:
        if isinstance(x, ast.FunctionDef):
            args = [a.arg for a in x.args.posonlyargs]
            args += [a.arg for a in x.args.args]
            args += [a.arg for a in x.args.kwonlyargs]
            functions.append(UDFFunctionDeclaration(name=x.name, arg_names=args))
    return functions
