import textwrap

import openeo.udf
import pyspark


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
