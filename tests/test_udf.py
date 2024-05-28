import textwrap

import pyspark
import pytest
from openeo.udf import UdfData, StructuredData
from openeogeotrellis.udf import assert_running_in_executor, run_udf_code, collect_python_udf_dependencies


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
        assert collect_python_udf_dependencies(pg) == {("Python", None): set()}

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
                {("Python", None): {"numpy", "pandas"}},
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
                {("Python3", "3.1415"): {"numpy", "pandas>=1.2.3"}},
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
        assert collect_python_udf_dependencies(pg) == {("Python", None): {"numpy", "pandas", "scipy"}}
