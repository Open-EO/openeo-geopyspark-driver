import textwrap

import pyspark
import pyspark.pandas
import pytest
from openeo.udf import UdfData, StructuredData
from openeogeotrellis.udf import assert_running_in_executor, run_udf_code, extract_udf_functions, UDFFunctionDeclaration


def test_assert_running_in_executor_in_driver():
    """In driver: should raise exception"""
    with pytest.raises(RuntimeError, match="Not running in PySpark executor context."):
        assert_running_in_executor()


@pytest.fixture(scope="module")
def spark_context() -> pyspark.SparkContext:
    return pyspark.SparkContext.getOrCreate()


def test_assert_running_in_executor_parallelize(spark_context):
    """In executor: no exception (just return None)"""
    data = spark_context.parallelize([1, 2, 3])
    result = data.map(lambda x: [assert_running_in_executor()] * x).collect()
    assert result == [[None], [None, None], [None, None, None]]


def test_assert_running_in_executor_pandas_apply(spark_context, tmp_path):
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("foo,bar\n1,2\n3,4\n")
    df = pyspark.pandas.read_csv([f"file://{csv_path.absolute()}"])
    result = df.groupby("foo").apply(lambda df: df.assign("nope", assert_running_in_executor()))
    out_dir = tmp_path / "output"
    result.to_csv(f"file://{out_dir}")
    assert list(out_dir.glob("*.csv")) == ["TODO"]


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


def test_extract_functions_simple():
    udf = textwrap.dedent(
        """
        def foo(x, y):
            return x + y
    """
    )
    assert extract_udf_functions(udf) == [UDFFunctionDeclaration(name="foo", arg_names=["x", "y"])]


@pytest.mark.parametrize(
    ["udf", "expected"],
    [
        (
            "def foo(x, y): return x+y",
            [UDFFunctionDeclaration(name="foo", arg_names=["x", "y"])],
        ),
        (
            """
            from math import cos

            CONST = 3.14

            def foo(x):
                return x + 2

            def bar(y: int) -> int:
                return y * CONST

            def incr(value: float, *, to_add: float = 1.1) -> float:
                return value + to_add
            """,
            [
                UDFFunctionDeclaration(name="foo", arg_names=["x"]),
                UDFFunctionDeclaration(name="bar", arg_names=["y"]),
                UDFFunctionDeclaration(name="incr", arg_names=["value", "to_add"]),
            ],
        ),
        (
            """
            class Thing:
                def __init__(self, name):
                    self.name = name
                def hello(self, x):
                    print("Hello", self.name, x)

            def foo(x: Thing) -> Thing:
                def bar(y):
                    x.hello(x=y)
                    return y
                return bar(3)
            """,
            [UDFFunctionDeclaration(name="foo", arg_names=["x"])],
        ),
    ],
)
def test_extract_functions(udf, expected):
    udf = textwrap.dedent(udf)
    assert extract_udf_functions(udf) == expected
