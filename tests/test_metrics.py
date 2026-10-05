import dirty_equals
import pytest
from openeo.util import ensure_dir
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import read_json
from pyspark import SparkContext

from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.deploy.batch_job import run_job
from openeogeotrellis.utils import get_jvm


def _reset_execution_metrics():
    execution_metrics = getattr(get_jvm().org.openeo.geotrelliscommon, "ExecutionMetrics$")
    getattr(execution_metrics, "MODULE$").store(get_jvm().org.openeo.geotrelliscommon.ExecutionMetrics(0, 0, 0.0, 0))


@pytest.fixture
def clean_execution_metrics():
    # ExecutionMetrics is JVM-global state: avoid leaking into (or from) other tests
    _reset_execution_metrics()
    yield
    _reset_execution_metrics()


def test_execution_metrics(tmp_path, clean_execution_metrics):
    spark_context = SparkContext.getOrCreate()
    spark_listener = get_jvm().org.openeo.sparklisteners.BatchJobProgressListener()
    spark_context._jsc.sc().addSparkListener(spark_listener)

    job_spec = {
        "title": "my job",
        "process_graph": {
            "lc": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-05", "2021-01-06"],
                    "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                    "bands": ["Longitude", "Latitude"],
                },
            },
            "save": {
                "process_id": "save_result",
                "arguments": {"data": {"from_node": "lc"}, "format": "GTiff"},
                "result": True,
            },
        },
    }
    job_dir = ensure_dir(tmp_path / "job_dir")
    metadata_file = job_dir / JOB_METADATA_FILENAME
    try:
        run_job(
            job_spec,
            output_file=job_dir / "out.tif",
            metadata_file=metadata_file,
            api_version="1.0.0",
            job_dir=job_dir,
            dependencies=[],
            user_id="jenkins",
        )
    finally:
        spark_context._jsc.sc().removeSparkListener(spark_listener)

    metadata = read_json(metadata_file)
    assert metadata["start_datetime"] == "2021-01-05T00:00:00Z"
    assert len(metadata["assets"]) == 1
    assert metadata["usage"] == DictSubSet(
        {
            "total_stage_runtime": dirty_equals.IsPositiveInt,
            "total_executor_allocation_time": dirty_equals.IsInt(ge=0),
            "cpu_utilization_ratio": dirty_equals.IsFloat(ge=0),
            "total_stage_failures": 0,
        }
    )