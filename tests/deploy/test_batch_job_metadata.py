import pytest
from openeogeotrellis.deploy.batch_job_metadata import href_from_job_local_path
from openeogeotrellis.testing import gps_config_overrides


@pytest.mark.parametrize(
    ["job_local_href_format", "expected"],
    [
        (None, "file:///tmp/aux.txt"),
        ("s3", "s3://openeo-batch-job-data/tmp/aux.txt"),
    ],
)
def test_href_from_job_local_path(job_local_href_format, expected):

    with gps_config_overrides(
        job_local_href_format=job_local_href_format,
        s3_bucket_name="openeo-batch-job-data",
    ):
        assert href_from_job_local_path("/tmp/aux.txt") == expected
