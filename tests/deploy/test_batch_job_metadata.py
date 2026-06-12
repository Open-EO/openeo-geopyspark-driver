import pytest
from openeogeotrellis.deploy.batch_job_metadata import convert_job_local_hrefs
from openeogeotrellis.testing import gps_config_overrides


@pytest.mark.parametrize(
    ["job_local_href_format", "expected"],
    [
        (None, "file:///tmp/aux.txt"),
        ("s3", "s3://openeo-batch-job-data/tmp/aux.txt"),
    ],
)
def test_convert_job_local_hrefs_basic(job_local_href_format, expected):
    metadata = {
        "rel": "aux",
        "href": "openeo-job-local:///tmp/aux.txt",
        "title": "auxiliary",
    }

    with gps_config_overrides(
        job_local_href_format=job_local_href_format,
        s3_bucket_name="openeo-batch-job-data",
    ):
        converted = convert_job_local_hrefs(metadata)

    assert converted == {
        "rel": "aux",
        "href": expected,
        "title": "auxiliary",
    }
    # Original metadata should not be mutated
    assert metadata == {
        "rel": "aux",
        "href": "openeo-job-local:///tmp/aux.txt",
        "title": "auxiliary",
    }


def test_convert_job_local_hrefs_deep():
    metadata = {
        "id": "thing123",
        "items": [
            {
                "id": "item1",
                "assets": {
                    "foo": {
                        "id": "asset1",
                        "links": [
                            {"rel": "root", "href": "http://a.test/"},
                            {"rel": "tmp", "href": "openeo-job-local:///tmp/foo.txt"},
                        ],
                    }
                },
            },
        ],
        "links": [
            {"rel": "self", "href": "http://a.test/thing123"},
            {"rel": "aux", "href": "openeo-job-local:///tmp/aux.txt"},
        ],
    }
    converted = convert_job_local_hrefs(metadata)
    assert converted == {
        "id": "thing123",
        "items": [
            {
                "id": "item1",
                "assets": {
                    "foo": {
                        "id": "asset1",
                        "links": [
                            {"rel": "root", "href": "http://a.test/"},
                            {"rel": "tmp", "href": "file:///tmp/foo.txt"},
                        ],
                    }
                },
            }
        ],
        "links": [
            {"rel": "self", "href": "http://a.test/thing123"},
            {"rel": "aux", "href": "file:///tmp/aux.txt"},
        ],
    }
