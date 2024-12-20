from unittest import mock

import dirty_equals
import kubernetes.client
import pytest
import moto
import boto3

from openeogeotrellis.integrations.calrissian import (
    CalrissianJobLauncher,
    CalrissianS3Result,
)


@pytest.fixture
def generate_unique_id_mock() -> str:
    # TODO: move this mock fixture to a more generic place
    with mock.patch("openeo_driver.utils.uuid") as uuid:
        fake_uuid = "0123456789abcdef0123456789abcdef"
        uuid.uuid4.return_value.hex = fake_uuid
        yield fake_uuid


class TestCalrissianJobLauncher:
    def test_create_input_staging_job_manifest(self, generate_unique_id_mock):
        launcher = CalrissianJobLauncher(namespace="calrissian-test", name_base="r-1234")

        manifest, cwl_path = launcher.create_input_staging_job_manifest(cwl_content="class: Dummy")

        assert cwl_path == "/calrissian/input-data/r-1234-cal-inp-01234567.cwl"

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        assert manifest_dict["metadata"] == dirty_equals.IsPartialDict(
            {
                "name": "r-1234-cal-inp-01234567",
                "namespace": "calrissian-test",
            }
        )
        assert manifest_dict["spec"] == dirty_equals.IsPartialDict(
            {
                "backoff_limit": 1,
            }
        )
        assert manifest_dict["spec"]["template"]["spec"] == dirty_equals.IsPartialDict(
            {
                "containers": [
                    dirty_equals.IsPartialDict(
                        {
                            "name": "calrissian-input-staging",
                            "image": "alpine:3",
                            "command": ["/bin/sh"],
                            "args": [
                                "-c",
                                "set -euxo pipefail; echo 'Y2xhc3M6IER1bW15' | base64 -d > /calrissian/input-data/r-1234-cal-inp-01234567.cwl",
                            ],
                            "volume_mounts": [
                                dirty_equals.IsPartialDict(
                                    {
                                        "mount_path": "/calrissian/input-data",
                                        "name": "calrissian-input-data",
                                        "read_only": False,
                                    }
                                ),
                            ],
                        }
                    )
                ],
                "volumes": [
                    dirty_equals.IsPartialDict(
                        {
                            "name": "calrissian-input-data",
                            "persistent_volume_claim": {"claim_name": "calrissian-input-data", "read_only": False},
                        }
                    ),
                ],
            }
        )

    def test_create_cwl_job_manifest(self, generate_unique_id_mock):
        launcher = CalrissianJobLauncher(namespace="calrissian-test", name_base="r-123")

        manifest, output_dir = launcher.create_cwl_job_manifest(
            cwl_path="/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
            cwl_arguments=["--message", "Howdy Earth!"],
        )

        assert output_dir == "r-123-cal-cwl-01234567"

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        assert manifest_dict["metadata"] == dirty_equals.IsPartialDict(
            {
                "name": "r-123-cal-cwl-01234567",
                "namespace": "calrissian-test",
            }
        )
        assert manifest_dict["spec"] == dirty_equals.IsPartialDict(
            {
                "backoff_limit": 1,
            }
        )
        assert manifest_dict["spec"]["template"]["spec"] == dirty_equals.IsPartialDict(
            {
                "containers": [
                    dirty_equals.IsPartialDict(
                        {
                            "name": "r-123-cal-cwl-01234567",
                            "command": ["calrissian"],
                            "args": dirty_equals.Contains(
                                "--tmp-outdir-prefix",
                                "/calrissian/tmpout/",
                                "--outdir",
                                "/calrissian/output-data/r-123-cal-cwl-01234567",
                                "/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
                                "--message",
                                "Howdy Earth!",
                            ),
                            "volume_mounts": [
                                dirty_equals.IsPartialDict(
                                    {
                                        "mount_path": "/calrissian/input-data",
                                        "name": "calrissian-input-data",
                                        "read_only": True,
                                    }
                                ),
                                dirty_equals.IsPartialDict(
                                    {
                                        "mount_path": "/calrissian/tmpout",
                                        "name": "calrissian-tmpout",
                                        "read_only": False,
                                    }
                                ),
                                dirty_equals.IsPartialDict(
                                    {
                                        "mount_path": "/calrissian/output-data",
                                        "name": "calrissian-output-data",
                                        "read_only": False,
                                    }
                                ),
                            ],
                        }
                    )
                ],
                "volumes": [
                    dirty_equals.IsPartialDict(
                        {
                            "name": "calrissian-input-data",
                            "persistent_volume_claim": {"claim_name": "calrissian-input-data", "read_only": True},
                        }
                    ),
                    dirty_equals.IsPartialDict(
                        {
                            "name": "calrissian-tmpout",
                            "persistent_volume_claim": {"claim_name": "calrissian-tmpout", "read_only": False},
                        }
                    ),
                    dirty_equals.IsPartialDict(
                        {
                            "name": "calrissian-output-data",
                            "persistent_volume_claim": {"claim_name": "calrissian-output-data", "read_only": False},
                        }
                    ),
                ],
            }
        )


class TestCalrissianS3Result:
    @pytest.fixture
    def s3_output(self):
        with moto.mock_aws():
            s3 = boto3.client("s3")
            bucket = "the-bucket"
            s3.create_bucket(Bucket=bucket)
            key = "path/to/output.txt"
            s3.put_object(Bucket=bucket, Key=key, Body="Howdy, Earth!")
            yield bucket, key

    def test_read(self, s3_output):
        bucket, key = s3_output
        result = CalrissianS3Result(s3_bucket=bucket, s3_key=key)
        assert result.read() == b"Howdy, Earth!"

    def test_read_encoding(self, s3_output):
        bucket, key = s3_output
        result = CalrissianS3Result(s3_bucket=bucket, s3_key=key)
        assert result.read(encoding="utf-8") == "Howdy, Earth!"
