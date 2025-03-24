from typing import Dict
from unittest import mock

import boto3
import dirty_equals
import kubernetes.client
import moto
import pytest

from openeogeotrellis.integrations.calrissian import (
    CalrissianJobLauncher,
    CalrissianS3Result,
    CwLSource,
)


@pytest.fixture
def generate_unique_id_mock() -> str:
    """Fixture to fix the UUID used in `generate_unique_id`"""
    # TODO: move this mock fixture to a more generic place
    with mock.patch("openeo_driver.utils.uuid") as uuid:
        fake_uuid = "0123456789abcdef0123456789abcdef"
        uuid.uuid4.return_value.hex = fake_uuid
        yield fake_uuid


class TestCalrissianJobLauncher:
    NAMESPACE = "test-calrissian"

    def test_create_input_staging_job_manifest(self, generate_unique_id_mock):
        launcher = CalrissianJobLauncher(namespace=self.NAMESPACE, name_base="r-1234")

        manifest, cwl_path = launcher.create_input_staging_job_manifest(
            cwl_source=CwLSource.from_string("class: Dummy")
        )

        assert cwl_path == "/calrissian/input-data/r-1234-cal-inp-01234567.cwl"

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        assert manifest_dict["metadata"] == dirty_equals.IsPartialDict(
            {
                "name": "r-1234-cal-inp-01234567",
                "namespace": self.NAMESPACE,
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
        launcher = CalrissianJobLauncher(namespace=self.NAMESPACE, name_base="r-123")

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
                "namespace": self.NAMESPACE,
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

    @pytest.fixture()
    def k8_pvc_api(self):
        """Mock for PVC API in kubernetes.client.CoreV1Api"""
        pvc_to_volume_name = {
            "calrissian-output-data": "1234-abcd-5678-efgh",
        }

        def read_namespaced_persistent_volume_claim(name: str, namespace: str):
            assert namespace == self.NAMESPACE
            return kubernetes.client.V1PersistentVolumeClaim(
                spec=kubernetes.client.V1PersistentVolumeClaimSpec(volume_name=pvc_to_volume_name[name])
            )

        with mock.patch("kubernetes.client.CoreV1Api") as CoreV1Api:
            CoreV1Api.return_value.read_namespaced_persistent_volume_claim = read_namespaced_persistent_volume_claim
            yield

    def test_get_output_volume_name(self, k8_pvc_api):
        launcher = CalrissianJobLauncher(namespace=self.NAMESPACE, name_base="r-123")
        assert launcher.get_output_volume_name() == "1234-abcd-5678-efgh"

    @pytest.fixture()
    def k8s_batch_api(self):
        """mock for kubernetes.client.BatchV1Api"""

        class BatchV1Api:
            def __init__(self):
                self.jobs: Dict[str, kubernetes.client.V1Job] = {}

            def create_namespaced_job(self, namespace: str, body: kubernetes.client.V1Job):
                assert body.metadata.namespace == namespace
                job = kubernetes.client.V1Job(metadata=body.metadata)
                self.jobs[job.metadata.name] = job
                return job

            def read_namespaced_job(self, name: str, namespace: str):
                assert name in self.jobs
                assert self.jobs[name].metadata.namespace == namespace
                return kubernetes.client.V1Job(
                    metadata=self.jobs[name].metadata,
                    status=kubernetes.client.V1JobStatus(
                        # TODO: way to specify timeline of job conditions?
                        conditions=[kubernetes.client.V1JobCondition(type="Complete", status="True")]
                    ),
                )

        with mock.patch("kubernetes.client.BatchV1Api", new=BatchV1Api):
            yield

    def test_launch_job_and_wait_basic(self, k8s_batch_api, caplog):
        launcher = CalrissianJobLauncher(namespace=self.NAMESPACE, name_base="r-456")
        job_manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(name="cal-123", namespace=self.NAMESPACE)
        )
        result = launcher.launch_job_and_wait(manifest=job_manifest)
        assert isinstance(result, kubernetes.client.V1Job)

        assert caplog.messages[-1] == dirty_equals.IsStr(regex=".*job_name='cal-123'.*final_status='complete'.*")

    def test_run_cwl_workflow_basic(self, k8_pvc_api, k8s_batch_api, generate_unique_id_mock, caplog):
        launcher = CalrissianJobLauncher(namespace=self.NAMESPACE, name_base="r-456", s3_bucket="test-bucket")
        res = launcher.run_cwl_workflow(
            cwl_source=CwLSource.from_string("class: Dummy"),
            cwl_arguments=["--message", "Howdy Earth!"],
            output_paths=["output.txt"],
        )
        assert res == {
            "output.txt": CalrissianS3Result(
                s3_bucket="test-bucket",
                s3_key="1234-abcd-5678-efgh/r-456-cal-cwl-01234567/output.txt",
            ),
        }


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

    def test_generate_presigned_url(self, s3_output, monkeypatch):
        monkeypatch.setenv("SWIFT_URL", "https://s3.example.com")
        bucket, key = s3_output
        result = CalrissianS3Result(s3_bucket=bucket, s3_key=key)
        assert result.generate_presigned_url() == dirty_equals.IsStr(
            regex=r"https://s3.example.com/the-bucket/path/to/output.txt\?AWSAccessKeyId=.*"
        )


class TestCwlSource:
    def test_from_string(self):
        content = "cwlVersion: v1.0\nclass: CommandLineTool\n"
        cwl = CwLSource.from_string(content=content)
        assert cwl.get_content() == "cwlVersion: v1.0\nclass: CommandLineTool\n"

    def test_from_string_auto_dedent(self):
        content = """
            cwlVersion: v1.0
            class: CommandLineTool
            inputs:
                message:
                    type: string
        """
        cwl = CwLSource.from_string(content=content)
        expected = "\ncwlVersion: v1.0\nclass: CommandLineTool\ninputs:\n    message:\n        type: string\n"
        assert cwl.get_content() == expected

    def test_from_path(self, tmp_path):
        path = tmp_path / "dummy.cwl"
        path.write_text("cwlVersion: v1.0\nclass: CommandLineTool\n")
        cwl = CwLSource.from_path(path=path)
        assert cwl.get_content() == "cwlVersion: v1.0\nclass: CommandLineTool\n"

    def test_from_url(self, requests_mock):
        url = "https://example.com/dummy.cwl"
        requests_mock.get(url, text="cwlVersion: v1.0\nclass: CommandLineTool\n")
        cwl = CwLSource.from_url(url=url)
        assert cwl.get_content() == "cwlVersion: v1.0\nclass: CommandLineTool\n"

    def test_from_resource(self):
        cwl = CwLSource.from_resource(anchor="openeogeotrellis.integrations", path="cwl/hello.cwl")
        assert "Hello World" in cwl.get_content()
