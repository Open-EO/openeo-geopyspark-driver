import datetime
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional
from unittest import mock

import boto3
import dirty_equals
import kubernetes.client
import moto
import pytest
import yaml

from openeo_driver.utils import EvalEnv
from openeogeotrellis.config.integrations.calrissian_config import (
    DEFAULT_CALRISSIAN_IMAGE,
    DEFAULT_CALRISSIAN_RUNNER_RESOURCE_REQUIREMENTS,
    CalrissianConfig,
)
from openeogeotrellis.integrations.calrissian import (
    CalrissianJobLauncher,
    CalrissianLaunchConfigBuilder,
    CalrissianS3Result,
    CwLSource,
    parse_cwl_outputs_listing,
    find_stac_root,
    cwl_to_stac,
)
from openeogeotrellis.integrations.s3proxy.sts import STSCredentials
from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.util.runtime import ENV_VAR_OPENEO_BATCH_JOB_ID
from openeogeotrellis.utils import s3_client
from tests.data import get_test_data_file


@pytest.fixture
def generate_unique_id_mock() -> Iterator[str]:
    """Fixture to fix the UUID used in `generate_unique_id`"""
    # TODO: move this mock fixture to a more generic place
    with mock.patch("openeo_driver.utils.uuid") as uuid:
        fake_uuid = "0123456789abcdef0123456789abcdef"
        uuid.uuid4.return_value.hex = fake_uuid
        yield fake_uuid


class TestCalrissianJobLauncher:
    NAMESPACE = "test-calrissian"
    BUCKET = "test-calrissian-bucket"
    AWS_ACCESS_KEY_ID = "akid"
    AWS_SECRET_ACCESS_KEY = "secret"
    AWS_SESSION_TOKEN = "token"
    JOB_NAME = "j-hello123"

    @pytest.fixture
    def s3_calrissian_bucket(self) -> str:
        with moto.mock_aws():
            s3 = boto3.client("s3")
            bucket = self.BUCKET
            s3.create_bucket(Bucket=bucket)
            yield bucket

    @pytest.fixture
    def mock_sts(self, monkeypatch) -> Iterator[STSCredentials]:
        c = STSCredentials(
            access_key_id=self.AWS_ACCESS_KEY_ID,
            secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            session_token=self.AWS_SESSION_TOKEN,
            expiration=datetime.datetime.now(),
        )

        with mock.patch("openeogeotrellis.integrations.s3proxy.sts._get_aws_credentials_for_proxy") as g:
            g.return_value = c
            yield c

    @pytest.fixture
    def calrissian_launch_config(self):
        yield CalrissianLaunchConfigBuilder(
            config=CalrissianConfig(s3_bucket=self.BUCKET),
            correlation_id="r-12345678",
        )

    def test_create_input_staging_job_manifest(
        self, generate_unique_id_mock, s3_calrissian_bucket, calrissian_launch_config
    ):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config,
            namespace=self.NAMESPACE,
            name_base="r-1234",
            s3_bucket=s3_calrissian_bucket,
        )

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
                            "name": "r-1234-cal-inp-01234567",
                            "image": "alpine:3",
                            "image_pull_policy": "IfNotPresent",
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
                            "resources": dirty_equals.IsPartialDict(
                                CalrissianJobLauncher._HARD_CODED_STAGE_JOB_RESOURCES
                            ),
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

    def test_create_cwl_job_manifest(self, generate_unique_id_mock, calrissian_launch_config):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config, namespace=self.NAMESPACE, name_base="r-123"
        )

        manifest, output_dir, cwl_outputs_listing = launcher.create_cwl_job_manifest(
            cwl_path="/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
            cwl_arguments=["--message", "Howdy Earth!"],
        )

        assert output_dir == "r-123-cal-cwl-01234567"
        assert cwl_outputs_listing == "r-123-cal-cwl-01234567.cwl-outputs.json"

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
                            "image": DEFAULT_CALRISSIAN_IMAGE,
                            "image_pull_policy": "IfNotPresent",
                            "command": ["calrissian"],
                            "args": dirty_equals.Contains(
                                "--tmp-outdir-prefix",
                                "/calrissian/tmpout/",
                                "--outdir",
                                "/calrissian/output-data/r-123-cal-cwl-01234567",
                                "--stdout",
                                "/calrissian/output-data/r-123-cal-cwl-01234567.cwl-outputs.json",
                                "/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
                                "--message",
                                "Howdy Earth!",
                            ),
                            "volume_mounts": dirty_equals.Contains(
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
                            ),
                        }
                    )
                ],
                "volumes": dirty_equals.Contains(
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
                ),
            }
        )

    def test_create_cwl_job_manifest_base_arguments(self, generate_unique_id_mock, calrissian_launch_config):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config,
            namespace=self.NAMESPACE,
            name_base="r-123",
            calrissian_base_arguments=["--max-ram", "64kB", "--max-cores", "42", "--flavor", "chocolate"],
        )

        manifest, output_dir, cwl_outputs_listing = launcher.create_cwl_job_manifest(
            cwl_path="/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
            cwl_arguments=["--message", "Howdy Earth!"],
        )

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        assert manifest_dict["spec"]["template"]["spec"]["containers"] == [
            dirty_equals.IsPartialDict(
                {
                    "name": "r-123-cal-cwl-01234567",
                    "command": ["calrissian"],
                    "args": dirty_equals.Contains(
                        "--max-ram",
                        "64kB",
                        "--max-cores",
                        "42",
                        "--flavor",
                        "chocolate",
                        "--tmp-outdir-prefix",
                        "/calrissian/tmpout/",
                        "--outdir",
                        "/calrissian/output-data/r-123-cal-cwl-01234567",
                        "--stdout",
                        "/calrissian/output-data/r-123-cal-cwl-01234567.cwl-outputs.json",
                        "/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
                        "--message",
                        "Howdy Earth!",
                    ),
                }
            )
        ]

    @pytest.fixture()
    def k8s_core_v1_api(self):
        """Mock k8s interactions with CoreV1API but don't do special checks"""
        with mock.patch("kubernetes.client.CoreV1Api") as CoreV1Api:
            yield CoreV1Api

    @pytest.fixture()
    def k8s_pvc_api(self, k8s_core_v1_api):
        """Mock for PVC API in kubernetes.client.CoreV1Api"""
        pvc_to_volume_name = {
            "calrissian-output-data": "1234-abcd-5678-efgh",
        }

        def read_namespaced_persistent_volume_claim(name: str, namespace: str):
            assert namespace == self.NAMESPACE
            return kubernetes.client.V1PersistentVolumeClaim(
                spec=kubernetes.client.V1PersistentVolumeClaimSpec(volume_name=pvc_to_volume_name[name])
            )

        k8s_core_v1_api.return_value.read_namespaced_persistent_volume_claim = read_namespaced_persistent_volume_claim

    @pytest.fixture()
    def k8s_secret_api_verify_mocked_sts(self, k8s_core_v1_api, mock_sts):
        def create_namespaced_secret(namespace: str, body: kubernetes.client.V1Secret):
            assert namespace == self.NAMESPACE
            assert CalrissianLaunchConfigBuilder._ENVIRONMENT_FILE in body.string_data
            env = yaml.safe_load(body.string_data[CalrissianLaunchConfigBuilder._ENVIRONMENT_FILE])
            for cred_part in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
                assert cred_part in env
                assert getattr(self, cred_part) == env[cred_part]

        k8s_core_v1_api.return_value.create_namespaced_secret = create_namespaced_secret
        yield

    def test_get_output_volume_name(self, k8s_pvc_api, calrissian_launch_config):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config, namespace=self.NAMESPACE, name_base="r-123"
        )
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
                if "instant-fail" in name:
                    return kubernetes.client.V1Job(
                        metadata=self.jobs[name].metadata,
                        status=kubernetes.client.V1JobStatus(
                            failed=1,
                        ),
                    )
                else:
                    return kubernetes.client.V1Job(
                        metadata=self.jobs[name].metadata,
                        status=kubernetes.client.V1JobStatus(
                            # TODO: way to specify timeline of job conditions?
                            conditions=[kubernetes.client.V1JobCondition(type="Complete", status="True")]
                        ),
                    )

        with mock.patch("kubernetes.client.BatchV1Api", new=BatchV1Api):
            yield

    def test_launch_job_and_wait_basic(self, k8s_batch_api, caplog, calrissian_launch_config):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config, namespace=self.NAMESPACE, name_base="r-456"
        )
        job_manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(name="cal-123", namespace=self.NAMESPACE)
        )
        result = launcher.launch_job_and_wait(manifest=job_manifest)
        assert isinstance(result, kubernetes.client.V1Job)

        assert caplog.messages[-1] == dirty_equals.IsStr(regex=".*job_name='cal-123'.*final_status='complete'.*")

    def test_launch_job_and_wait_fail(self, k8s_batch_api, caplog, calrissian_launch_config):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config, namespace=self.NAMESPACE, name_base="r-456"
        )
        job_manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(name="cal-123-instant-fail", namespace=self.NAMESPACE)
        )
        with pytest.raises(RuntimeError, match=re.compile(r"CWL Job.*")) as e_info:
            launcher.launch_job_and_wait(manifest=job_manifest)

    def test_run_cwl_workflow_basic(
        self,
        k8s_pvc_api,
        k8s_batch_api,
        generate_unique_id_mock,
        caplog,
        s3_calrissian_bucket,
        calrissian_launch_config,
    ):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config,
            namespace=self.NAMESPACE,
            name_base="r-456",
            s3_region="tatooine-east-1",
            s3_bucket=s3_calrissian_bucket,
        )
        # mock calrissian output listing file in S3
        s3_client().put_object(
            Bucket=s3_calrissian_bucket,
            Key="1234-abcd-5678-efgh/r-456-cal-cwl-01234567.cwl-outputs.json",
            Body=get_test_data_file("parse_cwl_outputs_listing/cwl_outputs_listing_txt.json").open(mode="rb"),
        )
        res = launcher.run_cwl_workflow(
            cwl_source=CwLSource.from_string("class: Dummy"),
            cwl_arguments=["--message", "Howdy Earth!"],
        )
        assert res == {
            "output.txt": CalrissianS3Result(
                s3_region="tatooine-east-1",
                s3_bucket=s3_calrissian_bucket,
                s3_key="1234-abcd-5678-efgh/r-456-cal-cwl-01234567/output.txt",
            ),
        }

    def test_run_cwl_workflow_request_too_much(self, calrissian_launch_config):
        launcher = CalrissianJobLauncher(
            launch_config=calrissian_launch_config,
            namespace=self.NAMESPACE,
            name_base="r-456",
            s3_region="tatooine-east-1",
        )
        cwl = CwLSource.from_resource(anchor="openeogeotrellis.integrations", path="cwl/request_too_much_1.cwl")
        with pytest.raises(ValueError):
            launcher.run_cwl_workflow(
                cwl_source=cwl,
                cwl_arguments=[],
            )

    @pytest.fixture()
    def job_context(self, monkeypatch, tmp_path):
        monkeypatch.setenv(ENV_VAR_OPENEO_BATCH_JOB_ID, self.JOB_NAME)
        cfg_dir = tmp_path / "config"
        cfg_dir.mkdir()
        with open(cfg_dir / "token", "w") as token_fh:
            token_fh.write("secretToken134")
        with gps_config_overrides(batch_job_config_dir=cfg_dir):
            yield

    def test_from_context(
        self,
        monkeypatch,
        generate_unique_id_mock,
        s3_calrissian_bucket,
        k8s_core_v1_api,
        calrissian_launch_config,
        mock_sts,
        job_context,
    ):
        calrissian_config = CalrissianConfig(
            namespace="namezpace",
            input_staging_image="albino:3.14",
            s3_bucket=s3_calrissian_bucket,
        )

        with gps_config_overrides(calrissian_config=calrissian_config):
            launcher = CalrissianJobLauncher.from_context()
            manifest, cwl_path = launcher.create_input_staging_job_manifest(
                cwl_source=CwLSource.from_string("class: Dummy")
            )

        assert cwl_path == "/calrissian/input-data/j-hello123-cal-inp-01234567.cwl"

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()
        assert manifest_dict["metadata"] == dirty_equals.IsPartialDict(
            {
                "name": "j-hello123-cal-inp-01234567",
                "namespace": "namezpace",
            }
        )
        assert manifest_dict["spec"]["template"]["spec"] == dirty_equals.IsPartialDict(
            {
                "containers": [
                    dirty_equals.IsPartialDict(
                        {
                            "name": "j-hello123-cal-inp-01234567",
                            "image": "albino:3.14",
                        }
                    )
                ]
            }
        )

    def test_from_context_sets_environment_variables(
        self,
        monkeypatch,
        generate_unique_id_mock,
        s3_calrissian_bucket,
        k8s_core_v1_api,
        calrissian_launch_config,
        mock_sts,
        job_context,
    ):
        calrissian_config = CalrissianConfig(
            namespace="namezpace",
            input_staging_image="albino:3.14",
            s3_bucket=s3_calrissian_bucket,
        )

        with gps_config_overrides(calrissian_config=calrissian_config):
            launcher = CalrissianJobLauncher.from_context()
            manifest, output_dir, cwl_outputs_listing = launcher.create_cwl_job_manifest(
                cwl_path="/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
                cwl_arguments=["--message", "Howdy Earth!"],
            )

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        pod_template_spec = manifest_dict["spec"]["template"]["spec"]

        assert pod_template_spec["containers"] == [
            dirty_equals.IsPartialDict(
                {
                    "name": "j-hello123-cal-cwl-01234567",
                    "command": ["calrissian"],
                    "args": dirty_equals.Contains(
                        "--pod-env-vars",
                        "/calrissian/config/environment.yaml",
                        "--message",
                        "Howdy Earth!",
                    ),
                }
            )
        ]
        assert pod_template_spec["volumes"] == dirty_equals.Contains(
            dirty_equals.IsPartialDict(
                {
                    "name": "calrissian-launch-config",
                    "secret": dirty_equals.IsPartialDict({"secret_name": self.JOB_NAME}),
                }
            )
        )

        assert pod_template_spec["containers"][0]["volume_mounts"] == dirty_equals.Contains(
            dirty_equals.IsPartialDict(
                {
                    "mount_path": "/calrissian/config",
                    "name": "calrissian-launch-config",
                    "read_only": True,
                }
            )
        )

    def test_launch_from_context_passes_sts_values(
        self,
        monkeypatch,
        generate_unique_id_mock,
        s3_calrissian_bucket,
        k8s_batch_api,
        k8s_secret_api_verify_mocked_sts,
        k8s_pvc_api,
        calrissian_launch_config,
        job_context,
    ):
        calrissian_config = CalrissianConfig(
            namespace=self.NAMESPACE,
            input_staging_image="albino:3.14",
            s3_bucket=s3_calrissian_bucket,
        )

        with gps_config_overrides(calrissian_config=calrissian_config):
            launcher = CalrissianJobLauncher.from_context()

            # Mock calrissian output listing file in S3.
            s3_client().put_object(
                Bucket=s3_calrissian_bucket,
                Key="1234-abcd-5678-efgh/j-hello123-cal-cwl-01234567.cwl-outputs.json",
                Body=get_test_data_file("parse_cwl_outputs_listing/cwl_outputs_listing_txt.json").open(mode="rb"),
            )

            launcher.run_cwl_workflow(
                cwl_source=CwLSource.from_string("class: Dummy"),
                cwl_arguments=["--message", "Howdy Earth!"],
            )

    def test_from_context_sets_container_resources(
        self,
        monkeypatch,
        generate_unique_id_mock,
        s3_calrissian_bucket,
        k8s_core_v1_api,
        calrissian_launch_config,
        mock_sts,
        job_context,
    ):
        @dataclass
        class TestCase:
            description: str
            givenConfigRunnerRequirents: Optional[dict]
            expectedResourcesDict: dict

        testcases = [
            TestCase(
                description="ConfigOverrides should be set",
                givenConfigRunnerRequirents=dict(limits={"memory": "10Mi"}, requests={"cpu": "1m", "memory": "10Mi"}),
                expectedResourcesDict={"limits": {"memory": "10Mi"}, "requests": {"memory": "10Mi", "cpu": "1m"}},
            ),
            TestCase(
                description="If no config overrides we expect the defaults to be set",
                givenConfigRunnerRequirents=None,
                expectedResourcesDict=DEFAULT_CALRISSIAN_RUNNER_RESOURCE_REQUIREMENTS,
            ),
        ]

        for testcase in testcases:
            calrissian_config = CalrissianConfig(
                namespace="namezpace",
                input_staging_image="albino:3.14",
                s3_bucket=s3_calrissian_bucket,
                runner_resource_requirements=testcase.givenConfigRunnerRequirents,
            )

            with gps_config_overrides(calrissian_config=calrissian_config):
                launcher = CalrissianJobLauncher.from_context()
                manifest, output_dir, cwl_outputs_listing = launcher.create_cwl_job_manifest(
                    cwl_path="/calrissian/input-data/r-1234-cal-inp-01234567.cwl",
                    cwl_arguments=["--message", "Howdy Earth!"],
                )

            assert isinstance(manifest, kubernetes.client.V1Job)
            manifest_dict = manifest.to_dict()

            pod_template_spec = manifest_dict["spec"]["template"]["spec"]

            assert pod_template_spec["containers"] == [
                dirty_equals.IsPartialDict(
                    {
                        "name": "j-hello123-cal-cwl-01234567",
                        "command": ["calrissian"],
                        "args": dirty_equals.Contains(
                            "--pod-env-vars",
                            "/calrissian/config/environment.yaml",
                            "--message",
                            "Howdy Earth!",
                        ),
                        "resources": dirty_equals.IsPartialDict(testcase.expectedResourcesDict),
                    }
                )
            ]

    def test_no_sync(self):
        env = EvalEnv({"sync_job": "true"})
        # Sync jobs are disabled for CWL, as it has no credit tracking.
        with pytest.raises(RuntimeError):
            cwl_to_stac(
                cwl_arguments=[],
                env=env,
                cwl_source=CwLSource.from_string("class: Dummy"),
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

    def test_s3_uri(self):
        result = CalrissianS3Result(s3_bucket="bucky", s3_key="path/to/collection.json")
        assert result.s3_uri() == "s3://bucky/path/to/collection.json"

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

    def test_generate_public_url(self, s3_output, monkeypatch):
        monkeypatch.setenv("SWIFT_URL", "https://s3.example.com")
        bucket, key = s3_output
        result = CalrissianS3Result(s3_bucket=bucket, s3_key=key)
        assert result.generate_public_url() == "https://s3.example.com/the-bucket/path/to/output.txt"

    def test_download(self, s3_output, tmp_path):
        bucket, key = s3_output
        result = CalrissianS3Result(s3_bucket=bucket, s3_key=key)
        path = tmp_path / "result.data"
        result.download(path)
        assert path.read_bytes() == b"Howdy, Earth!"

    def test_download_to_dir(self, s3_output, tmp_path):
        bucket, key = s3_output
        result = CalrissianS3Result(s3_bucket=bucket, s3_key=key)
        folder = tmp_path / "data"
        folder.mkdir(parents=True)
        path = result.download(target=folder)
        assert path.relative_to(folder) == Path("output.txt")
        assert path.read_bytes() == b"Howdy, Earth!"


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

    @pytest.mark.parametrize(
        ["cwl_path", "expected_memory"],
        [
            ("cwl/request_too_much_1.cwl", 999000),
            ("cwl/request_too_much_2.cwl", 999000),
            ("cwl/request_too_much_3.cwl", 999000),
            ("cwl/hello.cwl", -1),
            ("cwl/multistep.cwl", 7000),
        ],
    )
    def test_request_too_much(self, cwl_path, expected_memory):
        cwl = CwLSource.from_resource(anchor="openeogeotrellis.integrations", path=cwl_path)
        max_memory = cwl.estimate_max_memory_usage()
        print(f"{max_memory=}")
        print(f"{expected_memory=}")
        assert max_memory == expected_memory


class TestCalrissianUtils:
    def test_parse_cwl_outputs_listing_directory(self):
        results = parse_cwl_outputs_listing(
            json.load(get_test_data_file("parse_cwl_outputs_listing/cwl_outputs_listing_directory.json").open())
        )
        print(results)
        assert len(results) == 7
        assert results[0].startswith("r-")

    def test_parse_cwl_outputs_listing_directory_force(self):
        # JSON trimmed for brevity. Got json from cwltool without calrissian.
        results = parse_cwl_outputs_listing(
            json.load(get_test_data_file("parse_cwl_outputs_listing/cwl_outputs_listing_directory_force.json").open())
        )
        print(results)
        assert len(results) == 5
        assert results[0].startswith("/home/emile/openeo/apex-force-openeo/l2-ard")

    def test_parse_cwl_outputs_listing_file_array(self):
        results = parse_cwl_outputs_listing(
            json.load(get_test_data_file("parse_cwl_outputs_listing/cwl_outputs_listing_file_array.json").open())
        )
        print(results)
        assert len(results) == 7
        assert results[0].startswith("r-")

    def test_parse_cwl_outputs_listing_txt(self):
        results = parse_cwl_outputs_listing(
            json.load(get_test_data_file("parse_cwl_outputs_listing/cwl_outputs_listing_txt.json").open())
        )
        print(results)
        assert len(results) == 1

    def test_find_stac_root_dictionary(self):
        listing_directory = [
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/collection.json",
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/openEO_2023-06-01Z.tif",
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/openEO_2023-06-01Z.tif.json",
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/openEO_2023-06-04Z.tif",
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/openEO_2023-06-04Z.tif.json",
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/openEO_2023-06-06Z.tif",
            "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/openEO_2023-06-06Z.tif.json",
        ]
        result = find_stac_root(listing_directory)
        assert result
        assert isinstance(result, str)
        assert result == "r-2512020921004d489e-cal-cwl-e706a8a7/o1kip6_h/collection.json"

    def test_find_stac_root_file_array_01(self):
        listing_directory = [
            "aaa/collection.json",
            "bbb/collection-custom.json",
        ]
        result = find_stac_root(listing_directory, "collection-custom.json")
        assert result
        assert isinstance(result, str)
        assert result == "bbb/collection-custom.json"

    def test_find_stac_root_file_array_02(self):
        listing_directory = [
            "aaa/collection.json",
            "bbb/collection-custom.json",
        ]
        result = find_stac_root(listing_directory)
        assert result
        assert isinstance(result, str)
        assert result == "aaa/collection.json"

    def test_find_stac_root_file_array_03(self):
        listing_directory = [
            "aaa/collection.json",
            "bbb/collection-custom.json",
            "ccc/catalog.json",
            "ddd/catalogue.json",
        ]
        result = find_stac_root(listing_directory)
        assert result
        assert isinstance(result, str)
        assert result == "ccc/catalog.json"

    def test_find_stac_root_file_array_04(self):
        listing_directory = [
            "aaa/collection.json",
            "bbb/collection-custom.json",
            "ddd/catalogue.json",
        ]
        result = find_stac_root(listing_directory)
        assert result
        assert isinstance(result, str)
        assert result == "ddd/catalogue.json"
