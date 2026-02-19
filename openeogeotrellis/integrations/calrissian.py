from __future__ import annotations

import base64
import dataclasses
import json
import logging
import os
import textwrap
import time
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

import kubernetes.client
import requests
import yaml
from openeo.util import ContextTimer
from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER
from openeo_driver.backend import ErrorSummary
from openeo_driver.config import ConfigException
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.utils import EvalEnv, generate_unique_id, smart_bool

from openeogeotrellis.config import get_backend_config, s3_config
from openeogeotrellis.config.integrations.calrissian_config import (
    DEFAULT_CALRISSIAN_BASE_ARGUMENTS,
    DEFAULT_CALRISSIAN_IMAGE,
    DEFAULT_CALRISSIAN_S3_BUCKET,
    DEFAULT_CALRISSIAN_S3_REGION,
    DEFAULT_INPUT_STAGING_IMAGE,
    DEFAULT_SECURITY_CONTEXT,
    DEFAULT_CALRISSIAN_RUNNER_RESOURCE_REQUIREMENTS,
    CalrissianConfig,
)
from openeogeotrellis.integrations.kubernetes import ensure_kubernetes_config
from openeogeotrellis.integrations.s3proxy import sts
from openeogeotrellis.util.runtime import get_job_id, get_request_id, ENV_VAR_OPENEO_BATCH_JOB_ID
from openeogeotrellis.utils import s3_client

try:
    # TODO #1060 importlib.resources on Python 3.8 is pretty limited so we need backport
    import importlib_resources
except ImportError:
    import importlib.resources

    importlib_resources = importlib.resources


_log = logging.getLogger(__name__)


class CalrissianLaunchConfigBuilder:
    """
    A helper to create config files that can be staged within a path that is mounted in the container that
    issues the calrissian command. It also helps to provide the calrissian arguments to use these files.
    """

    _BASE_PATH = "/calrissian/config"
    _VOLUME_NAME = "calrissian-launch-config"
    _ENVIRONMENT_FILE = "environment.yaml"
    _POD_LABELS_FILE = "pod-labels.json"

    def __init__(self, *, config: CalrissianConfig, correlation_id: str, env_vars: Optional[Dict[str, str]] = None):
        self.correlation_id = correlation_id
        self._config = config
        self._env_vars = deepcopy(env_vars or {})

    def get_files_dict(self):
        """
        Get a dictionary that maps filenames that are relative to the /calrissian/config directory to their content
        """
        return {
            self._ENVIRONMENT_FILE: yaml.safe_dump(self._env_vars),
            self._POD_LABELS_FILE: json.dumps({"correlation_id": self.correlation_id}),
        }

    def get_k8s_manifest(self, job: str):
        return kubernetes.client.V1Secret(
            metadata=kubernetes.client.V1ObjectMeta(
                name=self.correlation_id, namespace=self._config.namespace, labels={"calrissian-job": job}
            ),
            string_data=self.get_files_dict(),
        )

    def create_secret_for_files(self, job: str):
        kubernetes.client.CoreV1Api().create_namespaced_secret(
            namespace=self._config.namespace, body=self.get_k8s_manifest(job=job)
        )
        _log.debug(f"Secret created for calrissian {self.correlation_id=}")

    def cleanup_secret_for_files(self):
        try:
            kubernetes.client.CoreV1Api().delete_namespaced_secret(
                name=self.correlation_id,
                namespace=self._config.namespace,
            )
        except Exception as e:
            _log.warning(f"Exception when cleaning up calrissian secret {self.correlation_id}", exc_info=e)

    def get_volume_mount(self) -> kubernetes.client.V1VolumeMount:
        return kubernetes.client.V1VolumeMount(name=self._VOLUME_NAME, mount_path=self._BASE_PATH, read_only=True)

    def get_volume(self) -> kubernetes.client.V1Volume:
        return kubernetes.client.V1Volume(
            name=self._VOLUME_NAME,
            secret=kubernetes.client.V1SecretVolumeSource(
                secret_name=self.correlation_id,
                default_mode=0o444,
            ),
        )

    def get_calrissian_args(self) -> list[str]:
        return [
            "--pod-env-vars", f"{self._BASE_PATH}/{self._ENVIRONMENT_FILE}",
            "--pod-labels", f"{self._BASE_PATH}/{self._POD_LABELS_FILE}"
        ]


@dataclasses.dataclass(frozen=True)
class VolumeInfo:
    name: str
    claim_name: str
    mount_path: str


@dataclasses.dataclass(frozen=True)
class CalrissianS3Result:
    s3_bucket: str
    s3_key: str
    s3_region: Optional[str] = None

    def s3_uri(self) -> str:
        return f"s3://{self.s3_bucket}/{self.s3_key}"

    def read(self, encoding: Union[None, str] = None) -> Union[bytes, str]:
        # mocking might give invalid values. Check for them:
        assert "<" not in self.s3_bucket, self.s3_bucket
        assert "<" not in self.s3_key, self.s3_key

        _log.info(f"Reading from S3: {self.s3_bucket=}, {self.s3_key=}")
        s3_file_object = s3_client().get_object(Bucket=self.s3_bucket, Key=self.s3_key)
        body = s3_file_object["Body"]
        content = body.read()
        if encoding:
            content = content.decode(encoding)
        return content

    def generate_presigned_url(self, expiration=3600) -> str:
        return s3_client().generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.s3_bucket, "Key": self.s3_key},
            ExpiresIn=expiration,
        )

    def generate_public_url(self) -> str:
        """Assuming the object (or its bucket) is public: generate a public URL"""
        # TODO (#1133 related) is there a better way than just chopping off the query string from a pre-signed URL?
        url = self.generate_presigned_url()
        return url.split("?", maxsplit=1)[0]

    def download(self, target: Union[str, Path]) -> Path:
        target = Path(target)
        if target.is_dir():
            target = target / Path(self.s3_key).name
        _log.info(f"Downloading from S3: {self.s3_bucket=} {self.s3_key=} to {target=}")
        with target.open(mode="wb") as f:
            s3_client().download_fileobj(Bucket=self.s3_bucket, Key=self.s3_key, Fileobj=f)
        return target


class CwLSource:
    """
    Simple container of CWL source code.

    For now, this is just a wrapper around a string (the CWL content),
    with factories to support multiple sources (local path, URL, ...).
    When necessary, this simple abstraction can be evolved easily in something more sophisticated.
    """

    def __init__(self, content: str):
        self._cwl = content
        yaml_parsed = list(yaml.safe_load_all(self._cwl))
        assert len(yaml_parsed) >= 1

    def get_content(self) -> str:
        return self._cwl

    @classmethod
    def from_any(cls, content: str) -> CwLSource:
        # noinspection HttpUrlsUsage
        if content.lower().startswith("http://") or content.lower().startswith("https://"):
            return cls.from_url(content)
        elif (
            content.lower().endswith(".cwl")
            or content.lower().endswith(".yaml")
            and not "\n" in content
            and not content.startswith("{")
        ):
            return cls.from_path(content)
        else:
            return cls(content=content)

    @classmethod
    def from_string(cls, content: str, auto_dedent: bool = True) -> CwLSource:
        if auto_dedent:
            content = textwrap.dedent(content)
        return cls(content=content)

    @classmethod
    def from_path(cls, path: Union[str, Path]) -> CwLSource:
        with Path(path).open(mode="r", encoding="utf-8") as f:
            return cls(content=f.read())

    @classmethod
    def from_url(cls, url: str) -> CwLSource:
        resp = requests.get(url)
        resp.raise_for_status()
        return cls(content=resp.text)

    @classmethod
    def from_resource(cls, anchor: str, path: str) -> CwLSource:
        """
        Read CWL from a packaged resource file in importlib.resources-style.
        """
        content = importlib_resources.files(anchor).joinpath(path).read_text(encoding="utf-8")
        return cls(content=content)


class CalrissianJobLauncher:
    """
    Helper class to launch a Calrissian job on Kubernetes.
    """

    # Hard code given the small but predictable requirements
    _HARD_CODED_STAGE_JOB_RESOURCES = {
        "limits": {"memory": "10Mi"},
        "requests": {"cpu": "1m", "memory": "10Mi"},
    }

    def __init__(
        self,
        *,
        namespace: str,
        launch_config: CalrissianLaunchConfigBuilder,
        name_base: Optional[str] = None,
        s3_region: Optional[str] = DEFAULT_CALRISSIAN_S3_REGION,
        s3_bucket: str = DEFAULT_CALRISSIAN_S3_BUCKET,
        calrissian_image: str = DEFAULT_CALRISSIAN_IMAGE,
        input_staging_image: str = DEFAULT_INPUT_STAGING_IMAGE,
        security_context: Optional[dict] = None,
        backoff_limit: int = 1,
        calrissian_base_arguments: Sequence[str] = DEFAULT_CALRISSIAN_BASE_ARGUMENTS,
        runner_resource_requirements: Optional[dict] = None,
    ):
        self._namespace = namespace
        self._name_base = name_base or generate_unique_id(prefix="cal")[:20]
        self._s3_region = s3_region
        self._s3_bucket = s3_bucket
        _log.info(f"CalrissianJobLauncher.__init__: {self._namespace=} {self._name_base=} {self._s3_bucket=}")

        self._calrissian_image = calrissian_image
        self._input_staging_image = input_staging_image
        self._backoff_limit = backoff_limit
        self._calrissian_base_arguments = list(calrissian_base_arguments)
        self._calrissian_launch_config = launch_config

        self._volume_input = VolumeInfo(
            name="calrissian-input-data",
            claim_name="calrissian-input-data",
            mount_path="/calrissian/input-data",
        )
        self._volume_tmp = VolumeInfo(
            name="calrissian-tmpout",
            claim_name="calrissian-tmpout",
            mount_path="/calrissian/tmpout",
        )
        self._volume_output = VolumeInfo(
            name="calrissian-output-data",
            claim_name="calrissian-output-data",
            mount_path="/calrissian/output-data",
        )

        self._security_context = kubernetes.client.V1PodSecurityContext(
            **(security_context or DEFAULT_SECURITY_CONTEXT)
        )

        self._runner_resource_requirements = kubernetes.client.V1ResourceRequirements(
            **(runner_resource_requirements or DEFAULT_CALRISSIAN_RUNNER_RESOURCE_REQUIREMENTS)
        )

    @staticmethod
    def get_s3proxy_access_env_vars(user_id: str, correlation_id: str) -> dict:
        """
        Get environment variables which configure S3 SDKs to allow access to earth observation data provided via an
        S3 proxy/endpoint
        """
        s3_endpoint = s3_config.AWSConfig.get_s3_endpoint_url()
        sts_endpoint = s3_config.AWSConfig.get_sts_endpoint_url()

        credential_session_details = {
            "role_arn": "arn:openeo:iam:::role/eodata",
            "session_name": "calrissian-eodata-access",
        }

        if get_job_id(default=None) is None:
            # Synchronous request so we are on webappdriver
            s3_credentials = sts.get_job_aws_credentials_for_proxy(
                job_id=correlation_id, user_id=user_id, **credential_session_details
            )
        else:
            s3_credentials = sts.get_aws_credentials_for_proxy_for_running_job(**credential_session_details)

        return {
            "AWS_ENDPOINT_URL_S3": s3_endpoint,
            "AWS_ENDPOINT_URL_STS": sts_endpoint,
            "AWS_REGION": "eodata",
            "AWS_VIRTUAL_HOSTING": "FALSE",
            **s3_credentials.as_env_vars(),
        }

    @staticmethod
    def from_context(env: Optional[EvalEnv] = None) -> CalrissianJobLauncher:
        """
        Factory for creating a CalrissianJobLauncher from the current context
        (e.g. using job_id/request_id for base name).
        """
        correlation_id = get_job_id(default=None) or get_request_id(default=None)
        name_base = correlation_id[:20] if correlation_id else None

        config: CalrissianConfig = get_backend_config().calrissian_config
        if not config:
            raise ConfigException("No calrissian (sub)config.")

        # To avoid custom logic in every calrissian container the default environment variables for S3 access are set.
        try:
            env_vars = CalrissianJobLauncher.get_s3proxy_access_env_vars(
                user_id=str(env.get("user", "unknown")) if env is not None else "", correlation_id=correlation_id
            )
        except Exception as e:
            detail = f"{e!r}"
            if env:
                error_summary = env.backend_implementation.summarize_exception(e)
                if isinstance(error_summary, ErrorSummary):
                    detail = error_summary.summary
            env_vars = {}
            if (
                "AWS_ENDPOINT_URL_S3" in os.environ
                and "AWS_ACCESS_KEY_ID" in os.environ
                and "AWS_SECRET_ACCESS_KEY" in os.environ
            ):
                env_vars["AWS_ENDPOINT_URL_S3"] = os.environ["AWS_ENDPOINT_URL_S3"]
                env_vars["AWS_ACCESS_KEY_ID"] = os.environ["AWS_ACCESS_KEY_ID"]
                env_vars["AWS_SECRET_ACCESS_KEY"] = os.environ["AWS_SECRET_ACCESS_KEY"]
                _log.info(f"Falling back to local s3 credentials.")  # for local debugging
            else:
                _log.warning(f"Failed to get s3 credentials: {detail}")

        if "OPENEO_USER_ID" in os.environ:
            env_vars["OPENEO_USER_ID"] = os.environ["OPENEO_USER_ID"]


        launch_config = CalrissianLaunchConfigBuilder(
            config=config,
            correlation_id=correlation_id,
            env_vars=env_vars,
        )

        return CalrissianJobLauncher(
            namespace=config.namespace,
            launch_config=launch_config,
            name_base=name_base,
            s3_region=config.s3_region,
            s3_bucket=config.s3_bucket,
            security_context=config.security_context,
            calrissian_image=config.calrissian_image,
            input_staging_image=config.input_staging_image,
            runner_resource_requirements=config.runner_resource_requirements,
        )

    def _build_unique_name(self, infix: str) -> str:
        """Build unique name from base name, an infix and something random, to be used for k8s resources"""
        suffix = generate_unique_id(date_prefix=False)[:8]
        return f"{self._name_base}-{infix}-{suffix}"

    def create_input_staging_job_manifest(self, cwl_source: CwLSource) -> Tuple[kubernetes.client.V1Job, str]:
        """
        Create a k8s manifest for a Calrissian input staging job.

        :param cwl_source: CWL source to dump to CWL file in the input volume.
        :return: Tuple of
            - k8s job manifest
            - path to the CWL file in the input volume.
        """
        cwl_content = cwl_source.get_content()

        name = self._build_unique_name(infix="cal-inp")
        _log.info(f"Creating input staging job manifest: {name=}")
        # Serialize CWL content to string that is safe to pass as command line argument
        cwl_serialized = base64.b64encode(cwl_content.encode("utf8")).decode("ascii")
        # TODO #1008 cleanup procedure of these CWL files?
        cwl_path = str(Path(self._volume_input.mount_path) / f"{name}.cwl")
        _log.info(
            f"create_input_staging_job_manifest creating {cwl_path=} from {cwl_content[:32]=} through {cwl_serialized[:32]=}"
        )

        s3_client().head_bucket(Bucket=self._s3_bucket)  # check if the bucket exists

        container = kubernetes.client.V1Container(
            name=name,
            image=self._input_staging_image,
            image_pull_policy="IfNotPresent",  # Avoid 'Always' as artifactory might be down.
            security_context=self._security_context,
            command=["/bin/sh"],
            args=["-c", f"set -euxo pipefail; echo '{cwl_serialized}' | base64 -d > {cwl_path}"],
            volume_mounts=[
                kubernetes.client.V1VolumeMount(
                    name=self._volume_input.name, mount_path=self._volume_input.mount_path, read_only=False
                )
            ],
            resources=kubernetes.client.V1ResourceRequirements(**self._HARD_CODED_STAGE_JOB_RESOURCES),
        )
        manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
                labels={"correlation_id": self._calrissian_launch_config.correlation_id},
            ),
            spec=kubernetes.client.V1JobSpec(
                active_deadline_seconds=300,  # could be because target bucket does not exist
                ttl_seconds_after_finished=300,  # can get stuck in "Terminating" state
                template=kubernetes.client.V1PodTemplateSpec(
                    spec=kubernetes.client.V1PodSpec(
                        containers=[container],
                        restart_policy="Never",
                        volumes=[
                            kubernetes.client.V1Volume(
                                name=self._volume_input.name,
                                persistent_volume_claim=kubernetes.client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=self._volume_input.claim_name,
                                    read_only=False,
                                ),
                            )
                        ],
                    ),
                    metadata=kubernetes.client.V1ObjectMeta(
                        labels={"correlation_id": self._calrissian_launch_config.correlation_id},
                    ),
                ),
                backoff_limit=self._backoff_limit,
            ),
        )

        return manifest, cwl_path

    def create_cwl_job_manifest(
        self,
        cwl_path: str,
        cwl_arguments: List[str],
        env_vars: Optional[Dict[str, str]] = None,
    ) -> Tuple[kubernetes.client.V1Job, str, str]:
        """
        Create a k8s manifest for a Calrissian CWL job.

        :param cwl_path: path to the CWL file to run (inside the input staging volume),
            as produced by `create_input_staging_job_manifest`
        :param cwl_arguments:
        :param env_vars:
        :return: Tuple of
            - k8s job manifest
            - relative output directory (inside the output volume)
            - relative path to the CWL outputs listing (JSON dump inside the output volume)
        """
        name = self._build_unique_name(infix="cal-cwl")
        _log.info(f"Creating CWL job manifest: {name=}")

        # Pairs of (volume_info, read_only)
        volumes = [(self._volume_input, True), (self._volume_tmp, False), (self._volume_output, False)]

        # Ensure trailing "/" so that `tmp-outdir-prefix` is handled as a root directory.
        tmp_dir = self._volume_tmp.mount_path.rstrip("/") + "/"
        relative_output_dir = name
        relative_cwl_outputs_listing = f"{name}.cwl-outputs.json"
        output_dir = str(Path(self._volume_output.mount_path) / relative_output_dir)
        cwl_outputs_listing = str(Path(self._volume_output.mount_path) / relative_cwl_outputs_listing)

        labels_dict = {"correlation_id": self._calrissian_launch_config.correlation_id}

        calrissian_arguments = (
            self._calrissian_base_arguments
            + self._calrissian_launch_config.get_calrissian_args()
            + [
                "--tmp-outdir-prefix",
                tmp_dir,
                "--outdir",
                output_dir,
                "--stdout",
                cwl_outputs_listing,
                cwl_path,
            ]
            + cwl_arguments
        )

        print(f"create_cwl_job_manifest {calrissian_arguments=}")
        _log.info(f"create_cwl_job_manifest {calrissian_arguments=}")

        container_env_vars = [
            kubernetes.client.V1EnvVar(
                name="CALRISSIAN_POD_NAME",
                value_from=kubernetes.client.V1EnvVarSource(
                    field_ref=kubernetes.client.V1ObjectFieldSelector(field_path="metadata.name")
                ),
            ),
            kubernetes.client.V1EnvVar(
                name="RETRY_ATTEMPTS",  # Otherwise calrissian retry backoff till take 2400sec (40min)
                value="3",
            ),
            kubernetes.client.V1EnvVar(
                name="CALRISSIAN_STREAM_LOGS",  # Otherwise calrissian & logshipper streams logs
                value="NO",
            )
        ]
        if env_vars:
            container_env_vars.extend(kubernetes.client.V1EnvVar(name=k, value=v) for k, v in env_vars.items())

        container = kubernetes.client.V1Container(
            name=name,
            image=self._calrissian_image,
            image_pull_policy="IfNotPresent",  # Avoid 'Always' as artifactory might be down.
            security_context=self._security_context,
            command=["calrissian"],
            args=calrissian_arguments,
            volume_mounts=[
                kubernetes.client.V1VolumeMount(
                    name=volume_info.name, mount_path=volume_info.mount_path, read_only=read_only
                )
                for volume_info, read_only in volumes
            ]
            + [self._calrissian_launch_config.get_volume_mount()],
            env=container_env_vars,
            resources=self._runner_resource_requirements,
        )
        manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
                labels=labels_dict,
            ),
            spec=kubernetes.client.V1JobSpec(
                template=kubernetes.client.V1PodTemplateSpec(
                    spec=kubernetes.client.V1PodSpec(
                        containers=[container],
                        restart_policy="Never",
                        volumes=[
                            kubernetes.client.V1Volume(
                                name=volume_info.name,
                                persistent_volume_claim=kubernetes.client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=volume_info.claim_name,
                                    read_only=read_only,
                                ),
                            )
                            for volume_info, read_only in volumes
                        ]
                        + [self._calrissian_launch_config.get_volume()],
                    ),
                    metadata=kubernetes.client.V1ObjectMeta(
                        labels=labels_dict,
                    ),
                ),
                backoff_limit=self._backoff_limit,
            ),
        )
        return manifest, relative_output_dir, relative_cwl_outputs_listing

    def launch_job_and_wait(
        self,
        manifest: kubernetes.client.V1Job,
        *,
        sleep: float = 5,
        # 12h is also the max timeout for sts tokens. A bit shorter, so the process times out before the sts.
        timeout: float = 60 * 60 * 12 - 120,
    ) -> kubernetes.client.V1Job:
        """
        Launch a k8s job and wait (with active polling) for it to finish.
        """

        k8s_batch = kubernetes.client.BatchV1Api()

        # Launch job.
        job: kubernetes.client.V1Job = k8s_batch.create_namespaced_job(
            namespace=self._namespace,
            body=manifest,
        )
        job_name = job.metadata.name
        _log.info(
            f"Created CWL job {job.metadata.name=} {job.metadata.namespace=} {job.metadata.creation_timestamp=} {job.metadata.uid=}"
        )
        # TODO: add assertions here about successful job creation?

        # Track job status (active polling).
        final_status = None
        try:
            with ContextTimer() as timer:
                while timer.elapsed() < timeout:
                    job = k8s_batch.read_namespaced_job(name=job_name, namespace=self._namespace)
                    _log.info(f"CWL job poll loop: {job_name=} {timer.elapsed()=:.2f} {job.status.to_dict()=}")
                    if job.status.failed == 1:
                        final_status = "failed"
                        break
                    if job.status.conditions:
                        if any(c.type == "Failed" and c.status == "True" for c in job.status.conditions):
                            final_status = "failed"
                            break
                        elif any(c.type == "Complete" and c.status == "True" for c in job.status.conditions):
                            final_status = "complete"
                            break
                    time.sleep(sleep)
        except (SystemExit, KeyboardInterrupt):
            _log.info(f"Stopping CWL job: {job_name=}")
            k8s_batch.delete_namespaced_job(
                name=job_name,
                namespace=self._namespace,
                propagation_policy="Foreground",  # Make sure the pods in this job get deleted too.
                async_req=True,  # Prefer to fail deleting compared to waiting in a terminating state.
            )
            raise

        _log.info(f"CWL job poll loop done: {job_name=} {timer.elapsed()=:.2f} {final_status=}")
        if final_status == "complete":
            pass
        elif final_status is None:
            raise TimeoutError(f"CWL Job {job_name} did not finish within {timeout}s")
        elif final_status != "complete":
            messages = ""
            if job.status.conditions:
                messages = f" Messages: {set(c.message for c in job.status.conditions)}."
            raise RuntimeError(
                f"CWL Job {job_name} failed with {final_status=} after {timer.elapsed()=:.2f}s.{messages}"
            )
        else:
            raise ValueError("CWL")

        # TODO #1008 automatic cleanup after success?
        # TODO #1008 automatic cleanup after fail?
        return job

    def get_output_volume_name(self) -> str:
        """Get the actual name of the output volume claim."""
        core_api = kubernetes.client.CoreV1Api()
        pvc = core_api.read_namespaced_persistent_volume_claim(
            name=self._volume_output.claim_name, namespace=self._namespace
        )
        volume_name = pvc.spec.volume_name
        return volume_name

    def run_cwl_workflow(
        self,
        cwl_source: CwLSource,
        cwl_arguments: Union[List[str], dict],
        env_vars: Optional[Dict[str, str]] = None,
    ) -> Dict[str, CalrissianS3Result]:
        """
        Run a CWL workflow on Calrissian and return the output as a string.

        :param cwl_source:
        :param cwl_arguments: arguments to pass to the CWL workflow.
        :param env_vars: environment variables set to the pod that runs calrissian these are not passsed to pods spawned
                         by calrissian.
        :return: output of the CWL workflow as a string.
        """
        # Input staging
        input_staging_manifest, cwl_path = self.create_input_staging_job_manifest(cwl_source=cwl_source)
        self.launch_job_and_wait(manifest=input_staging_manifest)

        if isinstance(cwl_arguments, dict):
            cwl_source_arguments = CwLSource.from_string(json.dumps(cwl_arguments))

            input_staging_arguments_manifest, cwl_arguments_path = self.create_input_staging_job_manifest(
                cwl_source=cwl_source_arguments
            )
            input_staging_arguments_job = self.launch_job_and_wait(manifest=input_staging_arguments_manifest)
            cwl_arguments = [cwl_arguments_path]

        # CWL job
        cwl_manifest, relative_output_dir, relative_cwl_outputs_listing = self.create_cwl_job_manifest(
            cwl_path=cwl_path,
            cwl_arguments=cwl_arguments,
            env_vars=env_vars,
        )

        # Calrissian secret for launch config file
        self._calrissian_launch_config.create_secret_for_files(job=cwl_manifest.metadata.name)

        cwl_job = self.launch_job_and_wait(manifest=cwl_manifest)
        self._calrissian_launch_config.cleanup_secret_for_files()

        # Collect results
        _log.info(f"run_cwl_workflow: {relative_cwl_outputs_listing}")
        output_volume_name = self.get_output_volume_name()
        outputs_listing_result = CalrissianS3Result(
            s3_region=self._s3_region,
            s3_bucket=self._s3_bucket,
            s3_key=f"{output_volume_name}/{relative_cwl_outputs_listing.strip('/')}",
        )
        j = json.loads(outputs_listing_result.read())
        outputs_listing_result_paths = parse_cwl_outputs_listing(j)
        prefix = relative_output_dir.strip("/") + "/"
        output_paths = [p[len(prefix) :] if p.startswith(prefix) else p for p in outputs_listing_result_paths]

        results = {
            output_path: CalrissianS3Result(
                s3_region=self._s3_region,
                s3_bucket=self._s3_bucket,
                s3_key=f"{output_volume_name}/{relative_output_dir.strip('/')}/{output_path.strip('/')}",
            )
            for output_path in output_paths
        }
        return results


def parse_cwl_outputs_listing(cwl_outputs_listing: dict) -> List[str]:
    def recurse(obj):
        if isinstance(obj, list):
            list_list = [recurse(item) for item in obj]
            # flatten lists:
            return [item for sublist in list_list for item in sublist]
        if obj["class"] == "File":
            return [obj["path"]]
        elif obj["class"] == "Directory":
            list_list = [recurse(item) for item in obj.get("listing", [])]
            # flatten lists:
            return [item for sublist in list_list for item in sublist]
        else:
            raise ValueError(f"Unknown class in CWL output: {obj['class']}")

    results_list = [recurse(cwl_outputs_listing[key]) for key in cwl_outputs_listing.keys()]
    results = [item for sublist in results_list for item in sublist]
    prefix = "/calrissian/output-data/"
    results = [p[len(prefix) :] if p.startswith(prefix) else p for p in results]
    return results


def find_stac_root(paths: set, stac_root_filename: Optional[str] = "catalog.json") -> Optional[str]:
    paths = [Path(p) for p in paths]

    def search(stac_root_filename_local: str):
        matches = [x for x in paths if x.name == stac_root_filename_local]
        if matches:
            if len(matches) > 1:
                _log.warning(f"Multiple STAC root files found: {[str(x) for x in matches]}. Using the first one.")
            return str(matches[0])
        return None

    if stac_root_filename:
        ret = search(stac_root_filename)
        if ret:
            return ret
    ret = search("catalog.json")
    if ret:
        return ret
    ret = search("catalogue.json")
    if ret:
        return ret
    ret = search("collection.json")
    if ret:
        return ret
    return None


def cwl_to_stac(
    cwl_arguments: Union[List[str], dict],
    env: EvalEnv,
    cwl_source: CwLSource,
    direct_s3_mode=False,
) -> str:
    if env and smart_bool(env.get("sync_job", "false")):
        msg = "CWL can only be used for batch jobs."
        if smart_bool(os.environ.get("OPENEO_LOCAL_DEBUGGING", "false")):
            # Running local batch jobs for debugging is hard. So allow debugging with sync jobs.
            _log.warning(msg)
        else:
            raise RuntimeError(msg)
    dry_run_tracer: DryRunDataTracer = env.get(ENV_DRY_RUN_TRACER)
    if dry_run_tracer:
        # TODO: use something else than `dry_run_tracer.load_stac`
        #       to avoid risk on conflict with "regular" load_stac code flows?
        return "dummy"

    ensure_kubernetes_config()

    _log.info(f"Loading CWL from {cwl_source=}")

    launcher = CalrissianJobLauncher.from_context(env)
    results = launcher.run_cwl_workflow(
        cwl_source=cwl_source,
        cwl_arguments=cwl_arguments,
    )

    # TODO: provide generic helper to log some info about the results
    for k, v in results.items():
        _log.info(f"result {k!r}: {v.generate_public_url()=} {v.generate_presigned_url()=}")

    stac_root = find_stac_root(set(results.keys()))

    if direct_s3_mode:
        collection_url = results[stac_root].s3_uri()
    else:
        collection_url = results[stac_root].generate_public_url()
    return collection_url
