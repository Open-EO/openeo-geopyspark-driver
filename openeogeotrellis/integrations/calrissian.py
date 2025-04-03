from __future__ import annotations

import textwrap

import base64
import dataclasses
import logging
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import kubernetes.client
import time

import yaml

from openeo.util import ContextTimer
from openeo_driver.utils import generate_unique_id
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.util.runtime import get_job_id, get_request_id
from openeogeotrellis.utils import s3_client


try:
    # TODO #1060 importlib.resources on Python 3.8 is pretty limited so we need backport
    import importlib_resources
except ImportError:
    import importlib.resources

    importlib_resources = importlib.resources


_log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class VolumeInfo:
    name: str
    claim_name: str
    mount_path: str


@dataclasses.dataclass(frozen=True)
class CalrissianS3Result:
    s3_bucket: str
    s3_key: str

    def read(self, encoding: Union[None, str] = None) -> Union[bytes, str]:
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
        # TODO is there a better way than just chopping off the query string
        #      from a pre-signed URL? (related to eu-cdse/openeo-cdse-infra#479)
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

    def get_content(self) -> str:
        return self._cwl

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
        content = importlib_resources.files(anchor=anchor).joinpath(path).read_text(encoding="utf-8")
        return cls(content=content)


class CalrissianJobLauncher:
    """
    Helper class to launch a Calrissian job on Kubernetes.
    """

    def __init__(
        self,
        *,
        namespace: Optional[str] = None,
        name_base: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        backoff_limit: int = 1,
    ):
        self._namespace = namespace or get_backend_config().calrissian_namespace
        assert self._namespace
        self._name_base = name_base or generate_unique_id(prefix="cal")[:20]
        self._s3_bucket = s3_bucket or get_backend_config().calrissian_bucket

        _log.info(f"CalrissianJobLauncher.__init__: {self._namespace=} {self._name_base=} {self._s3_bucket=}")
        self._backoff_limit = backoff_limit

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

        # TODO #1129 config for this?
        self._security_context = kubernetes.client.V1SecurityContext(run_as_user=1000, run_as_group=1000)

    @staticmethod
    def from_context() -> CalrissianJobLauncher:
        """
        Factory for creating a CalrissianJobLauncher from the current context
        (e.g. using job_id/request_id for base name).
        """
        correlation_id = get_job_id(default=None) or get_request_id(default=None)
        name_base = correlation_id[:20] if correlation_id else None
        return CalrissianJobLauncher(name_base=name_base)

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
        yaml_parsed = list(yaml.safe_load_all(cwl_content))
        assert len(yaml_parsed) >= 1

        # Serialize CWL content to string that is safe to pass as command line argument
        cwl_serialized = base64.b64encode(cwl_content.encode("utf8")).decode("ascii")
        # TODO #1008 cleanup procedure of these CWL files?
        cwl_path = str(Path(self._volume_input.mount_path) / f"{name}.cwl")
        _log.info(
            f"create_input_staging_job_manifest creating {cwl_path=} from {cwl_content[:32]=} through {cwl_serialized[:32]=}"
        )

        container = kubernetes.client.V1Container(
            name="calrissian-input-staging",
            # TODO #1132 config to override this image or docker reg?
            image="registry.stag.warsaw.openeo.dataspace.copernicus.eu/rand/alpine:3",
            security_context=self._security_context,
            command=["/bin/sh"],
            args=["-c", f"set -euxo pipefail; echo '{cwl_serialized}' | base64 -d > {cwl_path}"],
            volume_mounts=[
                kubernetes.client.V1VolumeMount(
                    name=self._volume_input.name, mount_path=self._volume_input.mount_path, read_only=False
                )
            ],
        )
        manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
            ),
            spec=kubernetes.client.V1JobSpec(
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
                    )
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
    ) -> Tuple[kubernetes.client.V1Job, str]:
        """
        Create a k8s manifest for a Calrissian CWL job.

        :param cwl_path: path to the CWL file to run (inside the input staging volume),
            as produced by `create_input_staging_job_manifest`
        :param cwl_arguments:
        :return: Tuple of
            - k8s job manifest
            - relative output directory (inside the output volume)
        """
        name = self._build_unique_name(infix="cal-cwl")
        _log.info(f"Creating CWL job manifest: {name=}")

        container_image = get_backend_config().calrissian_image
        assert container_image

        # Pairs of (volume_info, read_only)
        volumes = [(self._volume_input, True), (self._volume_tmp, False), (self._volume_output, False)]

        # Ensure trailing "/" so that `tmp-outdir-prefix` is handled as a root directory.
        tmp_dir = self._volume_tmp.mount_path.rstrip("/") + "/"
        relative_output_dir = name
        output_dir = str(Path(self._volume_output.mount_path) / relative_output_dir)

        calrissian_arguments = [
            # TODO (still) need for this debug flag?
            "--debug",
            # TODO #1129 better RAM/CPU values than these arbitrary ones?
            "--max-ram",
            "2G",
            "--max-cores",
            "1",
            "--tmp-outdir-prefix",
            tmp_dir,
            "--outdir",
            output_dir,
            cwl_path,
        ] + cwl_arguments

        _log.info(f"create_cwl_job_manifest {calrissian_arguments=}")

        container_env_vars = [
            kubernetes.client.V1EnvVar(
                name="CALRISSIAN_POD_NAME",
                value_from=kubernetes.client.V1EnvVarSource(
                    field_ref=kubernetes.client.V1ObjectFieldSelector(field_path="metadata.name")
                ),
            )
        ]
        if env_vars:
            container_env_vars.extend(kubernetes.client.V1EnvVar(name=k, value=v) for k, v in env_vars.items())

        container = kubernetes.client.V1Container(
            name=name,
            image=container_image,
            security_context=self._security_context,
            command=["calrissian"],
            args=calrissian_arguments,
            volume_mounts=[
                kubernetes.client.V1VolumeMount(
                    name=volume_info.name, mount_path=volume_info.mount_path, read_only=read_only
                )
                for volume_info, read_only in volumes
            ],
            env=container_env_vars,
        )
        manifest = kubernetes.client.V1Job(
            metadata=kubernetes.client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
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
                        ],
                    )
                ),
                backoff_limit=self._backoff_limit,
            ),
        )
        return manifest, relative_output_dir

    def launch_job_and_wait(
        self,
        manifest: kubernetes.client.V1Job,
        *,
        sleep: float = 5,
        timeout: float = 900,
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
        with ContextTimer() as timer:
            while timer.elapsed() < timeout:
                job: kubernetes.client.V1Job = k8s_batch.read_namespaced_job(name=job_name, namespace=self._namespace)
                _log.info(f"CWL job poll loop: {job_name=} {timer.elapsed()=:.2f} {job.status.to_dict()=}")
                if job.status.conditions:
                    if any(c.type == "Failed" and c.status == "True" for c in job.status.conditions):
                        final_status = "failed"
                        break
                    elif any(c.type == "Complete" and c.status == "True" for c in job.status.conditions):
                        final_status = "complete"
                        break
                time.sleep(sleep)

        _log.info(f"CWL job poll loop done: {job_name=} {timer.elapsed()=:.2f} {final_status=}")
        if final_status == "complete":
            pass
        elif final_status is None:
            raise TimeoutError(f"CWL Job {job_name} did not finish within {timeout}s")
        elif final_status != "complete":
            raise RuntimeError(f"CWL Job {job_name} failed with {final_status=} after {timer.elapsed()=:.2f}s")
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
        cwl_arguments: List[str],
        output_paths: List[str],
        env_vars: Optional[Dict[str, str]] = None,
    ) -> Dict[str, CalrissianS3Result]:
        """
        Run a CWL workflow on Calrissian and return the output as a string.

        :param cwl_content: CWL content as a string.
        :param cwl_arguments: arguments to pass to the CWL workflow.
        :return: output of the CWL workflow as a string.
        """
        # Input staging
        input_staging_manifest, cwl_path = self.create_input_staging_job_manifest(cwl_source=cwl_source)
        input_staging_job = self.launch_job_and_wait(manifest=input_staging_manifest)

        # CWL job
        cwl_manifest, relative_output_dir = self.create_cwl_job_manifest(
            cwl_path=cwl_path,
            cwl_arguments=cwl_arguments,
            env_vars=env_vars,
        )
        cwl_job = self.launch_job_and_wait(manifest=cwl_manifest)

        # Collect results
        output_volume_name = self.get_output_volume_name()
        s3_bucket = self._s3_bucket
        results = {
            output_path: CalrissianS3Result(
                s3_bucket=s3_bucket,
                s3_key=f"{output_volume_name}/{relative_output_dir.strip('/')}/{output_path.strip('/')}",
            )
            for output_path in output_paths
        }
        return results
