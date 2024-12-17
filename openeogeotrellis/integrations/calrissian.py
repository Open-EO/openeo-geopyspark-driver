import base64
import gzip
import logging
import dataclasses
import textwrap
from pathlib import Path

import time
from typing import Optional, Union, Tuple, List

from openeo.util import ContextTimer
from openeo_driver.utils import generate_unique_id
from openeogeotrellis.config import get_backend_config

import kubernetes.client

_log = logging.getLogger(__name__)

@dataclasses.dataclass(frozen=True)
class VolumeInfo:
    name: str
    claim_name: str
    mount_path: str



class CalrissianJobLauncher:
    """
    Helper class to launch a Calrissian job on Kubernetes.
    """

    def __init__(
        self,
        *,
        namespace: Optional[str] = None,
        name_base: Optional[str] = None,
        backoff_limit: int = 1,
    ):
        self._namespace = namespace or get_backend_config().calrissian_namespace
        assert self._namespace
        self._name_base = name_base or generate_unique_id(prefix="cal")
        _log.info(f"CalrissianJobLauncher {self._namespace=} {self._name_base=}")
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

        # TODO: config for this?
        self._security_context = kubernetes.client.V1SecurityContext(run_as_user=1000, run_as_group=1000)

    def create_input_staging_job_manifest(self, cwl_content: str) -> Tuple[kubernetes.client.V1Job, str]:
        """
        Create a k8s manifest for a Calrissian input staging job.
        """
        name = f"{self._name_base}-cal-input"
        _log.info("Creating input staging job manifest: {name=}")

        # Serialize CWL content to string that is safe to pass as command line argument
        cwl_serialized = base64.b64encode(cwl_content.encode("utf8")).decode("ascii")
        # TODO: ensure isolation of different CWLs (e.g. req/job id is not enough). Include content hash
        # TODO: cleanup procedure of these CWL files?
        cwl_path = str(Path(self._volume_input.mount_path) / f"{self._name_base}.cwl")
        _log.info(
            f"create_input_staging_job_manifest creating {cwl_path=} from {cwl_content[:32]=} through {cwl_serialized[:32]=}"
        )

        container = kubernetes.client.V1Container(
            name="calrissian-input-staging",
            image="alpine:3",
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
                                    claim_name=self._volume_input.claim_name, read_only=False
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
        # TODO: arguments to set an actual CWL workflow and inputs
    ) -> kubernetes.client.V1Job:
        # TODO: name must be unique per invocation of this method, not just per instance/request/job.
        name = f"{self._name_base}-cal-cwl"
        _log.info(f"Creating CWL job manifest: {name=}")

        container_image = get_backend_config().calrissian_image
        assert container_image

        # Pairs of (volume_info, read_only)
        volumes = [(self._volume_input, True), (self._volume_tmp, False), (self._volume_output, False)]

        # Ensure trailing "/" so that `tmp-outdir-prefix` is handled as a root directory.
        tmp_dir = self._volume_tmp.mount_path.rstrip("/") + "/"
        output_dir = str(Path(self._volume_output.mount_path) / self._name_base)

        calrissian_arguments = [
            "--debug",
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

        container = kubernetes.client.V1Container(
            name="calrissian-job",
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
            env=[
                kubernetes.client.V1EnvVar(
                    name="CALRISSIAN_POD_NAME",
                    value_from=kubernetes.client.V1EnvVarSource(
                        field_ref=kubernetes.client.V1ObjectFieldSelector(field_path="metadata.name")
                    ),
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
        return manifest

    def launch_job_and_wait(
        self,
        manifest: kubernetes.client.V1Job,
        *,
        sleep: float = 5,
        timeout: float = 60,
    ) -> kubernetes.client.V1Job:
        """Launch a k8s job and wait (with active polling) for it to finish."""

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

        # Track job status (active polling).
        final_status = None
        with ContextTimer() as timer:
            while timer.elapsed() < timeout:
                job: kubernetes.client.V1Job = k8s_batch.read_namespaced_job(name=job_name, namespace=self._namespace)
                _log.info(f"CWL job {job_name=} {timer.elapsed()=:.2f} {job.status=}")
                if job.status.conditions:
                    if any(c.type == "Failed" and c.status == "True" for c in job.status.conditions):
                        final_status = "failed"
                        break
                    elif any(c.type == "Complete" and c.status == "True" for c in job.status.conditions):
                        final_status = "complete"
                        break
                time.sleep(sleep)

        _log.info(f"CWL job {job_name=} {timer.elapsed()=:.2f} {final_status=}")
        if final_status == "complete":
            pass
        elif final_status is None:
            raise TimeoutError(f"CWL Job {job_name} did not finish within {timeout}s")
        elif final_status != "complete":
            raise RuntimeError(f"CWL Job {job_name} failed with {final_status=} after {timer.elapsed()=:.2f}s")
        else:
            raise ValueError("CWL")

        # TODO: how to resolve and extract the results?
        # TODO: automatic cleanup after success?
        # TODO: automatic cleanup after fail?

        return job

    def get_volume_name_from_pvc(self, pvc_name: str) -> str:
        """Get the name of the volume that is mounted by a PVC."""
        core_api = kubernetes.client.CoreV1Api()
        pvc = core_api.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=self._namespace)
        volume_name = pvc.spec.volume_name
        return volume_name
