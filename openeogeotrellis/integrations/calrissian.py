from typing import Optional

from openeo_driver.utils import generate_unique_id
import dataclasses

from openeogeotrellis.config import get_backend_config


@dataclasses.dataclass(frozen=True)
class VolumeInfo:
    name: str
    claim_name: str
    mount_path: str
    read_only: Optional[bool] = None


def create_cwl_job_body(
    *,
    # TODO: don't define a default namespace here
    namespace: Optional[str] = None,
    name: Optional[str] = None,
) -> "kubernetes.client.V1Job":
    import kubernetes.client

    name = name or generate_unique_id(prefix="cj")
    namespace = namespace or get_backend_config().calrissian_namespace
    container_image = get_backend_config().calrissian_image
    if not namespace or not container_image:
        raise ValueError(f"Must be set: {namespace=}, {container_image=}")

    # TODO: config for this
    security_context = kubernetes.client.V1SecurityContext(run_as_user=1000, run_as_group=1000)

    volumes = [
        VolumeInfo(
            name="calrissian-input-data",
            claim_name="calrissian-input-data",
            mount_path="/calrissian/input-data",
            read_only=True,
        ),
        VolumeInfo(
            name="calrissian-tmpout",
            claim_name="calrissian-tmpout",
            mount_path="/calrissian/tmpout",
        ),
        VolumeInfo(
            name="calrissian-output-data",
            claim_name="calrissian-output-data",
            mount_path="/calrissian/output-data",
        ),
    ]

    calrissian_arguments = [
        "--max-ram",
        "2G",
        "--max-cores",
        "1",
        "--debug",
        "--tmp-outdir-prefix",
        "/calrissian/tmpout/",
        "--outdir",
        "/calrissian/output-data/",
        "/calrissian/input-data/hello-workflow.cwl",
        "/calrissian/input-data/hello-input.yaml",
    ]

    body = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name=name,
            namespace=namespace,
        ),
        spec=kubernetes.client.V1JobSpec(
            template=kubernetes.client.V1PodTemplateSpec(
                spec=kubernetes.client.V1PodSpec(
                    containers=[
                        kubernetes.client.V1Container(
                            name="calrissian",
                            image=container_image,
                            security_context=security_context,
                            command=["calrissian"],
                            args=calrissian_arguments,
                            volume_mounts=[
                                kubernetes.client.V1VolumeMount(
                                    name=v.name,
                                    mount_path=v.mount_path,
                                    read_only=v.read_only,
                                )
                                for v in volumes
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
                    ],
                    restart_policy="Never",
                    volumes=[
                        kubernetes.client.V1Volume(
                            name=v.name,
                            persistent_volume_claim=kubernetes.client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=v.claim_name,
                                read_only=v.read_only,
                            ),
                        )
                        for v in volumes
                    ],
                )
            )
        ),
    )
    return body
