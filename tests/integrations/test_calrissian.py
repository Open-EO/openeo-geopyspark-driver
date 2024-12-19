import dirty_equals
import kubernetes.client

from openeogeotrellis.integrations.calrissian import CalrissianJobLauncher, CalrissianS3Result


class TestCalrissianJobLauncher:

    def test_create_input_staging_job_manifest(self):
        launcher = CalrissianJobLauncher(namespace="calrissian-test", name_base="r-123")

        manifest, cwl_path = launcher.create_input_staging_job_manifest(cwl_content="class: Dummy")

        assert cwl_path == "/calrissian/input-data/r-123.cwl"

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        assert manifest_dict["metadata"] == dirty_equals.IsPartialDict(
            {
                "name": "r-123-cal-input",
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
                                "set -euxo pipefail; echo 'Y2xhc3M6IER1bW15' | base64 -d > /calrissian/input-data/r-123.cwl",
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

    def test_create_cwl_job_manifest(self):
        launcher = CalrissianJobLauncher(namespace="calrissian-test", name_base="r-123")

        manifest, output_dir = launcher.create_cwl_job_manifest(
            cwl_path="/calrissian/input-data/r-123.cwl", cwl_arguments=["--message", "Howdy Earth!"]
        )

        assert output_dir == "r-123-cal-cwl"

        assert isinstance(manifest, kubernetes.client.V1Job)
        manifest_dict = manifest.to_dict()

        assert manifest_dict["metadata"] == dirty_equals.IsPartialDict(
            {
                "name": "r-123-cal-cwl",
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
                            "name": "r-123-cal-cwl",
                            "command": ["calrissian"],
                            "args": dirty_equals.Contains(
                                "--tmp-outdir-prefix",
                                "/calrissian/tmpout/",
                                "--outdir",
                                "/calrissian/output-data/r-123-cal-cwl",
                                "/calrissian/input-data/r-123.cwl",
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
