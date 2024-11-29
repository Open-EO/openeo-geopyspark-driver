import dirty_equals
from openeogeotrellis.integrations.calrissian import create_cwl_job_body


def test_create_cwl_job_body():
    body = create_cwl_job_body(namespace="calrissian-test")

    assert body.to_dict() == {
        "api_version": None,
        "kind": None,
        "metadata": dirty_equals.IsPartialDict(
            {
                "name": dirty_equals.IsStr(regex="cj-2.*"),
                "namespace": "calrissian-test",
            }
        ),
        "spec": dirty_equals.IsPartialDict(
            {
                "template": dirty_equals.IsPartialDict(
                    {
                        "spec": dirty_equals.IsPartialDict(
                            {
                                "containers": [
                                    dirty_equals.IsPartialDict(
                                        {
                                            "args": [
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
                                            ],
                                            "command": ["calrissian"],
                                            "image": "ghcr.io/duke-gcb/calrissian/calrissian:0.17.1",
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
                                                        "read_only": None,
                                                    }
                                                ),
                                                dirty_equals.IsPartialDict(
                                                    {
                                                        "mount_path": "/calrissian/output-data",
                                                        "name": "calrissian-output-data",
                                                        "read_only": None,
                                                    }
                                                ),
                                            ],
                                            "working_dir": None,
                                        }
                                    )
                                ],
                                "volumes": [
                                    dirty_equals.IsPartialDict(
                                        {
                                            "name": "calrissian-input-data",
                                            "persistent_volume_claim": {
                                                "claim_name": "calrissian-input-data",
                                                "read_only": True,
                                            },
                                        }
                                    ),
                                    dirty_equals.IsPartialDict(
                                        {
                                            "name": "calrissian-tmpout",
                                            "persistent_volume_claim": {
                                                "claim_name": "calrissian-tmpout",
                                                "read_only": None,
                                            },
                                        }
                                    ),
                                    dirty_equals.IsPartialDict(
                                        {
                                            "name": "calrissian-output-data",
                                            "persistent_volume_claim": {
                                                "claim_name": "calrissian-output-data",
                                                "read_only": None,
                                            },
                                        }
                                    ),
                                ],
                            }
                        ),
                    }
                ),
            }
        ),
        "status": None,
    }
