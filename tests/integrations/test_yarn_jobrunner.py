import contextlib
import dataclasses
from typing import Optional, Union, List

import pytest
from unittest import mock
import subprocess
from openeogeotrellis.integrations.yarn_jobrunner import YARNBatchJobRunner
from ..data import get_test_data_file


@dataclasses.dataclass
class YarnSubmitMock:
    command: Union[List[str], None] = None
    kwargs: Optional[dict] = None
    env: Optional[dict] = None


@contextlib.contextmanager
def mock_yarn_submit_job():
    with mock.patch("subprocess.run") as run:
        stdout = get_test_data_file("spark-submit-stdout.txt").read_text()
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")
        result = YarnSubmitMock()
        yield result
        run.assert_called_once()
        result.command = run.call_args.args[0]
        result.kwargs = run.call_args.kwargs
        result.env = run.call_args.kwargs["env"]


@pytest.mark.usefixtures("mock_yarn_backend_config")
class TestYARNBatchJobRunner:
    JOB_INFO_MINIMAL = {
        "title": "Minimal 3+5",
        "process": {
            "process_graph": {
                "add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
            }
        },
    }

    def test_run_job_basic(self, tmp_path):
        runner = YARNBatchJobRunner()
        job_info = {
            **self.JOB_INFO_MINIMAL,
            "job_options": {"image_name": "python38"},
        }
        with mock_yarn_submit_job() as submit_mock:
            runner.run_job(job_info=job_info, job_id="j-123", job_work_dir=tmp_path, user_id="alice")
        assert "submit_batch_job_spark3.sh" in submit_mock.command[0]
        assert "j-123_user alice" in submit_mock.command[1]

    @pytest.mark.parametrize(
        ["image_name", "expected"],
        [
            ("python38", "docker.test/openeo-geopy38:3.5.8"),
            ("python311", "docker.test/openeo-geopy311:7.9.11"),
            ("docker.test/python:2.7", "docker.test/python:2.7"),
        ],
    )
    def test_run_job_option_image_name(self, tmp_path, image_name, expected):
        runner = YARNBatchJobRunner()
        job_info = {
            **self.JOB_INFO_MINIMAL,
            "job_options": {"image-name": image_name},
        }
        with mock_yarn_submit_job() as submit_mock:
            runner.run_job(job_info=job_info, job_id="j-123", job_work_dir=tmp_path, user_id="alice")

        assert submit_mock.env["YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"] == expected
