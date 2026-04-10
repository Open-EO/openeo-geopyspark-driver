
import pytest

from openeogeotrellis.integrations.yarn_jobrunner import YARNBatchJobRunner, _DEFAULT_MAX_RESULT_SIZE


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

    def test_run_job_basic(self, tmp_path, yarn_mocker):
        runner = YARNBatchJobRunner()
        job_info = {
            **self.JOB_INFO_MINIMAL,
            "job_options": {"image_name": "python38"},
        }
        with yarn_mocker.mock_yarn_submit_job() as yarn_submit_call:
            runner.run_job(job_info=job_info, job_id="j-123", job_work_dir=tmp_path, user_id="alice")

        assert "submit_batch_job_spark3.sh" in yarn_submit_call.command[0]
        assert "j-123_user alice" in yarn_submit_call.command[1]

    @pytest.mark.parametrize(
        ["image_name", "expected"],
        [
            ("python38", "docker.test/openeo-geopy38:3.5.8"),
            ("python311", "docker.test/openeo-geopy311:7.9.11"),
            ("docker.test/python:2.7", "docker.test/python:2.7"),
        ],
    )
    def test_run_job_option_image_name(self, tmp_path, yarn_mocker, image_name, expected):
        runner = YARNBatchJobRunner()
        job_info = {
            **self.JOB_INFO_MINIMAL,
            "job_options": {"image-name": image_name},
        }
        with yarn_mocker.mock_yarn_submit_job() as yarn_submit_call:
            runner.run_job(job_info=job_info, job_id="j-123", job_work_dir=tmp_path, user_id="alice")

        assert yarn_submit_call.env["YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"] == expected

    @pytest.mark.parametrize(
        ["driver_memory", "expected_max_result_size"],
        [
            # driver_memory > 5g: max_result_size should be set to driver_memory
            ("8G", "8G"),
            ("10G", "10G"),
            # driver_memory <= 5g: max_result_size should stay at the default 5g
            ("3G", _DEFAULT_MAX_RESULT_SIZE),
            ("5G", _DEFAULT_MAX_RESULT_SIZE),
        ],
    )
    def test_run_job_max_result_size_based_on_driver_memory(
        self, tmp_path, yarn_mocker, driver_memory, expected_max_result_size
    ):
        runner = YARNBatchJobRunner()
        job_info = {
            **self.JOB_INFO_MINIMAL,
            "job_options": {"driver-memory": driver_memory},
        }
        with yarn_mocker.mock_yarn_submit_job() as yarn_submit_call:
            runner.run_job(job_info=job_info, job_id="j-123", job_work_dir=tmp_path, user_id="alice")

        # max_result_size is the second-to-last argument in the args list (executor_stack_size is last)
        max_result_size_arg = yarn_submit_call.command[-2]
        assert max_result_size_arg == expected_max_result_size
