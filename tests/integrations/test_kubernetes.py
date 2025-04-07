from openeogeotrellis.integrations.kubernetes import (
    k8s_job_name,
    k8s_state_to_openeo_job_status,
)


def test_k8s_job_name():
    assert k8s_job_name().startswith("a-")
    assert len(k8s_job_name()) <= 35  # has a limit, previous implementation was 35 chars long
    assert k8s_job_name() != k8s_job_name()


def test_k8s_state_to_openeo_job_status():
    assert "queued" == k8s_state_to_openeo_job_status("foobar")
    assert "queued" == k8s_state_to_openeo_job_status("PENDING")
    assert "queued" == k8s_state_to_openeo_job_status("")
    assert "queued" == k8s_state_to_openeo_job_status("SUBMITTED")
    assert "running" == k8s_state_to_openeo_job_status("RUNNING")
    assert "finished" == k8s_state_to_openeo_job_status("COMPLETED")
    assert "error" == k8s_state_to_openeo_job_status("FAILED")
    assert "error" == k8s_state_to_openeo_job_status("FAILING")
    assert "running" == k8s_state_to_openeo_job_status("SUCCEEDING")
