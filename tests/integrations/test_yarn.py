from openeogeotrellis.integrations.yarn import yarn_state_to_openeo_job_status


def test_yarn_state_to_openeo_job_status():
    assert "queued" == yarn_state_to_openeo_job_status("foo", "bar")
    assert "queued" == yarn_state_to_openeo_job_status("NEW", "UNDEFINED")
    assert "queued" == yarn_state_to_openeo_job_status("SUBMITTED", "UNDEFINED")
    assert "queued" == yarn_state_to_openeo_job_status("ACCEPTED", "UNDEFINED")
    assert "running" == yarn_state_to_openeo_job_status("RUNNING", "UNDEFINED")
    assert "finished" == yarn_state_to_openeo_job_status("FINISHED", "SUCCEEDED")
    assert "error" == yarn_state_to_openeo_job_status("FAILED", "FAILED")
    assert "canceled" == yarn_state_to_openeo_job_status("KILLED", "KILLED")
