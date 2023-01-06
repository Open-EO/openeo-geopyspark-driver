from openeogeotrellis.integrations.kubernetes import k8s_job_name


def test_k8s_job_name():
    assert "job-123-abc" == k8s_job_name(job_id="j-123", user_id="abc")
    assert "job-1614a7e02c-e6c8213a985535af0654" == k8s_job_name(
        job_id="j-1614a7e02c49a641aee039e4cae740af52363822bf41ef07",
        user_id="e6c8213a985535af065403cbdf1fe13be267b16e1784@egi.eu",
    )
    assert "job-1614a7e02c-e6c8213a985535af0654" == k8s_job_name(
        job_id="1614a7e02c49a641aee039e4cae740af52363822bf41ef07",
        user_id="e6c8213a985535af065403cbdf1fe13be267b16e1784",
    )
