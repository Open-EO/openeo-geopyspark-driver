import pytest

from openeogeotrellis.util.runtime import get_job_id, ENV_VAR_OPENEO_BATCH_JOB_ID


def test_get_job_id_basic():
    assert get_job_id() is None


def test_get_job_id_batch_job_context(monkeypatch):
    monkeypatch.setenv(ENV_VAR_OPENEO_BATCH_JOB_ID, "j-123")
    assert get_job_id() == "j-123"


def test_get_job_id_with_default(monkeypatch):
    assert get_job_id(default="unknown") == "unknown"

    monkeypatch.setenv(ENV_VAR_OPENEO_BATCH_JOB_ID, "j-123")
    assert get_job_id() == "j-123"


@pytest.mark.parametrize("exception", [RuntimeError, RuntimeError("Nope!")])
def test_get_job_id_with_exception(monkeypatch, exception):
    with pytest.raises(RuntimeError):
        get_job_id(default=exception)

    monkeypatch.setenv(ENV_VAR_OPENEO_BATCH_JOB_ID, "j-123")
    assert get_job_id(default=exception) == "j-123"
