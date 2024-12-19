import dirty_equals
import flask
import pytest

from openeo_driver.util.logging import FlaskRequestCorrelationIdLogging
from openeogeotrellis.util.runtime import get_job_id, ENV_VAR_OPENEO_BATCH_JOB_ID, get_request_id


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


def test_get_request_id_no_flask():
    assert get_request_id() is None
    assert get_request_id(default=None) is None
    assert get_request_id(default="nope") == "nope"

    with pytest.raises(RuntimeError):
        get_request_id(default=RuntimeError)

    with pytest.raises(RuntimeError):
        get_request_id(default=RuntimeError("nope!"))


def test_get_request_id_in_flask():
    results = []

    app = flask.Flask(__name__)

    @app.before_request
    def before_request():
        FlaskRequestCorrelationIdLogging.before_request()

    @app.route("/hello")
    def hello():
        results.append(get_request_id())
        results.append(get_request_id(default="nope"))
        results.append(get_request_id(default=RuntimeError))
        return "Hello world"

    with app.test_client() as client:
        client.get("/hello")

    assert results == [
        dirty_equals.IsStr(regex="r-[0-9a-f]+"),
        dirty_equals.IsStr(regex="r-[0-9a-f]+"),
        dirty_equals.IsStr(regex="r-[0-9a-f]+"),
    ]
