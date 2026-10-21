import io
import json
import logging

import pythonjsonlogger.jsonlogger

from openeogeotrellis.util.audience_logging import AudienceLoggingFilter, UserFacingLogger, get_logging_config


def test_audience_default_and_user_facing():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(pythonjsonlogger.jsonlogger.JsonFormatter("%(message)s"))
    handler.addFilter(AudienceLoggingFilter())
    logger = logging.getLogger("test.audience")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)
    try:
        logger.info("dev")
        UserFacingLogger(logger).info("user")
        logger.info("explicit", extra={"audience": "user"})
    finally:
        logger.removeHandler(handler)
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [(r["message"], r["audience"]) for r in records] == [
        ("dev", "developer"),
        ("user", "user"),
        ("explicit", "user"),
    ]


def test_get_logging_config_adds_filter_to_json_handlers():
    config = get_logging_config(root_handlers=["stderr_json"])
    assert "AudienceLoggingFilter" in config["handlers"]["stderr_json"]["filters"]
    assert "AudienceLoggingFilter" in config["filters"]
    logging.config.dictConfig(config)
