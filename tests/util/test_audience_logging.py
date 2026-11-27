import io
import json
import logging
import logging.config

import pythonjsonlogger.jsonlogger

from openeogeotrellis.util.audience_logging import (
    AudienceLoggingFilter,
    DeveloperLogger,
    UserFacingLogger,
    get_logging_config,
    user_facing_log_context,
)


def test_audience_default_and_user_facing():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(pythonjsonlogger.jsonlogger.JsonFormatter("%(message)s"))
    handler.addFilter(AudienceLoggingFilter())
    logger = logging.getLogger("openeogeotrellis.test.audience")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)
    try:
        logger.info("default")
        DeveloperLogger(logger).info("dev")
        UserFacingLogger(logger).info("user")
        logger.info("explicit", extra={"audience": "developer"})
    finally:
        logger.removeHandler(handler)
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [(r["message"], r["audience"]) for r in records] == [
        ("default", "user"),
        ("dev", "developer"),
        ("user", "user"),
        ("explicit", "developer"),
    ]


def test_external_non_error_is_developer_audience():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(pythonjsonlogger.jsonlogger.JsonFormatter("%(message)s"))
    handler.addFilter(AudienceLoggingFilter())
    loggers = [logging.getLogger(n) for n in ["urllib3.connectionpool", "openeogeotrellis.foo"]]
    for logger in loggers:
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        logger.addHandler(handler)
    try:
        loggers[0].debug("ext debug", extra={"audience": "user"})
        loggers[0].info("ext info")
        loggers[0].warning("ext warning")
        loggers[0].error("ext error")
        with user_facing_log_context():
            loggers[0].debug("udf-context debug")
            loggers[0].warning("udf-context warning")
            loggers[1].debug("own debug")
        loggers[1].info("own info")
    finally:
        for logger in loggers:
            logger.removeHandler(handler)
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [(r["message"], r["audience"]) for r in records] == [
        ("ext debug", "developer"),
        ("ext info", "developer"),
        ("ext warning", "developer"),
        ("ext error", "user"),
        ("udf-context debug", "user"),
        ("udf-context warning", "user"),
        ("own debug", "user"),
        ("own info", "user"),
    ]


def test_get_logging_config_adds_filter_to_json_handlers():
    config = get_logging_config(root_handlers=["stderr_json"])
    assert "AudienceLoggingFilter" in config["handlers"]["stderr_json"]["filters"]
    assert "AudienceLoggingFilter" in config["filters"]
    logging.config.dictConfig(config)
