"""Tag log records with an `audience` ("user" or "developer"), to allow retrieving user-facing logs only."""

import logging
from typing import Optional

from openeo_driver.util.logging import get_logging_config as _get_logging_config

AUDIENCE_USER = "user"
AUDIENCE_DEVELOPER = "developer"
AUDIENCE_ATTR = "audience"


class AudienceLoggingFilter(logging.Filter):
    """Stamp `audience: "developer"` on log records that don't carry an audience yet."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not getattr(record, AUDIENCE_ATTR, None):
            setattr(record, AUDIENCE_ATTR, AUDIENCE_DEVELOPER)
        return True


class UserFacingLogger(logging.LoggerAdapter):
    """
    Logger adapter marking records as user-facing:

        user_log = UserFacingLogger(logging.getLogger(__name__))
        user_log.info("Requested extent contains no products for collection X")
    """

    def __init__(self, logger: logging.Logger, extra: Optional[dict] = None):
        super().__init__(logger, extra or {})

    def process(self, msg, kwargs):
        kwargs["extra"] = {**self.extra, **(kwargs.get("extra") or {}), AUDIENCE_ATTR: AUDIENCE_USER}
        return msg, kwargs


def get_logging_config(**kwargs) -> dict:
    """`openeo_driver.util.logging.get_logging_config` with the audience filter added to all handlers with filters."""
    config = _get_logging_config(**kwargs)
    config.setdefault("filters", {})["AudienceLoggingFilter"] = {"()": AudienceLoggingFilter}
    for handler in config.get("handlers", {}).values():
        if "filters" in handler:
            handler["filters"] = [*handler["filters"], "AudienceLoggingFilter"]
    return config
