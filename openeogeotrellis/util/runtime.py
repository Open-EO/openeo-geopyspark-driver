import os
from typing import Type, Union
import importlib.util

from openeo_driver.util.logging import FlaskRequestCorrelationIdLogging

ENV_VAR_OPENEO_BATCH_JOB_ID = "OPENEO_BATCH_JOB_ID"


def _is_exception_like(value) -> bool:
    """Is given value an exception or exception type (so that it can be raised)?"""
    return isinstance(value, Exception) or (isinstance(value, type) and issubclass(value, Exception))


def get_job_id(*, default: Union[None, str, Exception, Type[Exception]] = None) -> Union[str, None]:
    """
    Get job id from batch job context,
    or a default/exception if not in batch job context.
    """
    value = os.environ.get(ENV_VAR_OPENEO_BATCH_JOB_ID, default)
    if _is_exception_like(value):
        raise value
    return value


def in_batch_job_context() -> bool:
    return bool(get_job_id(default=None))


def get_request_id(*, default: Union[None, str, Exception, Type[Exception]] = None) -> Union[str, None]:
    """
    Get webapp request id from request context,
    or a default/exception if not in request context.
    """
    request_id = FlaskRequestCorrelationIdLogging.get_request_id(default=default)
    if _is_exception_like(request_id):
        raise request_id
    return request_id


def is_package_available(name: str) -> bool:
    # TODO: move this utility to openeo-python-driver or openeo-python-client
    return importlib.util.find_spec(name) is not None
