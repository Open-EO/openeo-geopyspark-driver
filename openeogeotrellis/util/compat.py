import inspect
from typing import Callable


def function_supports_kwargs(function: Callable) -> bool:
    """Does function accept keyword arguments?"""
    # TODO: move this to openeo_driver.util.compat
    signature = inspect.signature(function)
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values())
