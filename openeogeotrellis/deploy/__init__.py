import sys
import importlib
import logging

_log = logging.getLogger(__name__)


def load_custom_processes(logger=_log, _name="custom_processes"):
    """Try loading optional `custom_processes` module"""
    try:
        logger.info("Trying to load {n!r} with PYTHONPATH {p!r}".format(n=_name, p=sys.path))
        custom_processes = importlib.import_module(_name)
        logger.info("Loaded {n!r}: {p!r}".format(n=_name, p=custom_processes.__file__))
        return custom_processes
    except ImportError as e:
        logger.info('{n!r} not loaded: {e!r}.'.format(n=_name, e=e))
