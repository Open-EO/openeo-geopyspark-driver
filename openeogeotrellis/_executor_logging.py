from openeo_driver.util.logging import setup_logging, get_logging_config, LOGGING_CONTEXT_BATCH_JOB

setup_logging(get_logging_config(
        root_handlers=["stderr_json"],
        loggers={
            "openeo": {"level": "DEBUG"},
            "openeo_driver": {"level": "DEBUG"},
            "openeogeotrellis": {"level": "DEBUG"},
            "kazoo": {"level": "WARN"},
            "cropsar": {"level": "DEBUG"},
        },
        context=LOGGING_CONTEXT_BATCH_JOB))