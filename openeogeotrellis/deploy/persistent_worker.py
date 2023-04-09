import os
import sys
import time
import threading
import json
from pathlib import Path

from openeo_driver.util.logging import (get_logging_config, setup_logging, LOGGING_CONTEXT_BATCH_JOB, )
from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.deploy.batch_job import main, logger

OPENEO_LOGGING_THRESHOLD = os.environ.get("OPENEO_LOGGING_THRESHOLD", "INFO")


if __name__ == '__main__':
    setup_logging(get_logging_config(root_handlers = ["stderr_json" if ConfigParams().is_kube_deploy else "file_json"],
        context = LOGGING_CONTEXT_BATCH_JOB, root_level = OPENEO_LOGGING_THRESHOLD), capture_unhandled_exceptions = False,
    )

    config_params = ConfigParams()
    base_dir = config_params.persistent_worker_dir
    shutdown_file = base_dir / "shutdown"

    def check_exit_file():
        while True:
            if os.path.exists(shutdown_file):
                logger.info("Found shutdown file, shutting down")
                sys.exit(0)
            time.sleep(60)
    threading.Thread(target=check_exit_file, daemon=True).start()

    while True:
        # Search for a job_<UUID>.json without a corresponding job_<UUID>.lock file.
        for job_file in base_dir.glob("job_*.json"):
            lock_file = job_file.with_suffix(".lock")
            if not os.path.exists(lock_file):
                logger.info(f"Found job file {job_file}, starting job")
                # Create lock file to prevent other workers from picking up the same job.
                with open(lock_file, "w") as f:
                    f.write("locked")
                # Run the job
                try:
                    with open(job_file, "r") as f:
                        arguments = json.load(f)
                    main(arguments)
                except Exception as e:
                    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(e)
                    logger.exception("OpenEO persistent worker failed: " + error_summary.summary)
                finally:
                    os.remove(lock_file)
        # Wait before checking again.
        time.sleep(10)
