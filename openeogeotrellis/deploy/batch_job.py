import json
import logging
import os
import shutil
import stat
import sys
from pathlib import Path
from typing import Dict, List

from openeo import ImageCollection
from openeo.util import TimingLogger, ensure_dir
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.save_result import ImageCollectionResult, JSONResult, MultipleFilesResult
from pyspark import SparkContext

from openeogeotrellis.deploy import load_custom_processes
from openeogeotrellis.utils import kerberos, describe_path

LOG_FORMAT = '%(asctime)s:P%(process)s:%(levelname)s:%(name)s:%(message)s'

logger = logging.getLogger('openeogeotrellis.deploy.batch_job')
user_facing_logger = logging.getLogger('openeo-user-log')


def _setup_app_logging() -> None:
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)


def _setup_user_logging(log_file: str) -> None:
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.ERROR)

    user_facing_logger.addHandler(file_handler)

    _add_permissions(log_file, stat.S_IWGRP)


def _create_job_dir(job_dir: Path):
    logger.info("creating job dir {j!r} (parent dir: {p}))".format(j=job_dir, p=describe_path(job_dir.parent)))
    ensure_dir(job_dir)
    shutil.chown(job_dir, user=None, group='eodata')

    _add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group


def _add_permissions(path, mode: int):
    # TODO: maybe umask is a better/cleaner option
    current_permission_bits = os.stat(path).st_mode
    os.chmod(path, current_permission_bits | mode)


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'rt', encoding='utf-8') as f:
        job_specification = json.load(f)

    return job_specification


def main(argv: List[str]) -> None:
    logger.info("argv: {a!r}".format(a=argv))
    logger.info("pid {p}; ppid {pp}; cwd {c}".format(p=os.getpid(), pp=os.getppid(), c=os.getcwd()))

    if len(argv) < 4:
        print("usage: %s <job specification input file> <results output file> <user log file> [api version]" % argv[0],
              file=sys.stderr)
        exit(1)

    job_specification_file, output_file, log_file = argv[1], argv[2], argv[3]
    api_version = argv[4] if len(argv) == 5 else None

    _create_job_dir(Path(output_file).parent)

    _setup_user_logging(log_file)

    # Override default temp dir (under CWD). Original default temp dir `/tmp` might be cleaned up unexpectedly.
    temp_dir = Path(os.getcwd()) / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Using temp dir {t}".format(t=temp_dir))
    os.environ["TMPDIR"] = str(temp_dir)

    try:
        job_specification = _parse(job_specification_file)
        viewing_parameters = {'pyramid_levels': 'highest'}
        if api_version:
            viewing_parameters['version']= api_version

        process_graph = job_specification['process_graph']

        load_custom_processes(logger)

        with SparkContext.getOrCreate():
            kerberos()
            result = ProcessGraphDeserializer.evaluate(process_graph, viewing_parameters)
            logger.info("Evaluated process graph result of type {t}: {r!r}".format(t=type(result), r=result))

            if isinstance(result, DelayedVector):
                from shapely.geometry import mapping
                geojsons = (mapping(geometry) for geometry in result.geometries)
                result = JSONResult(geojsons)

            if isinstance(result, ImageCollection):
                format_options = job_specification.get('output', {})
                result.download(output_file, bbox="", time="", **format_options)
                _add_permissions(output_file, stat.S_IWGRP)
                logger.info("wrote image collection to %s" % output_file)
            elif isinstance(result, ImageCollectionResult):
                result.imagecollection.download(output_file, bbox="", time="", format=result.format, **result.options)
                _add_permissions(output_file, stat.S_IWGRP)
                logger.info("wrote image collection to %s" % output_file)
            elif isinstance(result, JSONResult):
                with open(output_file, 'w') as f:
                    json.dump(result.prepare_for_json(), f)
                _add_permissions(output_file, stat.S_IWGRP)
                logger.info("wrote JSON result to %s" % output_file)
            elif isinstance(result, MultipleFilesResult):
                result.reduce(output_file, delete_originals=True)
                _add_permissions(output_file, stat.S_IWGRP)
                logger.info("reduced %d files to %s" % (len(result.files), output_file))
            else:
                with open(output_file, 'w') as f:
                    json.dump(result, f)
                _add_permissions(output_file, stat.S_IWGRP)
                logger.info("wrote JSON result to %s" % output_file)
    except Exception as e:
        logger.exception("error processing batch job")
        user_facing_logger.exception("error processing batch job")
        raise e


if __name__ == '__main__':
    _setup_app_logging()

    with TimingLogger("batch_job.py main", logger=logger):
        main(sys.argv)
