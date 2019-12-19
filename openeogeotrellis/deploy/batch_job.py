import json
import logging
import sys
from typing import Dict, List

from openeo import ImageCollection
from openeo.util import TimingLogger
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.save_result import ImageCollectionResult, JSONResult
from openeogeotrellis.utils import kerberos
from pyspark import SparkContext

logger = logging.getLogger('openeogeotrellis.deploy.batch_job')


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'r') as f:
        job_specification = json.load(f)

    return job_specification


def main(argv: List[str]) -> None:
    # TODO: make log level configurable?
    logging.basicConfig(level=logging.INFO)

    logger.info("argv: {a!r}".format(a=argv))
    if len(argv) < 3:
        print("usage: %s <job specification input file> <results output file> [api version]" % argv[0])
        exit(1)

    job_specification_file, output_file = argv[1], argv[2]
    api_version = argv[3] if len(argv) == 4 else None

    job_specification = _parse(job_specification_file)
    viewing_parameters = {'version': api_version} if api_version else None
    process_graph = job_specification['process_graph']

    sc = SparkContext.getOrCreate()

    try:
        kerberos()
        result = ProcessGraphDeserializer.evaluate(process_graph, viewing_parameters)
        logger.info("Evaluated process graph result of type {t}: {r!r}".format(t=type(result), r=result))

        if isinstance(result, ImageCollection):
            format_options = job_specification.get('output', {})
            result.download(output_file, bbox="", time="", **format_options)
            logger.info("wrote image collection to %s" % output_file)
        elif isinstance(result, ImageCollectionResult):
            result.imagecollection.download(output_file, bbox="", time="", format=result.format, **result.options)
            logger.info("wrote image collection to %s" % output_file)
        elif isinstance(result, JSONResult):
            with open(output_file, 'w') as f:
                json.dump(result.prepare_for_json(), f)
            logger.info("wrote JSON result to %s" % output_file)
        else:
            with open(output_file, 'w') as f:
                json.dump(result, f)
            logger.info("wrote JSON result to %s" % output_file)
    finally:
        sc.stop()


if __name__ == '__main__':
    with TimingLogger("batch_job.py main", logger=logger):
        main(sys.argv)
