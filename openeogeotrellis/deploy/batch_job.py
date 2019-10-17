import json
import sys
from typing import Dict, List

from pyspark import SparkContext

from openeo import ImageCollection
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.save_result import ImageCollectionResult, JSONResult
from openeogeotrellis.utils import kerberos


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'r') as f:
        job_specification = json.load(f)

    return job_specification


def main(argv: List[str]) -> None:
    if len(argv) < 3:
        print("usage: %s <job specification input file> <results output file> [api version]" % argv[0])
        exit(1)

    job_specification_file, output_file = argv[1], argv[2]
    api_version = argv[3] if len(argv) == 4 else None

    job_specification = _parse(job_specification_file)
    viewing_parameters = {'version': api_version} if api_version else None

    sc = SparkContext.getOrCreate()

    try:
        kerberos()
        result = ProcessGraphDeserializer.evaluate(job_specification['process_graph'], viewing_parameters)

        if isinstance(result, ImageCollection):
            format_options = job_specification.get('output', {})
            result.download(output_file, bbox="", time="", **format_options)

            print("wrote image collection to %s" % output_file)
        elif isinstance(result, ImageCollectionResult):
            result.imagecollection.download(output_file, bbox="", time="", format=result.format, **result.options)

            print("wrote image collection to %s" % output_file)
        elif isinstance(result, JSONResult):
            with open(output_file, 'w') as f:
                json.dump(result.prepare_for_json(), f)
            print("wrote JSON result to %s" % output_file)
        else:
            with open(output_file, 'w') as f:
                json.dump(result, f)

            print("wrote JSON result to %s" % output_file)
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
