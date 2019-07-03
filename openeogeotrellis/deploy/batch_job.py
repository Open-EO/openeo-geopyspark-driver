import sys
import json
from pyspark import SparkContext
from openeogeotrellis import kerberos
from openeo import ImageCollection
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.save_result import *
from typing import Dict, List


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'r') as f:
        job_specification = json.load(f)

    return job_specification


def main(argv: List[str]) -> None:
    if len(argv) < 3:
        print("usage: %s <job specification input file> <results output file>" % argv[0])
        exit(1)

    job_specification_file, output_file = argv[1], argv[2]

    job_specification = _parse(job_specification_file)

    sc = SparkContext.getOrCreate()

    try:
        kerberos()
        result = ProcessGraphDeserializer.evaluate(processGraph=job_specification['process_graph'])

        if isinstance(result, ImageCollection):
            format_options = job_specification.get('output', {})
            result.download(output_file, bbox="", time="", **format_options)

            print("wrote image collection to %s" % output_file)
        elif isinstance(result, ImageCollectionResult):
            result.imagecollection.download(output_file, bbox="", time="", format=result.format, **result.options)

            print("wrote image collection to %s" % output_file)
        elif isinstance(result, JSONResult):
            with open(output_file, 'w') as f:
                json.dump(result.json_dict, f)

            print("wrote JSON result to %s" % output_file)
        else:
            with open(output_file, 'w') as f:
                json.dump(result, f)

            print("wrote JSON result to %s" % output_file)
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
