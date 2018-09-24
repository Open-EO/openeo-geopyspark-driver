import sys
import json
from pyspark import SparkContext
from openeogeotrellis import kerberos
from openeo import ImageCollection
from openeo_driver import ProcessGraphDeserializer
from typing import Dict, Any, List


def parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'r') as f:
        job_specification = json.load(f)

    return job_specification


def save(result: Any, output_file, format_options) -> None:
    if isinstance(result, ImageCollection):
        result.download(output_file, bbox="", time="", **format_options)
        print("wrote image collection to %s" % output_file)
    else:
        with open(output_file, 'w') as f:
            json.dump(result, f)

        print("wrote JSON result to %s" % output_file)


def main(argv: List[str]) -> None:
    if len(argv) < 3:
        print("usage: %s <job specification input file> <results output file>" % argv[0])
        exit(1)

    job_specification_file, output_file = argv[1], argv[2]

    job_specification = parse(job_specification_file)

    sc = SparkContext.getOrCreate()

    try:
        kerberos()
        result = ProcessGraphDeserializer.evaluate(processGraph=job_specification['process_graph'])
        save(result, output_file, job_specification['output'])
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
