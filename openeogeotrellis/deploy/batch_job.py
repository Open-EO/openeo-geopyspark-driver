import sys
import json
from pyspark import SparkContext
from openeogeotrellis import kerberos
from openeo import ImageCollection
from openeo_driver import ProcessGraphDeserializer
from typing import Dict, Any, List


def parse(process_graph_file: str) -> Dict:
    with open(process_graph_file, 'r') as f:
        process_graph = json.load(f)

    return process_graph


def save(result: Any, output_file) -> None:
    if isinstance(result, ImageCollection):
        result.download(output_file, bbox="", time="")
        print("wrote image collection to %s" % output_file)
    else:
        with open(output_file, 'w') as f:
            json.dump(result, f)

        print("wrote JSON result to %s" % output_file)


def main(argv: List[str]) -> None:
    if len(argv) < 3:
        print("usage: %s <process graph input file> <results output file>" % argv[0])
        exit(1)

    process_graph_file, output_file = argv[1], argv[2]

    process_graph = parse(process_graph_file)

    sc = SparkContext.getOrCreate()

    try:
        kerberos()
        result = ProcessGraphDeserializer.evaluate(process_graph)
        save(result, output_file)
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
