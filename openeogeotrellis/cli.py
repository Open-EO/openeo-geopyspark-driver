"""

Command line interface for evaluating openEO process graphs
directly against the openEO GeoPyspark Driver implementation
without the intermediate flask webserver layer.

Handy during development for quick use case testing
without having to setup/restart a local web service.
Also allows CI/testing or health checks in more restricted environments.

Simple usage example:

    $ python openeogeotrellis/cli.py '{"add":{"process_id":"add","arguments":{"x":3,"y":5},"result":true}}'
    ....
    8

"""


import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Callable, Tuple

from openeo_driver.save_result import ImageCollectionResult
from openeo_driver.utils import EvalEnv
from openeo_driver.utils import read_json

_log = logging.getLogger(__name__)


def _ensure_geopyspark(print=print):
    """Make sure GeoPySpark knows where to find Spark (SPARK_HOME) and py4j"""
    try:
        import geopyspark
        print("Succeeded to import geopyspark automatically: {p!r}".format(p=geopyspark))
    except KeyError as e:
        # Geopyspark failed to detect Spark home and py4j, let's fix that.
        from pyspark import find_spark_home
        pyspark_home = Path(find_spark_home._find_spark_home())
        print("Failed to import geopyspark automatically. "
              "Will set up py4j path using Spark home: {h}".format(h=pyspark_home))
        py4j_zip = next((pyspark_home / 'python' / 'lib').glob('py4j-*-src.zip'))
        print("[conftest.py] py4j zip: {z!r}".format(z=py4j_zip))
        sys.path.append(str(py4j_zip))


def _setup_local_spark(
        print: Callable = print, verbosity=0,
        master_url="local[2]", app_name="openEO-GeoPySpark-Driver"
):
    print("Setting up local Spark")

    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable

    _ensure_geopyspark(print=print)
    from geopyspark import geopyspark_conf
    from pyspark import SparkContext

    conf = geopyspark_conf(master=master_url, appName=app_name)
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    # Only show spark progress bars for high verbosity levels
    conf.set('spark.ui.showConsoleProgress', verbosity >= 3)
    conf.set('spark.ui.enabled', True)
    # TODO: allow finetuning the config more?

    print("SparkContext.getOrCreate with {c!r}".format(c=conf.getAll()))
    context = SparkContext.getOrCreate(conf)

    return context


def handle_cli(argv=None) -> Tuple[dict, argparse.Namespace]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "process_graph", nargs="?", default=None,
        help="Process graph to evaluate. Can be given as path to JSON file or directly as JSON string."
             " If nothing is given, the process graph should be given on standard input."
    )
    parser.add_argument(
        "-o", "--output", default=None, help="Output file name."
    )
    parser.add_argument("--api-version", default="1.0.0", help="openEO API version to evaluate against.")

    args = parser.parse_args(argv)

    if args.process_graph is None:
        # Read process graph from standard input
        _log.info("Reading process graph from STDIN ...")
        process_graph = json.load(sys.stdin)
    elif args.process_graph.strip().startswith("{"):
        # Process graph is given directly as JSON blob
        process_graph = json.loads(args.process_graph)
    elif args.process_graph.lower().endswith(".json"):
        # Process graph is given as JSON file
        process_graph = read_json(args.process_graph)
    else:
        raise ValueError(args.process_graph)
    return process_graph, args


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    process_graph, args = handle_cli(argv)

    _setup_local_spark(print=_log.info)

    # TODO: find cleaner way (without env variables) to "inject" driver implementation
    os.environ["DRIVER_IMPLEMENTATION_PACKAGE"] = "openeogeotrellis"
    from openeo_driver.ProcessGraphDeserializer import evaluate

    env = EvalEnv({
        "version": args.api_version,
        "pyramid_levels": "highest",
        "user": None,  # TODO
        "require_bounds": True,
        "correlation_id": f"cli-pid{os.getpid()}"
    })
    result = evaluate(process_graph, env=env)

    if isinstance(result, ImageCollectionResult):
        filename = args.output or f"result.{result.format}"
        _log.info(f"Saving result to {filename!r}")
        result.save_result(filename)
        _log.info(f"Saved result to {filename!r}")
    elif isinstance(result, dict):
        # TODO: support storing JSON result to file
        print(result)
    else:
        # TODO: support more result types
        raise ValueError(result)


if __name__ == '__main__':
    main(sys.argv[1:])
