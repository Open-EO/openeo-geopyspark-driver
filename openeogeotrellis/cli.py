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

from openeo.util import TimingLogger
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
        master_url="local[2]", app_name="openEO-GeoPySpark-Driver",
        additional_jar_dirs=[]
):
    print("Setting up local Spark")

    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable

    _ensure_geopyspark(print=print)
    from geopyspark import geopyspark_conf
    from pyspark import SparkContext

    conf = geopyspark_conf(master=master_url, appName=app_name, additional_jar_dirs=additional_jar_dirs)
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
    parser.add_argument(
        "-e", "--edit", metavar="PATH=VALUE", action="append", default=[],
        help="Preprocess the process graph before executing it."
             " Specify as `path=value`, with `path` the period separated path in JSON tree"
             " and `value` the new/updated value (in JSON format)."
             " For example, to change the 'west' border of load_collection bbox:"
             " `--edit loadcollection.arguments.spatial_extent.west=3.3`"
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

    if len(process_graph) == 1 and set(process_graph.keys()) == {"process_graph"}:
        process_graph = process_graph["process_graph"]

    # Edit process graph in-place
    for path, value in (e.split("=", 1) for e in args.edit):
        steps = path.split(".")
        cursor = process_graph
        for step in steps[:-1]:
            if step not in cursor:
                cursor[step] = {}
            cursor = cursor[step]
        cursor[steps[-1]] = json.loads(value)

    return process_graph, args


def safe_repr(x, max_length=2000) -> str:
    s = repr(x)
    if len(s) > max_length:
        i = max(0, (max_length - 3) // 2)
        j = max(0, max_length - 3 - i)
        return s[:i] + "..." + s[len(s) - j:]
    return s


@TimingLogger(title="main", logger=_log)
def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    process_graph, args = handle_cli(argv)
    _log.info("Evaluating process graph: {pg}".format(pg=safe_repr(process_graph)))

    _setup_local_spark(print=_log.info)

    # Local imports to workaround the pyspark import issues.
    from openeo_driver.ProcessGraphDeserializer import evaluate
    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    env = EvalEnv({
        "version": args.api_version,
        "pyramid_levels": "highest",
        "user": None,  # TODO
        "require_bounds": True,
        "correlation_id": f"cli-pid{os.getpid()}",
        "backend_implementation": GeoPySparkBackendImplementation(use_zookeeper=False),
    })

    with TimingLogger(title="Evaluate process graph", logger=_log):
        result = evaluate(process_graph, env=env)

    if isinstance(result, ImageCollectionResult):
        filename = args.output or f"result.{result.format}"
        with TimingLogger(title=f"Saving result to {filename!r}", logger=_log):
            result.save_result(filename)
    elif isinstance(result, dict):
        # TODO: support storing JSON result to file
        print(result)
    else:
        # TODO: support more result types
        raise ValueError(result)


if __name__ == '__main__':
    main(sys.argv[1:])
