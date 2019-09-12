import os
import sys
from pathlib import Path

import pytest
from _pytest.terminal import TerminalReporter


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    """Pytest configuration hook"""
    terminal_reporter = config.pluginmanager.get_plugin("terminalreporter")
    _ensure_geopyspark(terminal_reporter)
    _setup_local_spark(terminal_reporter, verbosity=config.getoption("verbose"))


def _ensure_geopyspark(out: TerminalReporter):
    """Make sure GeoPySpark knows where to find Spark (SPARK_HOME) and py4j"""
    try:
        import geopyspark
        out.write_line("Succeeded to import geopyspark automatically: {p!r}".format(p=geopyspark))
    except KeyError as e:
        # Geopyspark failed to detect Spark home and py4j, let's fix that.
        from pyspark import find_spark_home
        pyspark_home = Path(find_spark_home._find_spark_home())
        out.write_line("Failed to import geopyspark automatically. "
                       "Will set up py4j path using Spark home: {h}".format(h=pyspark_home))
        py4j_zip = next((pyspark_home / 'python' / 'lib').glob('py4j-*-src.zip'))
        sys.path.append(str(py4j_zip))


def _setup_local_spark(out: TerminalReporter, verbosity=0):
    # TODO make a "spark_context" fixture instead of doing this through pytest_configure
    out.write_line("Setting up local Spark")

    travis_mode = 'TRAVIS' in os.environ
    master_str = "local[2]" if travis_mode else "local[*]"

    from geopyspark import geopyspark_conf
    from pyspark import SparkContext

    conf = geopyspark_conf(master=master_str, appName="OpenEO-GeoPySpark-Driver-Tests")
    conf.set('spark.kryoserializer.buffer.max', value='1G')
    # Only show spark progress bars for high verbosity levels
    conf.set('spark.ui.showConsoleProgress', verbosity >= 3)

    if travis_mode:
        conf.set(key='spark.driver.memory', value='2G')
        conf.set(key='spark.executor.memory', value='2G')
        conf.set('spark.ui.enabled', False)
    else:
        conf.set('spark.ui.enabled', True)

    context = SparkContext.getOrCreate(conf)

    out.write_line("Validating the Spark context")
    dummy = context._jvm.org.openeo.geotrellis.OpenEOProcesses()
    answer = context.parallelize([9, 10, 11, 12]).sum()
    out.write_line(repr((answer, dummy)))

    return context
