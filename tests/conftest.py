import os
import sys
from pathlib import Path

import pytest
from _pytest.terminal import TerminalReporter
from .datacube_fixtures import imagecollection_with_two_bands_and_three_dates, imagecollection_with_two_bands_and_one_date
os.environ["DRIVER_IMPLEMENTATION_PACKAGE"] = "openeogeotrellis"


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    """Pytest configuration hook"""
    os.environ['PYTEST_CONFIGURE'] = (os.environ.get('PYTEST_CONFIGURE', '') + ':' + __file__).lstrip(':')
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
        out.write_line("py4j zip: {z!r}".format(z=py4j_zip))
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

    out.write_line("SparkContext.getOrCreate with {c!r}".format(c=conf.getAll()))
    context = SparkContext.getOrCreate(conf)
    out.write_line("JVM info: {d!r}".format(d={
        f: context._jvm.System.getProperty(f)
        for f in [
            "java.version", "java.vendor", "java.home",
            "java.class.version",
            # "java.class.path",
        ]
    }))

    out.write_line("Validating the Spark context")
    dummy = context._jvm.org.openeo.geotrellis.OpenEOProcesses()
    answer = context.parallelize([9, 10, 11, 12]).sum()
    out.write_line(repr((answer, dummy)))

    return context


@pytest.fixture(params=["0.4.0", "1.0.0"])
def api_version(request):
    return request.param

