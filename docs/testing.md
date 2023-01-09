
# (Unit) testing

## Basics

The unit tests expect that environment variable `SPARK_HOME` is set,
which can easily be done from within your development virtual environment as follows:

    export SPARK_HOME=$(find_spark_home.py)
    pytest


## Selection and filtering

Run specific test or subset of test: use `-k` option, e.g. run all tests with "udp" in function name:

    pytest -k udp

**Tip:**
The general setup in `conftest.py` spins up a local Spark instance (see `pytest_configure` hook)
as some tests need a working Spark cluster.
Setting this local Spark instance takes a bit of time, which can be annoying during development
if you repeatedly are running some subset of tests that don't require Spark in any way.
You can disable setting up Spark for these runs with this env var:

    OPENEO_TESTING_SETUP_SPARK=no

(Don't forget to unset this when running the full test suite or tests that *do* require Spark)


## Various tips and tricks

To disable capturing of `print` and logging, use something like this:

        pytest --capture=no --log-cli-level=INFO
