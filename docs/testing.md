


# Tips

- At the moment, `conftest.py` spins up a local Spark instance (see `pytest_configure` hook)
  as some tests need a working Spark cluster.
  Setting this local Spark instance takes a bit of time,
  which can be annoying during development
  if you repeatedly are running some subset of tests that don't require Spark in any way.
  You can disable setting up Spark for these runs with this env var:

        OPENEO_TESTING_SETUP_SPARK=no
