"""
Self-contained batch-job result-handling package.

Only depends on the standard library, third-party packages, `openeo` and
`openeo_driver`. Does not depend on the rest of `openeogeotrellis` or on
anything JVM-side (no `py4j`, `pyspark`, `geopyspark`), so it can be reused
and tested outside the Spark batch-job process. `tests/job_results/
test_package_boundary.py` enforces this.
"""
