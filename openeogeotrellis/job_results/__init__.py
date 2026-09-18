"""
Self-contained batch-job result-handling package.

Only depends on the standard library, third-party packages, `openeo` and
`openeo_driver`. Does not depend on the rest of `openeogeotrellis` or on
anything JVM-side (no `py4j`, `pyspark`, `geopyspark`).
See ``batch_job_decoupling/`` for the design behind this package.
"""
