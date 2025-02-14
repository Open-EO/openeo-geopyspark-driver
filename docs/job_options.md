# Tweak batch jobs with `job_options`

The VITO/Terrascope openEO back-end supports passing additional options at job creation time to tweak batch jobs:

```python
connection.create_job(process_graph, additional={
    "driver-memory": "8G",
    "driver-memoryOverhead": "2G"
})
```

```python
data_cube.create_job(job_options={
    "sentinel-hub": {
        "client-alias": "vito",
        "input": "sync"
    }
})
```

### Log level

To reduce:
* the amount of irrelevant logs for users to wade through,
* the amount of storage for these logs on disk,

the openEO API spec provides a `log_level` to set at job creation time,
for example with `create_job()` or `execute_batch()`:

```python
cube.create_job(
    log_level="warning",
)
```

(Note that, originally, this log level could also be set through a "logging-threshold" job option,
but that usage pattern is deprecated in favor of the standardized way mentioned above)

This log level effectively prevents log entries with a lower level (in this case: "debug" and "info") from being written to the logs.

It accepts the log levels as defined in the OpenEO API: "debug", "info", "warning" and "error" and defaults to "info".

Note: logs originating from ```openeo.udf.debug.inspect``` are unaffected by this threshold; regardless of their log
level, be it "debug", "info", "warning" or "error", they will always be written to the logs.

### Allow empty data cubes

Setting the "allow_empty_cubes" flag (defaults to: `false`) enables working with empty data cubes
where this otherwise would raise an error. It is applied to all collections within this job.

```python
job_options={
    "allow_empty_cubes": True
}
```
