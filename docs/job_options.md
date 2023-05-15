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

```python
data_cube.execute_batch(job_options={
    "logging-threshold": "info"
})
```

### Logging threshold

To reduce:
* the amount of irrelevant logs for users to wade through,
* the amount of storage for these logs on disk,

the logging threshold for a batch job can be set by means of the "logging-threshold" job_option, e.g.
```python
job_options={
    "logging-threshold": "warning"
}
```

This effectively prevents lower priority logs (in this case: "debug" and "info") from being written to the logs.

It accepts the log levels as defined in the OpenEO API: "debug", "info", "warning" and "error" and defaults to "info".

Note: logs originating from ```openeo.udf.debug.inspect``` are unaffected by this threshold; regardless of their log
level, be it "debug", "info", "warning" or "error", they will always be written to the logs.
