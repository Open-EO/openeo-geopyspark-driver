# Logging: user-facing vs developer-only

Every log record carries an `audience` field (`"user"` or `"developer"`).
Python driver and Scala/JVM records end up in the same Elasticsearch data stream,
so Kibana shows everything, while `GET /jobs/{id}/logs` only shows what is meant for users.
See [#872](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/872).

## What users see

`GET /jobs/{job_id}/logs?audience=...` (non-standard `audience` query parameter):

| `audience`            | Returned records                                              |
|-----------------------|---------------------------------------------------------------|
| `user` (**default**)  | records with audience `user` **and** records without audience |
| `developer`           | all records, no audience filtering                            |
| anything else         | HTTP 400 `InvalidAudience`                                    |

Records without an `audience` field (e.g. logs written before this feature) stay visible to users.

## Default behavior

Use plain `logging`. A filter (`AudienceLoggingFilter`, installed by
`openeogeotrellis.util.audience_logging.get_logging_config`) stamps every record:

- **`user`** by default, at any level: a plain `_log.info(...)` is visible to users.
- **`developer`** for `DEBUG`, `INFO` and `WARNING` records of external dependencies
  (logger name not starting with `openeo`, e.g. `urllib3`, `py4j`), even if they are marked otherwise.
  Their `ERROR` records stay `user`.
- **`user`** for everything logged while running UDF code (see below), including such external records.

```python
import logging

_log = logging.getLogger(__name__)

_log.info("Requested extent contains no products")   # audience: user (default)
```

## Making a message developer-only

Use `DeveloperLogger`, or set the field explicitly:

```python
from openeogeotrellis.util.audience_logging import DeveloperLogger

dev_log = DeveloperLogger(logging.getLogger(__name__))
dev_log.info("Retrying STAC request after timeout")            # audience: developer
dev_log.debug("Partitioner index: %s", index)                  # audience: developer

# or, equivalently
_log.warning("Spark executor lost", extra={"audience": "developer"})
```

Since the default is `user`, log anything technical (retries, internal state, stack traces of
recoverable errors, Spark internals) as developer-only. `UserFacingLogger` and
`extra={"audience": "user"}` still work, and are useful to be explicit.

## UDF code

`run_udf_code` (and the vector cube / `apply_metadata` UDF paths) run inside
`user_facing_log_context()`: **all** logging from UDF code, including `DEBUG` from libraries,
gets `audience: user`. Wrap new code paths that execute user-supplied code the same way:

```python
from openeogeotrellis.util.audience_logging import user_facing_log_context

with user_facing_log_context():
    result = run_user_code()
```

Note: the filter must be attached to the handler; use `get_logging_config(...)`
rather than building the handlers yourself.

## Scala / JVM

In the JVM code (openeo-geotrellis-extensions) `info`/`warn`/`error` are `user` by default and
`debug`/`trace` are `developer`; `JsonLayout.DEVELOPER` / `JsonLayout.USER` markers override this.
See `docs/logging.md` in
[openeo-geotrellis-extensions](https://github.com/Open-EO/openeo-geotrellis-extensions).
