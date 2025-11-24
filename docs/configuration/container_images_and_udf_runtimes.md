

# `container_images_and_udf_runtimes` configuration

## Background

The `run_udf` process in openEO allows users to run **UDFs**
(user-defined functions defined, for example, in Python)
on openEO data cubes.
It has `runtime` and `version` arguments to declare
the type/language of the UDF runtime (e.g. "Python")
and some specific version of that (e.g. "3.11").
This establishes some **predefined set of dependencies** the UDF can rely on,
as exposed by the `GET /udf_runtimes` endpoint.

In the openEO Geotrellis backend implementation,
these UDF runtimes are backed by specific, pre-built **container images**.
When launching an openEO batch job, it must be run in the best container image
that matches UDF runtime constraints.

The openEO Geotrellis backend also provides
the (non-standard) **job option "image-name"**
which allows to directly specify the container image to use for batch job execution
(overriding the automatic selection based on UDF runtime constraints).

## `container_images_and_udf_runtimes`

It should be clear that there is a tight coupling between
available container images and the UDF runtimes they support.
The `container_images_and_udf_runtimes` configuration field allows
to explicitly define this relation.

Its value should be a list of dictionaries,
where each dictionary defines a container image and its supported UDF runtimes.
(Also see `ContainerImageRecord` (and `_UdfRuntimeAndVersion`)
for the internal structure these entries map to.)
In particular, the following fields are supported:

- `image_name` (string, required):
  Full container image reference (including registry and tag)
- `image_aliases` (list of strings, optional, default: `[]`):
  Alternative/short/user-friendly names for the container image
- `preference` (integer, optional, default: `0`):
  Preference value (higher is better) for this container image
  to use as tie breaker when multiple images match given UDF runtime constraints.
- `udf_runtimes` (list of dictionaries, optional, default: `[]`):
  List of UDF runtimes supported by this container image.
  Part of this information is exposed at the `GET /udf_runtimes` endpoint.
  Each dictionary should contain the following fields:
  - `name` (string, required): UDF runtime name (e.g. "Python")
  - `version` (string, required): UDF runtime version (e.g. "3.11")
  - `preference` (integer, optional, default: `0`):
    Preference value (higher is better) for determining
    the default version of a UDF runtime (as exposed at `GET /udf_runtimes` endpoint)
- `udf_runtime_libraries` (dictionary/string, optional, default: `{}`):
  Mapping of available libraries with their version
  (also to be exposed at the `GET /udf_runtimes` endpoint).
  This mapping can be provided in multiple formats:
  a simple dictionary, a JSON-encoded dump (as string)
  or a path to a JSON file containing such a dump.


## Examples

A minimal configuration with a single container image
(but no aliases, nor defined UDF runtimes)
would look like:

```Python
config = GpsBackendConfig(
    ...,
    container_images_and_udf_runtimes=[
        {
            "image_ref": "docker.example/openeo-geotrellis:123",
        }
    ],
)
```

A more complete example with multiple container images,
aliases, UDF runtimes, and preferences:

```Python
config = GpsBackendConfig(
    ...,
    container_images_and_udf_runtimes=[
        {
            "image_ref": "docker.example/openeo-geotrellis-py38:123",
            "image_aliases": ["python38"],
            "preference": 38,
            "udf_runtimes": [
                {"name": "Python", "version": "3.8"},
            ],
            "udf_runtime_libraries": {"numpy": "1.22.4", "xarray": "0.16.2"},
        },
        {
            "image_ref": "docker.example/openeo-geotrellis-py311:123",
            "image_aliases": ["python311"],
            "preference": 311,
            "udf_runtimes": [
                {"name": "Python", "version": "3.11"},
                {"name": "Python", "version": "3", "preference": 100},
            ],
            "udf_runtime_libraries": {"numpy": "2.3.3", "xarray": "2024.7.0"},
        },
    ],
)
```
