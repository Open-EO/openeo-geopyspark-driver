
# UDF dependency handling



## Automatic Python UDF dependency handling

In 0.83.x, with [#237](https://github.com/Open-EO/openeo-geopyspark-driver/issues/237),
initial support was added for inline dependencies in Python UDFs
as discussed at https://open-eo.github.io/openeo-python-client/udf.html#standard-for-declaring-python-udf-dependencies,
for example:

```python
# /// script
# dependencies = [
#   "geojson",
#   "fancy-eo-library",
# ]
# ///
#
# This openEO UDF script implements ...
# based on the fancy-eo-library ... using geosjon data ...

import geojson
import fancyeo

def apply_datacube(cube: xarray.DataArray, context: dict) -> xarray.DataArray:
    ...
```

The approach here was to extract the dependencies,
install them in the job work directory from the batch job driver process,
and include that directory in the Python path of the executors
so that these libraries are available during UDF execution.


### ZIP based UDF handling

Installing the dependencies from the driver into the job work directory
didn't always work properly on some deployments (e.g. CDSE)
where the job work directory is backed by a FUSE-mounted S3 storage.
The full file tree of the dependencies can be pretty big (lot of small files),
which is not ideal in such contexts.

With [#845](https://github.com/Open-EO/openeo-geopyspark-driver/issues/845) (0.46.x)
an alternative approach was added:
- on the driver
  - install dependencies in a temporary directory
  - create a ZIP archive (a single large file) of the dependencies in the job work directory
- on the executors, when executing UDFs:
  - unzip the dependencies on-the-fly from the ZIP archive to a temporary directory
  - include that directory in the Python path

### Configuration

To support the different UDF dependency handling strategies,
a config option `udf_dependencies_install_mode` was added, with options:

- "disabled" to not do any automatic UDF dependency handling
- "direct" to install dependencies directly in the job work directory.
  This is currently the default, as it was the existing behavior,
  but it's likely that "disabled" will become the default in the future.
- "zip" to use the ZIP based UDF handling.
