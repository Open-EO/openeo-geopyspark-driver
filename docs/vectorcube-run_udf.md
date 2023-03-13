

# Using `run_udf` on a vector cube data cube

The openEO process `run_udf` is typically used on raster data cubes
in the "callback" of a process like `reduce_dimension`, `apply_dimension`, ...
where the UDF operates on a slice of raster data,
provided in some kind of multidimensional array format (like numpy, Xarray, pandas, ...).

The VITO/Terrascope openEO back-end also adds _experimental_ support
to use `run_udf` directly on a vector cube, e.g. to filter,
transform, enrich, postprocess the vector data.
In the original implementation (which is still the default),
the back-end calls the UDF with the _whole_ vector data set as input.
This was fine as proof of concept, but did not scale well
for large vector cubes as there was no way to leverage parallelization.

## Parallelized `run_udf` on vector cube data

Under [Open-EO/openeo-geopyspark-driver#251](https://github.com/Open-EO/openeo-geopyspark-driver/issues/251),
the experimental `run_udf` support on vector cubes was further expanded
to allow parallelized execution of the UDF logic.

The user-provided UDF is expected to work at the level of single geometries
and must follow the UDF signatures described below.

In the examples below, we will assume to apply the `run_udf` process
on the result of the `aggregate_spatial` process.
For example, something like this:

```python
import openeo
import openeo.processes
...
cube = connection.load_collection(...)
aggregates = cube.aggregate_spatial(geometries, reducer="mean")

result = openeo.processes.run_udf(data=aggregates, udf=udf_code, runtime="Python")
```

The resulting dataframe structure (to be downloaded in JSON format)
is currently structured according to the
["split" mode of `pandas.DataFrame.to_dict`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_dict.html)
For example, when using synchronous execution/download:

```pycon
>>> data = result.execute()
>>> data
{
    "columns": ["feature_index", "mean(band_0)", "mean(band_1)", "mean(band_2)"],
    "data": [
        [0, 0.4, 0.8, 1.4],
        [1, 0.3, 0.6, 1.9],
        ...
```


### Simple "Pandas DataFrame" mode with `udf_apply_feature_dataframe`

This mode can be enabled by defining your UDF entry point function as `udf_apply_feature_dataframe`,
which will be given a pandas DataFrame,
containing the data of a single geometry/feature in your vector cube:

```python
import pandas as pd

def udf_apply_feature_dataframe(df: pd.DataFrame):
    # df contains data for a single geometry/feature
    # with time dimension as index, and band dimension as columns
    ...
```

Depending on your use case, you can return different values:

- return a scalar (reduce all data of a feature to a single value),
  for example (extremely simplified example):

  ```python
  def udf_apply_feature_dataframe(df: pd.DataFrame) -> float:
    return 123.456
  ```

  The resulting output data structure will list the returned
  scalar value for each geometry/feature, e.g.:

  ```json
  {
    "columns": ["feature_index", "0"],
    "data": [
      [0, 123.456],
      [1, 123.456],
      ...
    ],
    ...
  }
  ```

- return a pandas Series to:
  - reduce the time dimension:

    ```python
    def udf_apply_feature_dataframe(df: pd.DataFrame) -> pd.Series:
        # Sum along index (time dimension)
        return df.sum(axis=0)
    ```
  - reduce the band dimension (make sure to convert the time index labels to strings):

    ```python
    def udf_apply_feature_dataframe(df: pd.DataFrame) -> pd.Series:
        # Sum along columns (band dimension)
        series = df.sum(axis=0)
        # Make sure index labels are strings
        series.index = series.index.strftime("%Y-%m-%d")
        return series
    ```

  The resulting output data structure will list the calculated values per geometry
  as follows:

  ```json
  {
    "columns": ["feature_index", "mean(band_0)", "mean(band_1)", "mean(band_2)"],
    "data": [
      [0, 0.4, 0.8, 1.4],
      [1, 1.3, 0.3, 2.3],
      ...
    ],
    ...
  }
  ```

- return a full pandas DataFrame, for example (very simplified example)

  ```python
  def udf_apply_feature_dataframe(df: pd.DataFrame) -> pd.DataFrame:
      return df + 1000
  ```

  The resulting output data structure will encode the preserved time dimension
  and band dimension as follows:

  ```json
  {
    "columns": ["feature_index", "date", "mean(band_0)", "mean(band_1)", "mean(band_2)"],
    "data": [
      [0, "2021-01-05T00:00:00.000Z", 1000.4, 1000.8, 1001.4],
      [0, "2021-02-12T00:00:00.000Z", 1000.8, 1002.8, 1000.9],
      [1, "2021-01-05T00:00:00.000Z", 1001.8, 1000.3, 1002.7],
      [1, "2021-01-12T00:00:00.000Z", 1000.3, 1000.9, 1001.6],
      ...
    ],
    ...
  }
  ```


### Classic `UdfData` mode with `udf_apply_udf_data`

This mode is more cumbersome to work with,
because there is more unpacking and packing boilerplate code
necessary.

See the unit tests for this mode for more information:

https://github.com/Open-EO/openeo-geopyspark-driver/blob/84c9c349159916037ea46c7982fe98143d8c40ee/tests/test_api_result.py#L1907-L2092
