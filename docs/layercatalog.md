# Layer Catalog Configuration


## Background

Data processing in openEO starts from loading EO data.
Originally, openEO just provided the `load_collection` process to load EO data sets,
called **collections** in the openEO API,
which are "predefined" by the openEO backend provider
(e.g. listed and documented under the `GET /collections` endpoint).
Later, the `load_stac` process was added, with the ambitious aim to load data
from any static STAC catalog or a STAC API Collection,
without the requirement that they have to be known in advance by the openEO backend provider.

Regardless of the value of `load_stac`, a backend probably still wants to expose
specific EO data sets as openEO collections,
to make them easily discoverable and straightforward to use.

## `layercatalog.json` Configuration

The openEO GeoPySpark driver allows to configure the available collections through
a "layer catalog" configuration file as follows:

- Create a JSON file, e.g. `layercatalog.json` to define the collections (see lower).
  If desired, it is also possible to work with multiple files, which will be merged automatically.

- Point to the layer configuration file(s), in one of the following ways:

  - Set the environment variable `OPENEO_CATALOG_FILES` to the path of this file
    (or comma separated sequence of paths to multiple files).
  - Set the `layer_catalog_files` config field
    in your `GpsBackendConfig` [configuration](./configuration.md) object
    to a list of (absolute) paths to the layer catalog files.

  By default, `layercatalog.json` in the current working directory is assumed,
  but it is recommended to use absolute paths to avoid any confusion.


### JSON structure

The basic structure of the `layercatalog.json` file is an array of
JSON objects, each representing a collection,
roughly following the STAC Collection schema:

```json
[
  {
    "stac_version": "1.0.0",
    "type": "Collection",
    "id": "EXAMPLE_STAC_CATALOG",
    "title": "example_stac_catalog.",
    ...
  },
  {
    "stac_version": "1.0.0",
    "type": "Collection",
    "id": "SENTINEL2_L2A",
    "title": "Sentinel2 (L2A)",
    ...
  },
  ...
]
```

### Data source type

The openEO GeoPySpark driver supports different types of data sources,
each with its own configuration options.
The data source type and its configuration options are specified under
nested fields "_vito" > "data_source" in the collection object, e.g.:

```json
[
  {
    "stac_version": "1.0.0",
    "type": "Collection",
    "id": "SENTINEL2_L2A",
    "_vito": {
      "data_source": {
        "type": "file-s2",
        "opensearch_collection_id": "S2",
        ...
```


## Minimal STAC based example

The easiest data source type is the "stac" type,
which just requires a URL pointing to a STAC catalog or collection.
For example, using the [example_stac_catalog](https://raw.githubusercontent.com/Open-EO/openeo-geopyspark-driver/refs/heads/master/docker/local_batch_job/example_stac_catalog/collection.json)
included in the `openeo-geopyspark-driver` repository:

```json
[
  {
    "id": "EXAMPLE_STAC_CATALOG",
    "experimental": true,
    "title": "example_stac_catalog.",
    "description": "Simple Sentinel-2 based stac catalog that is hosted on Github.",
    "license": "unknown",
    "summaries": {
      "eo:bands": [
        {
          "name": "B04",
          "common_name": null,
          "wavelength_um": null,
          "aliases": null,
          "gsd": null
        },
        {
          "name": "B03",
          "common_name": null,
          "wavelength_um": null,
          "aliases": null,
          "gsd": null
        },
        {
          "name": "B02",
          "common_name": null,
          "wavelength_um": null,
          "aliases": null,
          "gsd": null
        }
      ]
    },
    "_vito": {
      "data_source": {
        "type": "stac",
        "url": "https://raw.githubusercontent.com/Open-EO/openeo-geopyspark-driver/refs/heads/master/docker/local_batch_job/example_stac_catalog/collection.json"
      }
    }
  }
]
```



## More advanced data source types

TODO

- "file-s2",
- "file-s3",
- "file-s1-coherence"
- "file-agera5"
- "file-cgls2"
- "file-globspatialonly"
- "file-oscars"
- "file-probav"
- "sentinel-hub"
- "creodias-s1-backscatter"
