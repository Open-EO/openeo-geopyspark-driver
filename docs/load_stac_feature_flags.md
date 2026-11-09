
# Feature flags for `load_stac` and `load_collection`


The openEO process `load_stac` is a popular data loading process in openEO in general.
Moreover, it also powers most of `load_collection` usage under the hood in the Geotrellis backend implementation.

Ideally, `load_stac` and `load_collection` work out of the box for most use cases,
but sometimes it is necessary to tweak the behavior a bit through _feature flags_,
e.g. to circumvent problems with improperly formatted STAC resources,
or to enable certain experimental features that are not yet fully standardized in the openEO API.


## How to set feature flags

Feature flags can be set in a (non-standard) `featureflags` argument
of `load_stac` or `load_collection` nodes in the process graph.

For example in raw JSON representation:

```json
   "loadcollection1": {
      "process_id": "load_collection"
      "arguments": {
        "id": "SENTINEL2_L2A",
        "temporal_extent": ["2020-01-08", "2020-01-09"],
        ...
        "featureflags": {
          "tilesize": 32
        }
      },
    },
```

As this is a non-standard argument,
the openEO Python client does not support it directly in the `load_stac()` or `load_collection()` methods.
Instead, it has to be injected using a `.result_node().update_arguments()` trick
on the result of these methods.
For example, as follows:

```python
cube = connection.load_collection(
    "SENTINEL2_L2A",
    temporal_extent=["2020-01-08", "2020-01-09"],
    ...
)
cube.result_node().update_arguments(
    featureflags={
        "tilesize": 32,
    }
)
```


## Available feature flags

The following is a non-exhaustive, non-binding list of feature flags
that can be used in some use cases


### `allow_empty_cube`

### `granule_metadata_band_map`

Mainly intended for layer-catalog-based configuration.
Maps band names to `granule_metadata##N` angle bands of the Sentinel-2 `MTD_TL.xml` asset:
`N` from 0 to 3 are constant tile means, 4 to 7 per-pixel angles
(see [`layercatalog.md`](layercatalog.md)).
The map must list `granule_metadata##0` up to its highest index without gaps (and at most 10 entries),
because the index used is the position among the sorted values.

### `cellsize_override`

Mainly intended for layer-catalog-based configuration.

### `cellsize_fallback`

Mainly intended for layer-catalog-based configuration.

### `fix_proj_transform`

Mainly intended for layer-catalog-based configuration.

### `skipped_assets`

### `asset_id_to_bands_map`

Mainly intended for layer-catalog-based configuration.

### `preferred_url_prefix`

### `tilesize`

To set the RDD tile size.

### `retain_nodata_tiles`

Whether to retain tiles whose bands are all NoData when loading. Boolean value.
Default: False, which drops such tiles. This can be useful for pure Sentinel-2 angle requests
whose angle sources are unavailable and whose float32/NaN placeholder tiles would otherwise be filtered out.
This load flag is separate from the `save_result` format option with the same name.

### `property_filter_adaptations`

Mainly intended for layer-catalog-based configuration.


### `use-filter-extension`

### `stac_api_per_page_limit`

To fine-tune the number of items per page when querying a STAC API.
Integer value.
Default at time of this writing: 100.

### `stac_api_max_items`

The maximum number of items to retrieve in total from a STAC API
(to avoid spamming the STAC API with unbounded queries).
Integer value.
Default at time of this writing: 5000.

### `stac_api_filter_by_geometry`

Whether to send a more finegrained geometry than a simple bounding box
to the STAC API for spatial filtering.
Boolean value.
Default: True.

### `post_query_property_filtering`

Whether to do additional property filtering after retrieving the items from the STAC API.
Possible values:

- Boolean: True (default currently) to do additional propery filtering, or False to disable it
- a list of strings: to only filter on the given properties
- a dictionary with "allow" and/or "deny" properties (each a list of strings),
  to include or exclude certain properties from filtering

### `deduplicate_items`
Whether to deduplicate items retrieved from the STAC API.


### `use_raw_asset_href`

Whether to use the raw asset `href` field as-is,
without normalization or transformation provided by pystac.
Enabling of and depending on this feature flag should be avoided:
it is primarily intended for legacy use cases
with improperly normalized STAC asset hrefs.
Boolean value.
Default: False (i.e. use the normalized href by default).
