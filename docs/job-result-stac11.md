

# openEO batch job result metadata: "STAC 1.0" vs "STAC 1.1" style

This is a technical document describing the differences between
the legacy "STAC 1.0" style and the new "STAC 1.1" style of openEO batch job result metadata.

To sketch the historic context and timeline of openEO batch job result metadata a bit:

- openEO batch job results were originally (up to openEO API 1.0.0) described as a **STAC Item**.
- openEO API 1.1.0 (May 2021) introduced the possibility to describe batch job results
  as a **STAC Collection**, which is more flexible and appropriate for openEO workflows.
- STAC 1.1.0 (September 2024) introduced various changes
  that are relevant for openEO batch job result metadata.

Unconditionally adopting these STAC 1.1 changes however
comes with a **risk on compatibility issues** for existing openEO workflows.
Still, the openeo-geopyspark-driver implementation aims to unlock these improvements
with a gradual, **phased rollout** approach:

- _phase 1_: **default to legacy STAC 1.0 style** metadata
  and allow **opt-in** to use the new STAC 1.1 style metadata for new batch jobs via a job option
- _phase 2_: **default to new STAC 1.1 style** metadata
  and allow **opt-out** (again with a job option) to still get the legacy STAC 1.0 style metadata
- _phase 3_: **drop** the possibility to get **STAC 1.0** style metadata

Note: the exact timing of these phases is out of scope of this technical document
and will be communicated in more detail through the appropriate communication and news channels
of specific openeo-geopyspark-driver deployments (CDSE, VITO Terrascope, ...).


## Job option

The job option "stac-version" can be used to select the desired style:

- value `"1.0"` for legacy STAC 1.0 style metadata
- value `"1.1"` for new STAC 1.1 style metadata

For example, with the openEO Python client:

```python
job = connection.create_job(
    ...,
    job_options = {
        "stac-version": "1.1",
    }
)
```


## Technical overview of metadata changes

### Scope

The metadata changes apply to the following resources:

- the root document (STAC Collection) with  openEO batch job result metadata document,
  e.g. as provided by the endpoint `GET /jobs/{job_id}/result`
- child STAC Item metadata documents,
  e.g. as linked from the STAC Collection document (links with relation type "item")



### `stac_version` property

Trivial, but useful to detect the style of the metadata document:
the `stac_version` property shows the STAC style used:

- STAC 1.0 style:
  ```json
  "stac_version": "1.0.0",
  ```

- STAC 1.1 style:
  ```json
  "stac_version": "1.1.0",
  ```

### Collection-level assets

The root STAC Collection document lists batch result assets in the `assets` property.
The keys used in the `assets` property have changed in STAC 1.1 style metadata to ensure
unicity and avoid loss of assets due to key collisions.

The snippets below showing both the usage
of netCDF (where multiple time steps can be sored in a single file)
and GTiff assets (where each time step results in separate GTiff file):

- STAC 1.0 style:
  ```json
  {
    "assets": {
      "openEO.nc": {...},
      "openEO_2025-09-01Z.tif": {...},
      "openEO_2025-09-11Z.tif": {...},
  ```
- STAC 1.1 style:
  ```json
  {
    "assets": {
        "15f40353-02ea-417f-ae40-86a86cc7da31_openEO": {...},
        "5a5b12b6-08f1-4eb6-b226-7d548a5a3231_2025-09-21T00:00:00Z_openEO": {...},
        "a9cdc994-2ac7-4cc8-a0ac-16da13250617_2025-09-11T00:00:00Z_openEO": {...},
  ```

Note that the STAC Item id is prepended to the asset key in STAC 1.1 style metadata,
to eliminate the key collision risk.


### Band metadata

One of the STAC 1.1 improvements that are highly relevant for openEO
is the unification/centralization of band metadata
(single `bands` property, instead of `eo:bands`, `raster:bands`, ...).
This is reflected in both the batch job result STAC Collection document
and the child STAC Item documents.

For example in the `assets` property of the STAC Collection document:

- STAC 1.0 style:
  ```json
  {
    "assets": {
      "openEO.nc": {
        "eo:bands": [
          {"name": "ndvi"}
        ],
        "raster:bands": [
          {
            "name": "ndvi",
            "statistics": {"maximum": 0.9, "mean": 0.5, "minimum": 0.1, ...}
          }
        ],
  ```
- STAC 1.1 style:
  ```json
  {
    "assets": {
      "15f40353-02ea-417f-ae40-86a86cc7da31_openEO": {
        "bands": [
          {
            "name": "ndvi",
            "statistics": {"maximum": 0.9, "mean": 0.5, "minimum": 0.1, ...}
          }
        ],
  ```

### `item_assets` property

The `item_assets` property on STAC Collections is since version 1.1.0 part of the STAC core spec
(was a STAC extension before).

- STAC 1.0 style: not available
- STAC 1.1 style:
  ```json
  "item_assets": {
     "openEO": {
  ```


### `derived_from` links

To allow reconstruction of what input data sources were used in an openEO batch job,
the openeo-geopyspark-driver provides `derived_from` links in the STAC metadata (`links` section).

- STAC 1.0 style:
  - `derived_from` links are only provided in the root STAC Collection document
  - for each input data source item (e.g. STAC Items from a STAC API),
    a separate `derived_from` link object is added
    - the number of added links grows directly with the number of input data source items,
      which can lead to unreasonable large STAC Collection documents for large batch jobs.
      This can be disabled through the `omit_derived_from_links` job option,
      but then there is no way to reconstruct the input data sources anymore.
    - also note that the link's `href` field holds some kind of reference,
      but in the most common case (STAC API based collections),
      the STAC Item id is used, which is technically not a valid `href` value
  - Example:
    ```json
    "links": [
        {
            "href": "c_gls_NDVI300_202509210000_GLOBE_OLCI_V3.0.1_cog",
            "rel": "derived_from",
            "title": "Derived from c_gls_NDVI300_202509210000_GLOBE_OLCI_V3.0.1_cog",
            "type": "application/json"
        },
        {
            "href": "c_gls_NDVI300_202509110000_GLOBE_OLCI_V3.0.1_cog",
            "rel": "derived_from",
            "title": "Derived from c_gls_NDVI300_202509110000_GLOBE_OLCI_V3.0.1_cog",
            "type": "application/json"
        },
        ...
    ```
- STAC 1.1 style:
  - `derived_from` links are provided both in root STAC Collection and in the child STAC Item documents
  - There is just one `derived_from` link per `load_collection`/`load_stac` process graph node
    - which puts minimal load on the root STAC Collection document size, even for large batch jobs
    - the link `href` is a valid, signed URL to a separate STAC Item Collection document,
      which is an extensive list of all the input data sources described as STAC Items
  - Example:
    ```json
    "links": [
        {
            "href": "https://openeo.example.com/.../stac-item-collection-loadcollection1.json?signature=...",
            "rel": "derived_from",
            "type": "application/geo+json"
        },
    ```
    - Note the reference to the process graph node id `loadcollection1` in the STAC Item Collection reference
    - The STAC Item Collection document has roughly this structure:
      ```json
      {
        "type": "FeatureCollection",
        "features": [
          {
            "stac_version": "1.1.0",
            "type": "Feature",
            "id": "c_gls_NDVI300_202509210000_GLOBE_OLCI_V3.0.1_cog",
            "collection": "clms_ndvi_global_300m_10daily_v3_cog",
            "geometry": {...},
            "properties": {...},
            "assets": {...}

      ```


## References

- [[EPIC] STAC metadata upgrade & rework #1027](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1027)
- [unify asset keys across STAC items #1111](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1111)
- [include derived_from document in "stac-version": "1.1" feature #1488](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1488)
- [derived-from: weird itemcollection #1490](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1490)
- [Serialize/reuse ItemCollection #1618](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1618)
