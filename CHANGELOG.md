# Changelog
All notable changes to this project will be documented in this file.

This project relies on continuous integration for new features. So we do not yet have explicitly versioned
releases. Releases are simply built continuously, automatically tested, deployed to a development environment and then to production.

Note that the openEO API provides a way to support stable and unstable versions in the same implementation:
https://openeo.org/documentation/1.0/developers/api/reference.html#operation/connect

If needed, feature flags are used to allow testing unstable features in development/production,
without compromising stable operations.

## Unreleased

## 0.25.0

- The default for the soft-errors job option is now set to 0.1 and made configurable at backend level. This value better recognizes the fact that many EO archives have corrupt files that otherwise break jobs [#617](https://github.com/Open-EO/openeo-geopyspark-driver/issues/617).
- Support GeoParquet output format for `aggregate_spatial` ([#623](https://github.com/Open-EO/openeo-geopyspark-driver/issues/623))

## 0.24.0

- Start using `DynamicEtlApiJobCostCalculator` in job tracker. Effective ETL API selection strategy is to be configured through `EtlApiConfig`

### Bugfix

 - added max_processing_area_pixels custom option to sar_backscatter, avoiding going out of memory when processing too large chunks


## 0.23.1

### Bugfix

- Requests towards Job Registry Elastic API are unreliable; reconsider ZK as primary data store.

## 0.23.0

### Added

 - Support disabling ZkJobRegistry ([#632](https://github.com/Open-EO/openeo-geopyspark-driver/issues/632))

## 0.22.3

### Bugfix

- Restore batch job result metadata; this reverts the Zookeeper fix introduced in 0.22.2

## 0.22.2

### Bugfix

- Prevent Zookeeper from blocking requests (https://github.com/Open-EO/openeo-geopyspark-driver/pull/639)

## 0.22.1

### Bugfix

 - Prevent usage duplication in ETL API ([#41](https://github.com/eu-cdse/openeo-cdse-infra/issues/41))

## 0.22.0

### Added

- Added config `use_zk_job_registry` to disable `ZkJobRegistry` usage

### Bugfix

- apply_neighborhood: fix error if overlap is null/None ([#519](https://github.com/Open-EO/openeo-python-client/issues/519))


## 0.21.5

- Initial implementation of `DynamicEtlApiJobCostCalculator` and added caching feature to `get_etl_api()`  ([#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531))

## 0.21.4

- Support for reading GeoPackage vector data
- move legacy-vs-dynamic ETL selection logic to `get_etl_api()` ([#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531))

## 0.21.3

- job tracker: move app state mapping to CostDetails construction time

## 0.21.2

- job tracker: pass `job_options` to JobCostsCalculator through `CostDetails` (related to [#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531))

## 0.21.1

- job tracker: do job info iteration in streaming fashion (instead of loading all job info in memory at once)

## 0.21.0

- Initial support for dynamic ETL API configuration ([#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531))


## 0.20.1

- finetune `zookeeper_set.py` script and `concurrent_pod_limit` logic


## 0.20.0

- Introduce `GpsBackendConfig.zookeeper_hosts` and `GpsBackendConfig.zookeeper_root_path`


## 0.19.4

- eliminate `use_etl_api` arg from `GeoPySparkBackendImplementation` in favor of `use_etl_api_on_sync_processing` config field
- Upgrade GDAL to 3.8.1 and Orfeo Toolbox to 8.1.2 https://github.com/Open-EO/openeo-geopyspark-driver/issues/571
- Performance improvement for apply_dimension with target='bands'  (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/235 , https://github.com/Open-EO/openeo-geopyspark-driver/issues/595 )

## 0.19.3

### Feature

- Experimental support for filter_labels: only works with catalog based collections, and when used close to the load_collection call [#559](https://github.com/Open-EO/openeo-geopyspark-driver/issues/559)
- Support for UDF signature that works directly on XArray DataArray, avoiding the need for openEO specific wrapper class.
- Support filtering on tileId with a wildcard. https://github.com/Open-EO/openeo-opensearch-client/issues/25

### Bugfix
- Error fixed when doing aggregate_temporal + merge_cubes https://github.com/Open-EO/openeo-geotrellis-extensions/issues/201
- Avoid 'out-of-memory' errors when writing large netCDF files. Allows files of >600MB without custom memory settings. [#199](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/199)
- netCDF output will generate a more useful warning in case of a mismatch with cube band metadata

## 0.19.2

- (Temporarily) disable extensive `/validation` checks on production
  (related to [#566](https://github.com/Open-EO/openeo-geopyspark-driver/issues/566), [#575](https://github.com/Open-EO/openeo-geopyspark-driver/issues/575))


## 0.18.0a1

### Removed

- Remove old "v1" `job_tracker` script ([#545](https://github.com/Open-EO/openeo-geopyspark-driver/issues/545))

## 0.17.0a1

### Feature

- date_difference, date_replace_component: support for these two experimental processes https://github.com/Open-EO/openeo-geopyspark-driver/issues/515
- array_apply: provide access to the 'label' and 'index' parameter  https://github.com/Open-EO/openeo-geotrellis-extensions/issues/205

### Bugfix

- Fix batch job deletion in EJR code path (https://github.com/Open-EO/openeo-python-driver/issues/163,
  https://github.com/Open-EO/openeo-geopyspark-driver/issues/523, https://github.com/Open-EO/openeo-geopyspark-driver/issues/498)


## 2023-09-18 (0.9.5a1)

Important change: time intervals are now left closed. Workflows that are sensitive to exact time intervals may need
to be updated.

### Feature

- load_stac support, allowing to load STAC collections that conform to the mainstream approach
  for organizing metadata in STAC. (https://github.com/Open-EO/openeo-geopyspark-driver/issues/402)
- First support for UDF's that change resolution, in Python-Jep runtime. (https://github.com/Open-EO/openeo-geotrellis-extensions/pull/197)
- Improved support for running UDF's on vector cubes.
- Support load_geojson and load_url processes to create vector cubes. (https://github.com/Open-EO/openeo-python-driver/issues/211)
- The 'partial' query parameter is now supported when requesting job results, load_stac supports loading unfinished results. (https://github.com/Open-EO/openeo-geopyspark-driver/issues/489)
- Support new (experimental) vector_to_raster process, allowing to combine data from a vector source with EO data. (https://github.com/Open-EO/openeo-geopyspark-driver/issues/423)
-

### Bugfix
- Fixed numerical rounding errors in datacubes that use epsg:4326, which could lead to pixel shift or missing lines of data.
- Fixed a deadlock, causing the backend to 'hang' on requests.  (https://github.com/Open-EO/openeo-python-driver/issues/209)
- Time intervals are now left-closed (https://github.com/Open-EO/openeo-geopyspark-driver/issues/34)
- Fixed automatic selection of polarization for SENTINEL1_CARD4L.  (https://github.com/Open-EO/openeo-geopyspark-driver/issues/473)
- Reduced memory usage in specific case of apply_neighborhood on a smaller chunk size. (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/191)
- Fixed error with apply_neighborhood on Sentinelhub backed layers. (https://github.com/Open-EO/openeo-geopyspark-driver/issues/434)
- Fix in evaluation of 'mask' process, improving performance. (https://github.com/Open-EO/openeo-python-driver/issues/161)
- Fix wrong datacube organanization for sparse cubes in aggregate_temporal(_period) (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/209)

### Changed
- Improved performance for small (synchronous) requests. (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/186)


## 2023-07-30 (0.9.5a1)

### Feature

- array_element: Support band selection by label (https://github.com/Open-EO/openeo-geopyspark-driver/issues/43)
- apply_neigborhood: Support applying function over time intervals (https://github.com/Open-EO/openeo-geopyspark-driver/issues/415)

## 2023-06-30 (0.9.5a1)

### Feature
- Add statistics to asset metadata ([#391](https://github.com/Open-EO/openeo-geopyspark-driver/issues/391))
- Add array_apply ([openeo-geotrellis-extensions/#154](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/154))

### Changed
- Add filtering on log_level when retrieving logs from Elasticsearch. ([Open-EO/openeo-python-driver#170](https://github.com/Open-EO/openeo-python-driver/issues/170))
- Prevent running UDFs in Spark driver process ([#404](https://github.com/Open-EO/openeo-geopyspark-driver/issues/404))

## 2023-03-30 (0.9.5a1)

### Bugfix
- Fix "Permission denied" issue with `run_udf` usage on vector date cube
  ([#367](https://github.com/Open-EO/openeo-geopyspark-driver/issues/367))
- Fix: Extent in STAC result metadata should be lat lon ([#321](https://github.com/Open-EO/openeo-geopyspark-driver/issues/321))
- Single row/line results with SentinelHub
  ([#375](https://github.com/Open-EO/openeo-geopyspark-driver/issues/375))
- Fix: Creodias: Download asset from object storage (S3) before extracting projection metadata
  ([#403](https://github.com/Open-EO/openeo-geopyspark-driver/issues/403))

### Changed
- /validation now detects if the amount of pixels that will be processed is too large
  ([#320](https://github.com/Open-EO/openeo-geopyspark-driver/issues/320))
- Add projection extension metadata to batch job results ([openeo-geotrellis-extensions/#72](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/72))


## 2023-03-08 (0.9.3a1)

Build `20230307-1166`, with components:
`openeo-geopyspark-0.9.3a1.dev20230307+1073`, `openeo_driver-0.37.0a1.dev20230307+441`,
`openeo-0.15.0`, `geotrellis-extensions-static-2.3.0_2.12-SNAPSHOT-5822561`

### Feature
- Add "filename_prefix" to format_options.

### Bugfix
- Area returned by job metadata is now calculated using WGS84 ellipsoid (https://github.com/Open-EO/openeo-python-driver/issues/144)

## 2023-02-27 (0.7.0a1)

Build `20230221-1118`

Note: this deploy was rolled back to previous build `20230117-966` the same day.

- GeoParquet support to allow loading large vector files
- Improved specific log messages
- Better support for multiple filter_spatial prcesses in same process graph (https://github.com/Open-EO/openeo-geopyspark-driver/issues/147)
- Bugfix for sampling sentinelhub based collections (https://github.com/Open-EO/openeo-geopyspark-driver/issues/279)
- vector_buffer: Throw an error when a negative buffer size resuls in invalid geometries (https://github.com/Open-EO/openeo-python-driver/issues/164)
- batch jobs now also report usage of credits (https://github.com/Open-EO/openeo-geopyspark-driver/issues/272)
- non-utm collections should now have a better alignment to the original rasters, if the process graph does not apply an explicit resampling (https://github.com/Open-EO/openeo-geotrellis-extensions/issues/69)


## 2023-02-07 (0.6.7a1)

Build `20230117-966`

- Added initial support for the `inspect` process. It can be used on datacubes and in callbacks.
- The size of a single chunk is now automatically increased for larger jobs, to improve IO performance.
- resample_cube_spatial is no longer needed in all cases when using `merge_cubes`or `mask`
- Better detection of duplicate products in source catalogs
- The 'if' process will no longer evaluate the branch that is not accepted https://github.com/Open-EO/openeo-python-driver/issues/109

## 2023-01-20 (0.6.7a1)
- Changed: Getting a job's logs now leaves out log lines that have no loglevel or a level that is not supported. [openeo-python-driver/#160](https://github.com/Open-EO/openeo-python-driver/issues/160)

## 2022-11-28 (0.6.3a1)
- Added an experimental job option 'udf-dependency-archives' to pass on archives of UDF dependencies

## 2022-10-27 (0.6.3a1)
- Reprojection is performed at load time whenever possible, by pushing down parameters from resample_spatial and resample_cube_spatial
- PROBA-V collections can now be loaded at original resolution
- Overlap between original products is now handled based on the footprint in STAC/Opensearch metadata
- Logging for synchronous jobs is now more complete
- First prototype for running vector data UDF's on Spark
- Bugfix: allow large (multiple GB) CSV output
- Try to avoid going out of memory by reducing default partition size


## 2022-09-21 (0.6.3a1)
- Expose logging from UDF's
- Feature id's from GeoJSON are used to name timeseries in netCDF export
- NetCDF's are now cropped to provided extent
- Support remote STAC collections
- Sentinelhub usage is now recorded for batch jobs
- "task-cpus" job option to control number of cpu's for a single Spark task. Mostly relevant for UDF's that use multi-threaded libraries such as Tensorflow.
- New processes:
  - array_find
  - exp

## 2022-05-04 (0.6.3a1)

- Enable JSON logging from batch_job.py (and inject user_id/job_id)
- New processes:
  - predict_catboost (not-standard)
  - predict_random_forest
  - fit_class_random_forest
  - array_interpolate_linear
- Faster sar_backscatter both for large areas and sparse sampling
- STAC metadata for random forest models
- Colormap support in PNG's
- Support custom Sentinelhub collections, e.g. PlanetScope data
- 'soft-errors' job option to allow failure of individual Sentinelhub requests

## 2022-04-07 (0.6.2a1)

- EP-4012: implement collection source selection based on product availability (e.g. collection "SENTINEL2_L2A"
  "forwards" to "TERRASCOPE_S2_TOC_V2" when possible, but falls back to "SENTINEL2_L2A_SENTINELHUB"
  when there are missing products for the selected spatiotemporal extent.

## 2021-11-17

### Feature

- Support load_result
- Allow raster masks to filter a collection before loading any data
- Caching of Sentinelhub data
- Streaming writing of netCDF files
- Support filter_spatial
- Support first and last processes
- Jep based UDF implementation

## 2021-07-14

### Changed

- Add support for `openeo.udf` based UDFs and keep backward compatibility with `openeo_udf` based UDFs
  (EP-3856, [#78](https://github.com/Open-EO/openeo-geopyspark-driver/issues/78),
  [#93](https://github.com/Open-EO/openeo-geopyspark-driver/issues/93))


## 2021-04-08

### Feature
- Add support for (multiple) default OIDC clients (for EGI Check-in OIDC provider) (EP-3700, [Open-EO/openeo-api#366](https://github.com/Open-EO/openeo-api/pull/366))

## 2021-03-30
### Feature
- Add support for Sentinelhub layers on different endpoints (e.g. Landsat-8, MODIS)
- In batch jobs, write one geotiff per date as opposed to reducing all dates into a single pixel
- Improved CARD4L metadata generation for atmospheric_correction


## 2021-03-12

- Fix support for UDPs in batch jobs (EP-3754)
- Fix support for custom processes in batch jobs (EP-3771)

## 2021-01-26
### Feature
Add an experimental resolution_merge for Sentinel-2 based on the implemntation in FORCE.

Support reading Copernicus Global Land NetCDF files.

Support the Sentinelhub batch process API to generate Sentinel-1 backscatter data.

The atmospheric_correction process can now apply iCor on SentinelHub layers.

## 2021-01-25
### Feature
- Add implementation of on-the-fly Sentinel1 Backscatter (Sigma0) calculation using Orfeo Toolbox on Creodias (EP-3612)

## 2020-12-06
### Performance
Performance improvement for requests with small spatial extents. The backend was loading too much tile metadata.

## 2020-11-11
### Feature
Support the "if" process:https://processes.openeo.org/#if

Major performance improvements for SentinelHub layers. The UTM projection is now used
by default when processing these layers. The datatype is no longer set to float by default.

## 2020-10-28
### Internal
Refactored internal process graph parsing: first to a dry-run processing to extract
information that can help loading initial data sources. (EP-3509)

## 2020-10-14
### Feature
Support "PNG" output format (non-indexed only).

## 2020-10-06
### Performance improvement
Geotiff (GTiff) output format is faster to generate, and works for larger areas.

### Compatibility
Copernicus projections stored in UTM are now also processed and returned in UTM, as opposed to web mercator.
This affects processing parameters that depend on a specific projection, like the size of pixels in map units.

This change also improves memory usage and performance for a number of layers.
