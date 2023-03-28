# Changelog
All notable changes to this project will be documented in this file.

This project relies on continuous integration for new features. So we do not yet have explicitly versioned
releases. Releases are simply built continuously, automatically tested, deployed to a development environment and then to production.

Note that the openEO API provides a way to support stable and unstable versions in the same implementation:
https://openeo.org/documentation/1.0/developers/api/reference.html#operation/connect

If needed, feature flags are used to allow testing unstable features in development/production,
without compromising stable operations.

## [Unreleased]

- Fix "Permission denied" issue with `run_udf` usage on vector date cube
  ([#367](https://github.com/Open-EO/openeo-geopyspark-driver/issues/367))
- /validation now detects if the amount of pixels that will be processed is too large
  ([#320](https://github.com/Open-EO/openeo-geopyspark-driver/issues/320))
  

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
