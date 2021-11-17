# Changelog
All notable changes to this project will be documented in this file.

This project relies on continuous integration for new features. So we do not yet have explicitly versioned
releases. Releases are simply built continuously, automatically tested, deployed to a development environment and then to production.

Note that the openEO API provides a way to support stable and unstable versions in the same implementation:
https://openeo.org/documentation/1.0/developers/api/reference.html#operation/connect

If needed, feature flags are used to allow testing unstable features in development/production,
without compromising stable operations.


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


