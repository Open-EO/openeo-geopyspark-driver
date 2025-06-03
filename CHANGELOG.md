# Changelog
All notable changes to this project will be documented in this file.

This project relies on continuous integration for new features. So we do not yet have explicitly versioned
releases. Releases are simply built continuously, automatically tested, deployed to a development environment and then to production.

Note that the openEO API provides a way to support stable and unstable versions in the same implementation:
https://openeo.org/documentation/1.0/developers/api/reference.html#operation/connect

If needed, feature flags are used to allow testing unstable features in development/production,
without compromising stable operations.

<!-- start-of-changelog -->


## In progress: 0.66.0

- 'Batch job options' are now advertised following the openEO [processing parameters extension](https://github.com/Open-EO/openeo-api/tree/draft/extensions/processing-parameters)  ([Open-EO/openeo-python-driver#307](https://github.com/Open-EO/openeo-python-driver/issues/307))
- use region specific config for workspace actions
- `apply_neighborhood`/`apply_dimension`: support changing band names via apply_metadata ([#1155](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1155))
- `StacApiWorkspace`: support arbitrary paths in `merge`; the last part of a path becomes the collection ID in the STAC API ([#1074](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1074))
- Fix compatibility with Shapely 2 ([#1161](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1161))
- Change default `use_zk_job_registry` config to `False` ([#632](https://github.com/Open-EO/openeo-geopyspark-driver/issues/632), [#863](https://github.com/Open-EO/openeo-geopyspark-driver/issues/863), [#1165](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1165))
- Add timeout to metadata request on "stac" data sources in layer catalog ([eu-cdse/openeo-cdse-infra#525](https://github.com/eu-cdse/openeo-cdse-infra/issues/525))
- Improve detection of `apply_metadata` function in UDF code (related to [#1182](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1182))
- Fix `require_application_id` filter of `list_active_jobs` in job tracker ([#1197](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1197))
- Remove outdated and defunct ZooKeeper "cleaner" scripts and code paths ([#1219](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1219), [#1123](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1123))
- Move `integrations.s3` to Python driver [#1195](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1195)
- Add config `ejr_preserialize_process` to enable process graph pre-serialization when storing new jobs in EJR ([#1232](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1232))


## 0.65.0

- `sar_backscatter`: soft-errors can now be a fraction, allowing to tolerate a certain failure percentage provided as a number between 0 and 1. ([#443](https://github.com/Open-EO/openeo-geopyspark-driver/issues/443))
- `save_result`: write GeoTiff assets with valid tile size; override with `tile_size` format option. ([#1083](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1083))
- `sar_backscatter` support: automatically use backend-specific `coefficient` default even if there is no explicit `sar_backscatter` in the process graph ([Open-EO/openeo-python-driver#376](https://github.com/Open-EO/openeo-python-driver/issues/376))
- Add `capabilities_extras` config to easily extend capabilities document ([Open-EO/openeo-python-driver#384](https://github.com/Open-EO/openeo-python-driver/issues/384))
- Add `CalrissianS3Result.generate_presigned_url()` ([#937](https://github.com/Open-EO/openeo-geopyspark-driver/issues/937))
- Restore COG layout for `GTiff` output format ([Open-EO/openeo-geotrellis-extensions#393](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/393))
- Add `CalrissianS3Result.download()` ([#937](https://github.com/Open-EO/openeo-geopyspark-driver/issues/937))
- Support additional Sentinel 3 collections ([eu-cdse/openeo-cdse-infra#380](https://github.com/eu-cdse/openeo-cdse-infra/issues/380))
- Add `CalrissianS3Result.generate_public_url()` ([#937](https://github.com/Open-EO/openeo-geopyspark-driver/issues/937))
- Replace `ConfigParams.layer_catalog_metadata_files` with `GpsBackendConfig.layer_catalog_files` ([#285](https://github.com/Open-EO/openeo-geopyspark-driver/issues/285), [#1084](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1084))
- `StacApiWorkspace`: log body of error response from STAC API for better root cause analysis ([#1116](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1116))
- `export_workspace`: add `"derived_from"` links to STAC Collection ([#1050](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1050))
- Calrissian integration: avoid unnecessary pulls of `alpine` image ([#1132]https://github.com/Open-EO/openeo-geopyspark-driver/issues/1132)
- Calrissian integration: refactor config to a `CalrissianConfig` sub-config ([#1009](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1009))
- `save_result`: support zarr format (experimental)
- `save_result`: allow non-string values in `GTiff` `file_metadata` ([#1142](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1142))
- Add `udp_registry_zookeeper_client_reuse` config for `KazooClient` reuse in `ZooKeeperUserDefinedProcessRepository` ([#1037](https://github.com/Open-EO/openeo-geopyspark-driver/pull/1037))
- `GpsBackendConfig`: be more forgiving about unknown config keys to better support use cases that involve backward/forward incompatible configurations ([Open-EO/openeo-python-driver#322](https://github.com/Open-EO/openeo-python-driver/issues/322))
- Improved API alignment between `DoubleJobRegistry` and `JobRegistryInterface`/`ElasticJobRegistry` ([#863](https://github.com/Open-EO/openeo-geopyspark-driver/issues/863), [#1123](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1123))
- Add `use_new_feature_extent_intersection_2` option to `load_collection`: To use new intersection code to work with products crossing the antimeridian. ([#1072](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1072))


## 0.64.1

- `load_custom_processes`: allow specifying path directly


## 0.63.0

- Add `bbox` and `geometry` to exported STAC items pertaining to GeoTiff assets of a spatial data cube ([eu-cdse/openeo-cdse-infra#418](https://github.com/eu-cdse/openeo-cdse-infra/issues/418))
- Support UDF dependency extraction from remote process definitions (URL based UDPs) ([#1063](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1063))
- `StacApiWorkspace`: improve STAC requests resilience ([#1073](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1073))
- `StacApiWorkspace`: reject unsupported characters in `merge` ([eu-cdse/openeo-cdse-infra#415](https://github.com/eu-cdse/openeo-cdse-infra/issues/415))
- `load_stac`: fix delay in driver due to expensive item/asset processing ([#1081](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1081))


## 0.62.0

- `load_stac`: improve STAC requests resilience ([#818](https://github.com/Open-EO/openeo-geopyspark-driver/issues/818))
- Extract demo CWL content to package resources ([#1057](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1057))
- Disable EJR health check from batch job context ([#1066](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1066))
- `load_stac`: support empty data cubes ([#1049](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1049))
- Remove proof-of-concept CWL processes (now in openeo-geotrellis-kubernetes) from generic openeo-geopyspark-driver ([#1057](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1057)/[#1038](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1038))


## 0.61.0

- `python-memory`: make job option available on YARN, add a default config
- `load_stac`: optimize resolution with regard to requested bands (experimental) ([#1043](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1043))
- `load_stac`: apply offset (experimental) ([#1051](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1051))
- Deprecate non-standard "logging-threshold" job option in favor of standardized "log_level" job creation parameter ([#909](https://github.com/Open-EO/openeo-geopyspark-driver/issues/909))
- Fail fast on UDF dependency installation failure ([#1048](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1048))
- `load_stac`: avoid OOM on global low-res assets ([#1055](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1055))

## 0.60.1

- `load_stac`: restore spatial dimensions of data cube loaded from STAC Collection that lacks `cube:dimensions`. ([#1036](https://github.com/Open-EO/openeo-geopyspark-driver/issues/1036))
- `sar_backscatter`: report soft-errors fraction in usage metrics
- `apply_neighborhood`: for t='P1D', add date to dataarray.attrs with key 't'

## 0.60.0

- Make environment variables to be passed from web app driver to batch job driver configurable ([#867](https://github.com/Open-EO/openeo-geopyspark-driver/issues/867))
- `load_collection`/`load_stac`: support parameters in `properties` ([Open-EO/openeo-python-driver#327](https://github.com/Open-EO/openeo-python-driver/issues/327))

## 0.59.0

- load_stac: cube creation is now cached, just like load_collection ([#993](https://github.com/Open-EO/openeo-geopyspark-driver/issues/993))
- logs: Provide a performance summary at the end of a batch job.
- `StacApiWorkspace`: support `filepath_per_band` ([#867](https://github.com/Open-EO/openeo-geopyspark-driver/issues/867))
- `load_stac`: use STAC API Filter Extension to prevent driver OOM ([#979](https://github.com/Open-EO/openeo-geopyspark-driver/issues/979))

## 0.58.1

- Fix spatial and temporal extents of exported STAC Collection ([#867](https://github.com/Open-EO/openeo-geopyspark-driver/issues/867))

## 0.58.0

- Improve "App not found" logs to avoid red herrings in root cause analysis ([eu-cdse/openeo-cdse-infra#147](https://github.com/eu-cdse/openeo-cdse-infra/issues/147))
- Avoid pixel shift when source data is not aligned in load_stac. ([#648](https://github.com/Open-EO/openeo-geopyspark-driver/issues/648))
- Fix when outputting an empty vector cube to GeoParquet ([#987](https://github.com/Open-EO/openeo-geopyspark-driver/issues/987))
- Fix for outputting a vector cube to legacy `timeseries.json` format. ([#342](https://github.com/Open-EO/openeo-python-driver/issues/342))
- Add gdalinfo json files next to tiff files. ([openeo-geotrellis-extensions#352](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/352))
- Support exporting assets for different collections to different paths ([#867](https://github.com/Open-EO/openeo-geopyspark-driver/issues/867))
- load_collection: bugfix in 'global extent' computation, increases performance ([#334](https://github.com/Open-EO/openeo-python-driver/issues/334))

## 0.57.0

- Initial support for S3 profiles and tokens during batch job execution ([#969](https://github.com/Open-EO/openeo-geopyspark-driver/pull/969))

## 0.56.0

- Initial support for CWL based processes with Calrissian on a Kubernetes cluster ([#936](https://github.com/Open-EO/openeo-geopyspark-driver/issues/936))

## 0.55.0

- Support `file_metadata` format option to set file-specific metadata on `GTiff` output assets ([#970](https://github.com/Open-EO/openeo-geopyspark-driver/issues/970))
- load_collection for Sentinel-3 Level2 data: avoid data corruption in specific case ([#755](https://github.com/Open-EO/openeo-geopyspark-driver/issues/755))

## 0.54.0

- `export_workspace`: experimental support for merging into STAC API ([#867](https://github.com/Open-EO/openeo-geopyspark-driver/issues/867))

## 0.53.1

- `export_workspace`: fix `KeyError: 'alternate'` upon merging into existing STAC collection ([#677](https://github.com/Open-EO/openeo-geopyspark-driver/issues/677))

## 0.53.0

- `export_workspace`: experimental support for merging STAC Collections ([#677](https://github.com/Open-EO/openeo-geopyspark-driver/issues/677))
- Add support for `orgId` in ETL resource/cost reporting ([#671](https://github.com/Open-EO/openeo-geopyspark-driver/issues/671))
- `load_stac`: Align output pixels with source pixels if source data is UTM and has an offset. ([#648](https://github.com/Open-EO/openeo-geopyspark-driver/issues/648))

## 0.52.0

- Throw error when trying to use unsupported `target_dimension` in `aggregate_spatial` ([#951](https://github.com/Open-EO/openeo-geopyspark-driver/issues/951))
- Allow specifying region name for an ObjectStorageWorkspace ([#955](https://github.com/Open-EO/openeo-geopyspark-driver/pull/955))
- Better print ApiException. ([#962](https://github.com/Open-EO/openeo-geopyspark-driver/pull/962))
- Include job title by default in user job listings ([#963](https://github.com/Open-EO/openeo-geopyspark-driver/issues/963))
- Support pagination of user job listings ([#959](https://github.com/Open-EO/openeo-geopyspark-driver/issues/959)/[Open-EO/openeo-python-driver#332](https://github.com/Open-EO/openeo-python-driver/issues/332))

## 0.51.0

- `load_stac`: omit `datetime` parameter from STAC API item search request if no `temporal_extent` specified ([#950](https://github.com/Open-EO/openeo-geopyspark-driver/issues/950))

## 0.50.1

- Fix `reduce_dimension` of bands for GeoTIFF output in batch job ([#943](https://github.com/Open-EO/openeo-geopyspark-driver/issues/943))


## 0.50.0

- Fix type of `ZLEVEL` option for `GTiff` format
- Add `filepath_per_band` to `save_result` options. ([#877](https://github.com/Open-EO/openeo-geopyspark-driver/issues/877))
- Allow pointing to custom processes with `OPENEO_CUSTOM_PROCESSES` env var (related to [#936](https://github.com/Open-EO/openeo-geopyspark-driver/issues/936))


## 0.49.1

- Py4j log level is now always 'WARN' to avoid spurious messages.
- Fix removal of multiple original job result assets ([#883](https://github.com/Open-EO/openeo-geopyspark-driver/issues/883))
- Fix using asset_per_band with large extends giving partial tiff files ([Open-EO/openeo-geotrellis-extensions#329](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/329))

## 0.49.0

- Fix `load_stac` for collections from stac.terrascope.be ([#862](https://github.com/Open-EO/openeo-geopyspark-driver/issues/862))
- Point `href` of job result asset to workspace URI if original was removed ([#883](https://github.com/Open-EO/openeo-geopyspark-driver/issues/883))

## 0.48.2

- Fix `resample_spatial` of Sentinel-3 data cube ([#920](https://github.com/Open-EO/openeo-geopyspark-driver/issues/920))

## 0.48.0

- Expose `filename_prefix` format option for `netCDF` output assets ([#876](https://github.com/Open-EO/openeo-geopyspark-driver/issues/876))
- Make sure `OPENEO_BACKEND_CONFIG` env var is set in K8s executors

## 0.47.0

- Support `bands_metadata` format option to set band-specific scale, offset and other metadata on `GTiff` output assets ([Open-EO/openeo-geotrellis-extensions#317](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/317))

## 0.46.0

- Automatic Python UDF dependency handling: add option to work with ZIP archive
  instead of full tree in job work folder, to improve performance/stability
  in contexts where large file trees under the job work folder are not ideal,
  e.g. FUSE-mounted S3 storage
  ([#845](https://github.com/Open-EO/openeo-geopyspark-driver/issues/845), [docs](docs/udf-deps.md))

## 0.45.0

- Experimental support for removal of originals of assets exported to workspace ([#883](https://github.com/Open-EO/openeo-geopyspark-driver/issues/883))
- A rounding bug was fixed in a downstream library that in specific cases leads to a change in the number of pixel rows/columns in the output. We mainly observe this when the input bounding box is not well aligned to the pixel grid of the Copernicus data. [#297](https://github.com/Open-EO/openeo-geopyspark-driver/issues/297)
- Fixed an issue where jobs asset_per_band sometimes returned empty tiff files.  ([Open-EO/openeo-geotrellis-extensions#329](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/329))

## 0.44.1

- Stream assets from object storage to prevent batch job driver pod from being OOMKilled ([eu-cdse/openeo-cdse-infra#278](https://github.com/eu-cdse/openeo-cdse-infra/issues/278))

## 0.44.0

- Job tracker: only consider jobs _updated_ in last 2 weeks ([#902](https://github.com/Open-EO/openeo-geopyspark-driver/issues/902))

## 0.43.0

- Support exporting objects to object storage workspace ([eu-cdse/openeo-cdse-infra#278](https://github.com/eu-cdse/openeo-cdse-infra/issues/278))

## 0.42.0

- Job tracker (still based on `DoubleJobRegistry`): only consider last 2 weeks of "trackable" jobs ([#902](https://github.com/Open-EO/openeo-geopyspark-driver/issues/902))

## 0.41.0

- quantiles, when used in apply_dimension was corrected to use the interpolation method that is prescribed by the openEO process definition.
- return STAC Items with valid date/time for time series job results ([#852](https://github.com/Open-EO/openeo-geopyspark-driver/issues/852))
- filter_labels now also supported for collections that use sar_backscatter and use the internal Orfeo toolbox based method to compute backscatter on the fly. For example: SENTINEL1_GRD ([Open-EO/openeo-geotrellis-extensions#320](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/320))
- support mask assets in `load_stac` ([#874](https://github.com/Open-EO/openeo-geopyspark-driver/issues/874))
- align `DataCubeParameters` with `load_collection` ([#812](https://github.com/Open-EO/openeo-geopyspark-driver/issues/812))
- apply/apply_dimension(dimension='bands'): nodata tiles were removed as an optimization, but this could lead to unexpected results depending on subsequent steps. They are now replaced with a memory efficient implementation. ([WorldCereal issue][https://github.com/WorldCereal/worldcereal-classification/issues/141])
- `load_collection` with an excessive extent (temporal or spatial) will now be blocked to avoid excessive resource usage. This check can be disabled with `job_options.do_extent_check=False` ([#815](https://github.com/Open-EO/openeo-geopyspark-driver/issues/815))
- Mixing bands with signed and unsigned data types could lead to negative values being misrepresented. This is now fixed by using the correct data type for the output.
- Logging output is being reduced to focus on most relevant messages from a user perspective.
- Support multiple `export_workspace` processes ([eu-cdse/openeo-cdse-infra#264](https://github.com/eu-cdse/openeo-cdse-infra/issues/264))
- Fix `export_workspace` process not executed in process graph with multiple `save_result` processes ([eu-cdse/openeo-cdse-infra#264](https://github.com/eu-cdse/openeo-cdse-infra/issues/264))

## 0.40.1

- Fix `load_stac` of GeoTiff batch job results by returning compliant GeoJSON in `"geometry"` of STAC Items. [#854](https://github.com/Open-EO/openeo-geopyspark-driver/issues/854)

## 0.40.0

- a new 'python-memory' option allows to more explicitly limit memory usage for UDF's, sar_backscatter and Sentinel3 data loading. The executor-memoryOverhead option can be reduced or removed when using the new python-memory option.
- The default processing chunk size can now be configured for backends. If not set, the default may be determined automatically. We observe that a lower default, like 128 pixels, allows running jobs with less memory. ([Open-EO/openeo-geotrellis-extensions#311](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/311))
- aggregate_spatial: trying to use the probabilities argument in a _single_ 'quantiles' reduces was throwing an error. ([#821](https://github.com/Open-EO/openeo-geopyspark-driver/issues/821))
- sar_backscatter: when a target resolution is provided via resample_spatial, it is now immediately taken into account for computing backscatter, reducing memory usage.
- the temporary folder which is created for aggregate_spatial now contains a timestamp to aid cleanup scripts.
- apply_neighborhood: support applying UDF on cubes without a time dimension
- Add "separate_asset_per_band" to save_result options. Currently, for TIFF only.
- `load_stac`: presence of `eo:bands` is no longer a hard requirement; band name defaults to asset key [#762](https://github.com/Open-EO/openeo-geopyspark-driver/issues/762).
- Optionally sleep after automatic UDF dependency install (config `udf_dependencies_sleep_after_install`) (eu-cdse/openeo-cdse-infra#112)

## 0.39.0

- Correctly apply the method parameter in resample_spatial and resample_cube_spatial, when downsampling to lower resolution, and the sampling is not applied at load time. ([Open-EO/openeo-geotrellis-extensions#303](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/303))
- Use band names as column name in GeoParquet output ([#723](https://github.com/Open-EO/openeo-geopyspark-driver/issues/723))
- Prevent nightly cleaner from failing a job tracker run ([eu-cdse/openeo-cdse-infra#166](https://github.com/eu-cdse/openeo-cdse-infra/issues/166))
- Sentinelhub collections handle non zero nodata better ([openeo-geotrellis-extensions#300](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/300))
- Support `allow_empty_cubes` job option ([#649](https://github.com/Open-EO/openeo-geopyspark-driver/issues/649))
- Cross-backend jobs: support running main job on backends that lack `async_task` infrastructure. ([#786](https://github.com/Open-EO/openeo-geopyspark-driver/issues/786))
- Support `save_result` processes in arbitrary subtrees in the process graph i.e. those not necessarily contributing to the final result ([#424](https://github.com/Open-EO/openeo-geopyspark-driver/issues/424))

## 0.38.6

- Increase default resource usage configs for sync processing (eu-cdse/openeo-cdse-infra#158)

## 0.38.5

- Fix EJR configuration in batch jobs on YARN ([#792](https://github.com/Open-EO/openeo-geopyspark-driver/issues/792))
- Improvements to DriverVectorCube support in `apply_polygon`
  ([Open-EO/openeo-python-driver#287](https://github.com/Open-EO/openeo-python-driver/issues/287),
  [Open-EO/openeo-python-driver#288](https://github.com/Open-EO/openeo-python-driver/issues/288),
  [Open-EO/openeo-python-driver#291](https://github.com/Open-EO/openeo-python-driver/pull/291),
  [#801](https://github.com/Open-EO/openeo-geopyspark-driver/pull/801))

## 0.38.4

- Fix load_stac from unsigned job results URL in batch jobs ([#792](https://github.com/Open-EO/openeo-geopyspark-driver/issues/792))

## 0.38.3

- Automatically include declared/installed UDF dependencies in `PYTHONPATH` on YARN deploys ([#237](https://github.com/Open-EO/openeo-geopyspark-driver/issues/237))

## 0.38.2

- Automatically include declared/installed UDF dependencies in `PYTHONPATH` on K8s deploys ([#237](https://github.com/Open-EO/openeo-geopyspark-driver/issues/237))
- Less pixels will become nodata in special cases ([Open-EO/openeo-geotrellis-extensions#280](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/280))

## 0.38.1

- fix load_stac from MS Planetary Computer STAC API ([#784](https://github.com/Open-EO/openeo-geopyspark-driver/issues/784))

## 0.38.0

- Initial, experimental support for automatic installing declared UDF dependencies ([#237](https://github.com/Open-EO/openeo-geopyspark-driver/issues/237))

## 0.37.2

- load_stac: incorporate STAC Item geometry ([#778](https://github.com/Open-EO/openeo-geopyspark-driver/issues/778))

## 0.37.1

- load_stac from STAC API: fix CRS and resolution of output assets ([#781](https://github.com/Open-EO/openeo-geopyspark-driver/issues/781))

## 0.37.0

- Support additional options in `FreeIpaClient.user_add()` (eu-cdse/openeo-cdse-infra#56)

## 0.36.0

- Job tracker: skip jobs where application id can't be found (instead of giving status "error") to be
  less destructive in distributed contexts with partial replication
  (related to eu-cdse/openeo-cdse-infra#141)

## 0.35.0

- Add config `zk_job_registry_max_specification_size` to set a limit on the size of the process graph items
  when registering a new batch job with `ZkJobRegistry`. Jobs with a process graph that is too large
  will be partially stored in the registry: most metadata will be available, but use cases that try
  to get the process graph itself will fail with a JobNotFound-like error.
  This is intended to be combined with `ElasticJobRegistry` through `DoubleJobRegistry` to allow `ElasticJobRegistry`
  to act as fallback for jobs that are too large for `ZkJobRegistry`.
  (related to [#498](https://github.com/Open-EO/openeo-geopyspark-driver/issues/498), eu-cdse/openeo-cdse-infra#141)

## 0.34.0

- load_stac from unsigned job results URL: fix CRS and resolution of output assets ([#669](https://github.com/Open-EO/openeo-geopyspark-driver/issues/669))
- Align job registry implementations to omit "process" and "job_options" in user job listings
  (related to [#498](https://github.com/Open-EO/openeo-geopyspark-driver/issues/498), eu-cdse/openeo-cdse-infra#141)

## 0.33.1

- array_create bugfix to support mixed data types ([#287](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/287))

## 0.33.0

- Support correlation ID in job tracker logs ([#707](https://github.com/Open-EO/openeo-geopyspark-driver/issues/707))

## 0.32.0

- Always enable `allow_dynamic_etl_api` from synchronous processing (drop feature flag)
  ([#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531), eu-cdse/openeo-cdse-infra#114)

## 0.31.1

- Initial support for `job_options` handling in `OpenEoBackendImplementation.request_costs()`
  ([#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531), eu-cdse/openeo-cdse-infra#114)

## 0.31.0

- vector_to_raster now returns a valid raster cube ([Open-EO/openeo-python-driver#273](https://github.com/Open-EO/openeo-python-driver/issues/273))
- aggregate_spatial can now be used as input for vector_to_raster ([#663](https://github.com/Open-EO/openeo-geopyspark-driver/issues/663))
- raster_to_vector now returns a valid vector cube ([Open-EO/openeo-python-driver#276](https://github.com/Open-EO/openeo-python-driver/issues/276))
- raster_to_vector now includes the pixel values of the output polygons ([#578](https://github.com/Open-EO/openeo-geopyspark-driver/issues/578))
- raster_to_vector now adds an id to each polygon including band name, date, and index ([#578](https://github.com/Open-EO/openeo-geopyspark-driver/issues/578))
- resample_spatial now has better performance when resampling to a high resolution ([#265](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/265))
- support property filters for STAC based collections ([#460](https://github.com/Open-EO/openeo-geopyspark-driver/issues/460))
- performance improvement when reading from object storage ([#740](https://github.com/Open-EO/openeo-geopyspark-driver/issues/740))
- support Microsoft Planetary Computer in load_stac ([#760](https://github.com/Open-EO/openeo-geopyspark-driver/issues/760))

## 0.30.2

- load_stac: fix filtering by Item properties
- Fix common cause of 'TopologyException' errors in vector processing
- sample_by_feature will now use the 'feature_id_property' setting for naming generated assets. [#722](https://github.com/Open-EO/openeo-geopyspark-driver/issues/722)

## 0.30.1

- Reinstate `ejr_credentials_vault_path` config option.

## 0.30.0

- Remove deprecated and unused `ejr_credentials_vault_path` config option.

## 0.29.0

- aggregate_temporal(_period) Performance improvement to use number of bands in metadata rather than computing the band count.
- Added initial implementation of FreeIpaClient (eu-cdse/openeo-cdse-infra#56)

## 0.28.2

- Mask process now also works when the mask is in a different projection, so resample_cube_spatial is needed in fewer cases.
- Improve resilience by retrying ETL API requests ([#720](https://github.com/Open-EO/openeo-geopyspark-driver/issues/720)).

## 0.28.1

- Support excluding Sentinel Hub processing units from usage reporting ([openeo-cdse-infra#37](https://github.com/eu-cdse/openeo-cdse-infra/issues/37)).

## 0.28.0

- Export to JSON is now more robust, supports datetime objects returned by dimension_labels, and will default to the string representation.
- GDAL upgraded to 3.8.4 and Orfeo Toolbox to 8.1.2. This mainly reduces the volume of bytes read from object storage by GDAL. ([#571](https://github.com/Open-EO/openeo-geopyspark-driver/issues/571))
- Size of incoming requests is now limited to 2MB by default ([Open-EO/openeo-python-driver#254](https://github.com/Open-EO/openeo-python-driver/issues/254))
- load_stac: support loading netCDF multiple netCDF items with a time dimension, as produced with 'sample_by_feature' option
- In batch result STAC metadata proj:shape is fixed to be in Y-X order, as prescribed by the standard. ([#693](https://github.com/Open-EO/openeo-geopyspark-driver/issues/693))
- Copy batch job output assets to a workspace with the `export_workspace` process ([#676](https://github.com/Open-EO/openeo-geopyspark-driver/issues/676)).
- Support vector cubes loaded from `load_url` in "sample_by_feature" feature ([#700](https://github.com/Open-EO/openeo-geopyspark-driver/issues/700))
- Keep polygons and multipolygons sorted when calling `aggregate_spatial` ([#60](https://github.com/eu-cdse/openeo-cdse-infra/issues/60))

## 0.27.1

- Add timeout to requests towards ETL API to unblock JobTracker ([#690](https://github.com/Open-EO/openeo-geopyspark-driver/issues/690)).

## 0.27.0

- Expose "bbox" and "geometry" for spatial STAC Item with netCDF assets ([#646](https://github.com/Open-EO/openeo-geopyspark-driver/issues/646))

## 0.26.2

- `MultiEtlApiConfig`: don't fail-fast on missing env vars for credentials extraction, just skip with warnings for now

## 0.26.1

### Bugfix

- fix load_stac from unsigned job results URL in batch job ([#644](https://github.com/Open-EO/openeo-geopyspark-driver/issues/644))

## 0.26.0

- Introduce `MultiEtlApiConfig` to support multiple ETL API configurations ([#531](https://github.com/Open-EO/openeo-geopyspark-driver/issues/531))


## 0.25.0

- The default for the soft-errors job option is now set to 0.1 and made configurable at backend level. This value better recognizes the fact that many EO archives have corrupt files that otherwise break jobs [#617](https://github.com/Open-EO/openeo-geopyspark-driver/issues/617).
- Support GeoParquet output format for `aggregate_spatial` ([#623](https://github.com/Open-EO/openeo-geopyspark-driver/issues/623))

### Improved datatype conversion

A rather big improvement in this release is the handling of datatypes.
OpenEO does not have very explicit rules when it comes to datatypes, and in this implementation, in most
cases the datatype was simply preserved, like in most programming languages.

For most users, this resulted in unexpected behaviour, for instance when dividing integer dataypes,
or subtracting two unsigned 8 bit numbers, and expecting to get negative values.

This implementation will now try to use wider datatypes when necessary. For instance by
switching to floating point when performing a division. This change makes writing formulas more intuitive, and should
save time debugging issues.

When there is still a need to get a smaller datatype, users can use the 'linear_scale_range' process. This process
for instance will convert to 8 bit unsigned integers if the target range uses integer values and fits in the [0,255] range.

Relevant issues:
- [#225](https://github.com/Open-EO/openeo-geotrellis-extensions/issues/225)
- [#581](https://github.com/Open-EO/openeo-geopyspark-driver/issues/581)
- [#601](https://github.com/Open-EO/openeo-geopyspark-driver/issues/601)


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
- Changed: Getting a job's logs now leaves out log lines that have no loglevel or a level that is not supported. [Open-EO/openeo-python-driver#160](https://github.com/Open-EO/openeo-python-driver/issues/160)

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

<!-- end-of-changelog -->
