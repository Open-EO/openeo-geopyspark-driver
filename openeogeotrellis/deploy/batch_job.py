import concurrent
import json
import logging
import os
import shutil
import stat
import sys
import time
from copy import deepcopy
from functools import lru_cache, partial
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import pystac
from openeo.util import TimingLogger, dict_no_none, ensure_dir
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.backend import BatchJobs
from openeo_driver.datacube import DriverDataCube, DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import (
    ImageCollectionResult,
    JSONResult,
    MlModelResult,
    SaveResult,
    VectorCubeResult,
)
from openeo_driver.users import User
from openeo_driver.util.logging import (
    LOG_HANDLER_FILE_JSON,
    LOG_HANDLER_STDERR_JSON,
    LOGGING_CONTEXT_BATCH_JOB,
    GlobalExtraLoggingFilter,
    get_logging_config,
    setup_logging,
)
from openeo_driver.utils import EvalEnv
from openeo_driver.views import OPENEO_API_VERSION_DEFAULT
from openeo_driver.workspacerepository import Workspace, WorkspaceRepository, backend_config_workspace_repository
from py4j.protocol import Py4JError, Py4JJavaError
from pyspark import SparkConf, SparkContext
from pyspark.profiler import BasicProfiler
from shapely.geometry import mapping

from openeogeotrellis._version import __version__
from openeogeotrellis.backend import (
    GeoPySparkBackendImplementation,
)
from openeogeotrellis.collect_unique_process_ids_visitor import (
    CollectUniqueProcessIdsVisitor,
)
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.constants import UDF_DEPENDENCIES_INSTALL_MODE
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.deploy import load_custom_processes
from openeogeotrellis.deploy.batch_job_metadata import (
    _assemble_result_metadata,
    _convert_asset_outputs_to_s3_urls,
    _get_tracker_metadata,
    _transform_stac_metadata,
)
from openeogeotrellis.integrations.gdal import get_abs_path_of_asset
from openeogeotrellis.integrations.hadoop import setup_kerberos_auth
from openeogeotrellis.job_options import JobOptions
from openeogeotrellis.stac_save_result import StacSaveResult, get_files_from_stac_catalog
from openeogeotrellis.udf import (
    UdfDependencyHandlingFailure,
    build_python_udf_dependencies_archive,
    collect_python_udf_dependencies,
    install_python_udf_dependencies,
)
from openeogeotrellis.util.runtime import get_job_id
from openeogeotrellis.utils import (
    add_permissions,
    describe_path,
    get_jvm,
    json_default,
    log_memory,
    to_jsonable,
    to_s3_url,
    unzip,
    wait_till_path_available,
    add_permissions_with_failsafe,
    BadlyHashable,
)

logger = logging.getLogger("openeogeotrellis.deploy.batch_job")


OPENEO_LOGGING_THRESHOLD = os.environ.get("OPENEO_LOGGING_THRESHOLD", "INFO")
GlobalExtraLoggingFilter.set("job_id", get_job_id(default="unknown-job"))


def _create_job_dir(job_dir: Path):
    if not ConfigParams().is_kube_deploy:
        if not job_dir.exists():
            logger.error(
                "Expected job dir to exist {j!r} (parent dir: {p}))".format(j=job_dir, p=describe_path(job_dir.parent))
            )
            # Create the directory with read/write/execute permissions for everyone as a fallback.
            ensure_dir(job_dir)
            add_permissions(job_dir, stat.S_IRWXO)
        return
    logger.debug("creating job dir {j!r} (parent dir: {p}))".format(j=job_dir, p=describe_path(job_dir.parent)))
    ensure_dir(job_dir)
    if not get_backend_config().fuse_mount_batchjob_s3_bucket:
        add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, "rt", encoding="utf-8") as f:
        job_specification = json.load(f)

    return job_specification


def _deserialize_dependencies(arg: str) -> List[dict]:
    return json.loads(arg)


def _get_sentinel_hub_credentials_from_spark_conf(conf: SparkConf) -> Optional[Tuple[str, str]]:
    default_client_id = conf.get("spark.openeo.sentinelhub.client.id.default")
    default_client_secret = conf.get("spark.openeo.sentinelhub.client.secret.default")

    return (default_client_id, default_client_secret) if default_client_id and default_client_secret else None


def _get_vault_token(conf: SparkConf) -> Optional[str]:
    return conf.get("spark.openeo.vault.token")


def _get_access_token(conf: SparkConf) -> Optional[str]:
    return conf.get("spark.openeo.access_token")


def main(argv: List[str]) -> None:
    logger.debug(f"batch_job.py argv: {argv}")
    logger.debug(f"batch_job.py {os.getpid()=} {os.getppid()=} {os.getcwd()=}")
    logger.debug(f"batch_job.py version info {get_backend_config().capabilities_deploy_metadata}")
    # TODO: lower log level once dust of 3.11 migration has settled
    logger.info(f"batch_job.py {sys.version=}")

    if len(argv) < 9:
        raise Exception(
            f"usage: {argv[0]} "
            "<job specification input file> <job directory> <results output file name> "
            "<metadata file name> <api version> <dependencies> <user id> <max soft errors ratio> "
            "[Sentinel Hub client alias]"
        )

    job_specification_file = argv[1]
    job_dir = Path(argv[2])
    output_file = job_dir / argv[3]
    metadata_file = job_dir / argv[4]
    api_version = argv[5]
    dependencies = _deserialize_dependencies(argv[6])
    user_id = argv[7]
    GlobalExtraLoggingFilter.set("user_id", user_id)
    max_soft_errors_ratio = float(argv[8])
    sentinel_hub_client_alias = argv[9] if len(argv) >= 10 else None

    _create_job_dir(job_dir)

    # Override default temp dir (under CWD). Original default temp dir `/tmp` might be cleaned up unexpectedly.
    temp_dir = Path(os.getcwd()) / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    logger.debug("Using temp dir {t}".format(t=temp_dir))
    os.environ["TMPDIR"] = str(temp_dir)

    if ConfigParams().is_kube_deploy:
        if not get_backend_config().fuse_mount_batchjob_s3_bucket:
            from openeogeotrellis.utils import s3_client

            bucket = os.environ.get("SWIFT_BUCKET")
            s3_instance = s3_client()

            s3_instance.download_file(bucket, job_specification_file.strip("/"), job_specification_file)

    job_specification = _parse(job_specification_file)
    load_custom_processes()

    conf = (
        SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set(key="spark.kryo.registrator", value="geotrellis.spark.store.kryo.KryoRegistrator")
        .set(
            "spark.kryo.classesToRegister",
            "ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion",
        )
    )

    def context_with_retry(conf):
        retry_counter = 0
        while retry_counter < 5:
            retry_counter += 1
            try:
                return SparkContext(conf=conf)
            except Py4JJavaError as e:
                if retry_counter == 5:
                    raise
                else:
                    logger.info(
                        f"Failed to create SparkContext, retrying {retry_counter} ... {repr(GeoPySparkBackendImplementation.summarize_exception_static(e))}"
                    )

    with context_with_retry(conf) as sc:
        try:
            principal = sc.getConf().get("spark.yarn.principal")
            key_tab = sc.getConf().get("spark.yarn.keytab")

            default_sentinel_hub_credentials = _get_sentinel_hub_credentials_from_spark_conf(sc.getConf())
            vault_token = _get_vault_token(sc.getConf())
            access_token = _get_access_token(sc.getConf())

            if get_backend_config().setup_kerberos_auth:
                setup_kerberos_auth(principal, key_tab)

            def run_driver():
                run_job(
                    job_specification=job_specification,
                    output_file=output_file,
                    metadata_file=metadata_file,
                    api_version=api_version,
                    job_dir=job_dir,
                    dependencies=dependencies,
                    user_id=user_id,
                    max_soft_errors_ratio=max_soft_errors_ratio,
                    default_sentinel_hub_credentials=default_sentinel_hub_credentials,
                    sentinel_hub_client_alias=sentinel_hub_client_alias,
                    vault_token=vault_token,
                    access_token=access_token,
                )

            if sc.getConf().get("spark.python.profile", "false").lower() == "true":
                # Including the driver in the profiling: a bit hacky solution but spark profiler api does not allow passing args&kwargs
                driver_profile = BasicProfiler(sc)
                driver_profile.profile(run_driver)
                # running the driver code and adding driver's profiling results as "RDD==-1"
                sc.profiler_collector.add_profiler(-1, driver_profile)
                # collect profiles into a zip file
                profile_dumps_dir = job_dir / "profile_dumps"
                sc.dump_profiles(profile_dumps_dir)

                profile_zip = shutil.make_archive(
                    base_name=str(profile_dumps_dir), format="gztar", root_dir=profile_dumps_dir
                )
                add_permissions(Path(profile_zip), stat.S_IWGRP)

                shutil.rmtree(
                    profile_dumps_dir,
                    onerror=lambda func, path, exc_info: logger.warning(
                        f"could not recursively delete {profile_dumps_dir}: {func} {path} failed", exc_info=exc_info
                    ),
                )

                logger.info("Saved profiling info to: " + profile_zip)
            else:
                run_driver()
        finally:
            try:
                get_jvm().com.azavea.gdal.GDALWarp.deinit()
            except Py4JError as e:
                if str(e) == "com.azavea.gdal.GDALWarp does not exist in the JVM":
                    logger.debug(f"intentionally swallowing exception {e}", exc_info=True)
                else:
                    raise


@log_memory
def run_job(
    job_specification,
    *,
    output_file: Union[str, Path],
    metadata_file: Union[str, Path],
    api_version: str = OPENEO_API_VERSION_DEFAULT,
    job_dir: Union[str, Path],
    dependencies: Optional[List[dict]] = None,
    user_id: str = None,
    max_soft_errors_ratio: float = 0.0,
    default_sentinel_hub_credentials=None,
    sentinel_hub_client_alias="default",
    vault_token: str = None,
    access_token: str = None,
):
    result_metadata = {}
    tracker_metadata = {}
    items = []
    dependencies = dependencies or []

    # TODO: migrate all raw job option usage to parsed job options
    job_options = job_specification.get("job_options", {})
    parsed_job_options: JobOptions = JobOptions.from_dict(job_options)

    is_stac11 = job_options.get("stac-version", job_options.get("stac-version-experimental", "1.0")) == "1.1"
    omit_derived_from_links = parsed_job_options.omit_derived_from_links or is_stac11

    try:
        # We actually expect type Path, but in reality paths as strings tend to
        # slip in anyway, so we better catch them and convert them.
        output_file = Path(output_file).absolute()
        metadata_file = Path(metadata_file).absolute()
        job_dir = Path(job_dir).absolute()

        logger.info(f"Job spec: {json.dumps(job_specification, indent=1)}")
        logger.debug(f"{job_dir=}, {job_dir=}, {output_file=}, {metadata_file=}")
        process_graph = job_specification["process_graph"]

        try:
            _extract_and_install_udf_dependencies(process_graph=process_graph)
        except UdfDependencyHandlingFailure as e:
            raise e
        except Exception as e:
            raise UdfDependencyHandlingFailure(message=f"Failed extracting/installing UDF dependencies.") from e

        backend_implementation = GeoPySparkBackendImplementation(
            use_job_registry=bool(get_backend_config().ejr_api),
            do_ejr_health_check=False,
        )

        if default_sentinel_hub_credentials is not None:
            backend_implementation.set_default_sentinel_hub_credentials(*default_sentinel_hub_credentials)

        logger.debug(f"Using backend implementation {backend_implementation}")
        correlation_id = get_job_id(default="unknown-job")
        logger.info(f"Correlation id: {correlation_id}")
        env_values = {
            "version": api_version or "1.0.0",
            "pyramid_levels": "highest",
            "user": User(user_id=user_id, internal_auth_data=dict_no_none(access_token=access_token)),
            "require_bounds": True,
            "correlation_id": correlation_id,
            "dependencies": dependencies.copy(),  # will be mutated (popped) during evaluation
            "backend_implementation": backend_implementation,
            "max_soft_errors_ratio": max_soft_errors_ratio,
            "sentinel_hub_client_alias": sentinel_hub_client_alias,
            "vault_token": vault_token,
        }
        job_option_whitelist = [
            "data_mask_optimization",
            "node_caching",
            EVAL_ENV_KEY.ALLOW_EMPTY_CUBES,
            EVAL_ENV_KEY.DO_EXTENT_CHECK,
        ]
        env_values.update({k: job_options[k] for k in job_option_whitelist if k in job_options})
        env = EvalEnv(env_values)
        tracer = DryRunDataTracer()
        logger.debug("Starting process graph evaluation")
        pg_copy = deepcopy(process_graph)
        result = ProcessGraphDeserializer.evaluate(process_graph, env=env, do_dry_run=tracer)
        logger.info("Evaluated process graph, result (type {t}): {r!r}".format(t=type(result), r=result))

        if isinstance(result, DelayedVector):
            geojsons = (mapping(geometry) for geometry in result.geometries_wgs84)
            result = JSONResult(geojsons)

        if isinstance(result, DriverDataCube):
            format_options = job_specification.get("output", {})
            format_options["batch_mode"] = True
            result = ImageCollectionResult(cube=result, format="GTiff", options=format_options)

        if isinstance(result, DriverVectorCube):
            format_options = job_specification.get("output", {})
            format_options["batch_mode"] = True
            result = VectorCubeResult(cube=result, format="GTiff", options=format_options)

        results = result if isinstance(result, List) else [result]
        results = [result if isinstance(result, SaveResult) else JSONResult(result) for result in results]

        global_metadata_attributes = {
            "title": job_specification.get("title", ""),
            "description": job_specification.get("description", ""),
            "institution": f"{get_backend_config().processing_facility} - {get_backend_config().capabilities_backend_version}",
        }

        ml_model_metadata = None

        unique_process_ids = CollectUniqueProcessIdsVisitor().accept_process_graph(process_graph).process_ids

        result_metadata = _assemble_result_metadata(
            tracer=tracer,
            result=results[0],
            job_dir=job_dir,
            unique_process_ids=unique_process_ids,
            apply_gdal=False,
            asset_metadata={},
            ml_model_metadata=ml_model_metadata,
            is_item=is_stac11,
        )
        # perform a first metadata write _before_ actually computing the result. This provides a bit more info, even if the job fails.
        tracker_metadata = _get_tracker_metadata("", omit_derived_from_links=omit_derived_from_links)
        write_metadata({**result_metadata, **tracker_metadata}, metadata_file, is_stac11)

        for result in results:
            result.options["batch_mode"] = True
            result.options["file_metadata"] = {**global_metadata_attributes, **result.options.get("file_metadata", {})}
            if result.options.get("sample_by_feature"):
                geoms = tracer.get_last_geometry("filter_spatial")
                if geoms is None:
                    logger.warning(
                        "sample_by_feature enabled, but no geometries found. "
                        "They can be specified using filter_spatial."
                    )
                else:
                    result.options["geometries"] = geoms
                if result.options.get("geometries") is None:
                    logger.error(
                        "sample_by_feature was set, but no geometries provided through filter_spatial. "
                        "Make sure to provide geometries."
                    )
            if isinstance(result, MlModelResult):
                ml_model_metadata = result.get_model_metadata(str(output_file))
                logger.info("Extracted ml model metadata from %s" % output_file)

        def result_write_assets(result_arg) -> (dict, dict):
            items = result_arg.write_assets(str(output_file))
            if items and "assets" not in next(iter(items.values())):  # no "assets" property so assets themselves
                assets = items
                logger.warning(f"save_result: got an 'assets' object instead of items for {result_arg}")
                # TODO: this is here to avoid having to sync changes with openeo-python-driver
                # it can and should be removed as soon as we have introduced returning items in all SaveResult subclasses
                import uuid

                item_id = str(uuid.uuid4())
                items = {
                    item_id: {
                        "id": item_id,
                        "assets": assets,
                    }
                }

            keys = set()

            def unique_key(asset_id, href):
                # try to make the key unique, and backwards compatible if possible
                if href is not None:
                    try:
                        if str(href).startswith("s3://"):
                            url = urlparse(str(href))
                            temp_key = str(Path(url.path).relative_to(output_file.parent))
                        else:
                            hrefPath = Path(str(href))
                            if hrefPath.is_absolute():
                                temp_key = str(hrefPath.relative_to(Path(str(output_file)).parent))
                            else:
                                temp_key = str(hrefPath)
                    except ValueError as e:
                        url = urlparse(str(href))
                        temp_key = url.path.split("/")[-1]
                else:
                    temp_key = asset_id
                counter = 0
                while temp_key in keys:
                    temp_key = f"{asset_id}_{counter}"
                    counter += 1
                keys.add(temp_key)
                return temp_key

            assets = {
                unique_key(asset_key, asset.get("href", None)): asset
                for item in items.values()
                for asset_key, asset in item.get("assets", {}).items()
            }
            return assets, items

        concurrent_save_results = int(job_options.get("concurrent-save-results", 1))
        if concurrent_save_results == 1:
            assets_metadata, results_items = unzip(*map(result_write_assets, results))
        elif concurrent_save_results > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_save_results) as executor:
                futures = []
                for result in results:
                    futures.append(executor.submit(result_write_assets, result))

                for _ in concurrent.futures.as_completed(futures):
                    continue
            assets_metadata, results_items = unzip(*map(lambda f: f.result(), futures))
        else:
            raise ValueError(f"Invalid concurrent_save_results: {concurrent_save_results}")
        assets_metadata = list(assets_metadata)

        # flattens items for each results into one list
        items = [item for result in results_items for item in result.values()]

        for the_assets_metadata in assets_metadata:
            for name, asset in the_assets_metadata.items():
                href = str(asset["href"])
                url = urlparse(href)
                if url.scheme in ["", "file"]:
                    file_path = url.path
                    # fusemount could have some delay to make files accessible, so poll a bit:
                    asset_path = get_abs_path_of_asset(file_path, job_dir)
                    wait_till_path_available(asset_path)
                add_permissions_with_failsafe(Path(asset["href"]), stat.S_IWGRP)
            logger.info(f"wrote {len(the_assets_metadata)} assets to {output_file}")

        if any(dependency["card4l"] for dependency in dependencies):  # TODO: clean this up
            logger.debug("awaiting Sentinel Hub CARD4L data...")

            s3_service = get_jvm().org.openeo.geotrellissentinelhub.S3Service()

            poll_interval_secs = 10
            max_delay_secs = 600

            card4l_source_locations = [
                dependency["source_location"] for dependency in dependencies if dependency["card4l"]
            ]

            for source_location in set(card4l_source_locations):
                uri_parts = urlparse(source_location)
                bucket_name = uri_parts.hostname
                request_group_id = uri_parts.path[1:]

                try:
                    # TODO: incorporate index to make sure the files don't clash
                    s3_service.download_stac_data(
                        bucket_name, request_group_id, str(job_dir), poll_interval_secs, max_delay_secs
                    )
                    logger.info(
                        "downloaded CARD4L data in {b}/{g} to {d}".format(b=bucket_name, g=request_group_id, d=job_dir)
                    )
                except Py4JJavaError as e:
                    java_exception = e.java_exception

                    if (
                        java_exception.getClass().getName()
                        == "org.openeo.geotrellissentinelhub.S3Service$StacMetadataUnavailableException"
                    ):
                        logger.warning(
                            "could not find CARD4L metadata to download from s3://{b}/{r} after {d}s".format(
                                b=bucket_name, r=request_group_id, d=max_delay_secs
                            )
                        )
                    else:
                        raise e

            _transform_stac_metadata(job_dir)

        # this is subtle: result now points to the last of possibly several results (#295); it corresponds to
        # the terminal save_result node of the process graph
        if "file_metadata" in result.options:
            result.options["file_metadata"]["providers"] = [
                {
                    "name": "VITO",
                    "description": "This data was processed on an openEO backend maintained by VITO.",
                    "roles": ["processor"],
                    "processing:facility": "openEO Geotrellis backend",
                    "processing:software": {"Geotrellis backend": __version__},
                    "processing:expression": {"format": "openeo", "expression": pg_copy},
                }
            ]

        assets_for_result_metadata = {
            item_key: item
            for result_item_metadata in list(results_items)
            for item_key, item in result_item_metadata.items()
        } if is_stac11 else {
            # TODO: flattened instead of per-result, clean this up?
            asset_key: asset_metadata
            for result_assets_metadata in assets_metadata
            for asset_key, asset_metadata in result_assets_metadata.items()
        }
        result_metadata = _assemble_result_metadata(
            tracer=tracer,
            result=result,
            job_dir=job_dir,
            unique_process_ids=unique_process_ids,
            apply_gdal=False,
            asset_metadata=assets_for_result_metadata,
            ml_model_metadata=ml_model_metadata,
            is_item=is_stac11,
        )

        tracker_metadata = _get_tracker_metadata("", omit_derived_from_links=omit_derived_from_links)
        if "sar_backscatter_soft_errors" in tracker_metadata.get("usage", {}):
            soft_errors = tracker_metadata["usage"]["sar_backscatter_soft_errors"]["value"]
            if soft_errors > max_soft_errors_ratio:
                raise ValueError(f"sar_backscatter: Too many soft errors ({soft_errors} > {max_soft_errors_ratio})")

        meta = (
            {**result_metadata, **tracker_metadata, **{"items": items}}
            if is_stac11
            else {**result_metadata, **tracker_metadata}
        )
        write_metadata(meta, metadata_file, is_stac11)
        logger.debug("Starting GDAL-based retrieval of asset metadata")

        assets_for_result_metadata = {
            item_key: item
            for result_item_metadata in list(results_items)
            for item_key, item in result_item_metadata.items()
        } if is_stac11 else {
            # TODO: flattened instead of per-result, clean this up?
            asset_key: asset_metadata
            for result_assets_metadata in assets_metadata
            for asset_key, asset_metadata in result_assets_metadata.items()
        }
        result_metadata = _assemble_result_metadata(
            tracer=tracer,
            result=result,
            job_dir=job_dir,
            unique_process_ids=unique_process_ids,
            apply_gdal=job_options.get("detailed_asset_metadata", True),
            asset_metadata=assets_for_result_metadata,
            ml_model_metadata=ml_model_metadata,
            is_item=is_stac11,
        )

        assert len(results) == len(assets_metadata)
        assert len(results) == len(results_items)
        for result, result_assets_metadata, result_items_metadata in zip(results, assets_metadata, results_items):
            if is_stac11:
                _export_to_workspaces_item(
                    result,
                    result_metadata,
                    result_items_metadata=result_items_metadata,
                    job_dir=job_dir,
                    remove_exported_assets=job_options.get("remove-exported-assets", False),
                    enable_merge=job_options.get("export-workspace-enable-merge", False),
                    omit_derived_from_links=omit_derived_from_links,
                    attach_derived_from_document=is_stac11,
                )
            else:
                _export_to_workspaces(
                    result,
                    result_metadata,
                    result_assets_metadata=result_assets_metadata,
                    job_dir=job_dir,
                    remove_exported_assets=job_options.get("remove-exported-assets", False),
                    enable_merge=job_options.get("export-workspace-enable-merge", False),
                    omit_derived_from_links=omit_derived_from_links,
                )
    finally:
        if len(tracker_metadata) == 0:
            tracker_metadata = _get_tracker_metadata("", omit_derived_from_links=omit_derived_from_links)
        meta = (
            {**result_metadata, **tracker_metadata, **{"items": items}}
            if is_stac11
            else {**result_metadata, **tracker_metadata}
        )
        write_metadata(meta, metadata_file, is_stac11)


def write_metadata(metadata: dict, metadata_file: Path, is_stac11: bool):
    def log_asset_hrefs(context: str):
        if is_stac11:
            items = {item["id"]: item for item in metadata.get("items", [])}
            asset_hrefs =  {item_key + ", " + asset_key: asset.get("href") for item_key, item in items.items() for asset_key, asset in item.get("assets").items() }
            logger.info(f"{context} asset hrefs: {asset_hrefs!r}")
        else:
            asset_hrefs = {asset_key: asset.get("href") for asset_key, asset in metadata.get("assets", {}).items()}
            logger.info(f"{context} asset hrefs: {asset_hrefs!r}")

    log_asset_hrefs("input")
    out_metadata = metadata
    if ConfigParams().is_kube_deploy:
        out_metadata = _convert_asset_outputs_to_s3_urls(metadata)
    log_asset_hrefs("output")

    if is_stac11:
        out_metadata = deepcopy(out_metadata)  # avoid mutating an object that is going to be reused

        for auxiliary_link in _copy_auxiliary_links(
            auxiliary_links=BadlyHashable(out_metadata.get("auxiliary_links", [])),
            job_dir=metadata_file.parent,  # TODO: ugly way to get job_dir
            for_export_workspace=False,
        ):
            for item in out_metadata.get("items", []):
                item.setdefault("links", []).append(auxiliary_link)

    with open(metadata_file, "w") as f:
        json.dump(out_metadata, f, default=json_default)
    add_permissions(metadata_file, stat.S_IWGRP)
    logger.info("wrote metadata to %s" % metadata_file)

    if ConfigParams().is_kube_deploy and not get_backend_config().fuse_mount_batchjob_s3_bucket:
        from openeogeotrellis.utils import s3_client

        bucket = os.environ.get("SWIFT_BUCKET")
        s3_instance = s3_client()

        # asset files are already uploaded by Scala code TODO: this is not generally true e.g. assets generated by Python
        s3_instance.upload_file(str(metadata_file), bucket, str(metadata_file).strip("/"))


@lru_cache
def _copy_auxiliary_links(*, auxiliary_links: BadlyHashable, job_dir: Path, for_export_workspace: bool) -> List[dict]:
    """files should be downloadable from the web app driver"""
    # enforce keyword arguments because distinct argument patterns lead to separate cache entries in lru_cache

    copied_auxiliary_links = []

    for auxiliary_link in auxiliary_links.target:
        auxiliary_file = Path(auxiliary_link["href"])

        # TODO: add proper cross-region support
        if ConfigParams().is_kube_deploy and not for_export_workspace:
            from openeogeotrellis.utils import s3_client

            job_bucket = get_backend_config().s3_bucket_name
            auxiliary_prefix = str(job_dir / auxiliary_file.name).strip("/")
            s3_instance = s3_client()
            s3_instance.upload_file(str(auxiliary_file), job_bucket, auxiliary_prefix)
            downloadable_href = to_s3_url(auxiliary_prefix, job_bucket)
            logger.debug(f"uploaded {auxiliary_file} to {downloadable_href}")
        else:
            downloadable_file = job_dir / auxiliary_file.name
            shutil.copy(auxiliary_file, downloadable_file)
            add_permissions(downloadable_file, stat.S_IWGRP | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            logger.debug(f"copied {auxiliary_file} to {downloadable_file}")
            downloadable_href = f"file://{downloadable_file}"

        copied_auxiliary_links.append(dict(auxiliary_link, href=downloadable_href))

    return copied_auxiliary_links


def _export_to_workspaces_item(
    result: SaveResult,
    result_metadata: dict,
    *,
    result_items_metadata: dict,
    job_dir: Path,
    remove_exported_assets: bool,
    enable_merge: bool,
    omit_derived_from_links: bool,
    attach_derived_from_document: bool,
):
    workspace_repository: WorkspaceRepository = backend_config_workspace_repository
    workspace_exports = sorted(
        list(result.workspace_exports),
        key=lambda export: export.workspace_id + (export.merge or ""),  # arbitrary but deterministic order of hrefs
    )

    if not workspace_exports:
        return

    stac_hrefs = [
        f"file:{path}"
        for path in _write_exported_stac_collection_from_item(
            job_dir,
            result_metadata,
            item_metadata=result_items_metadata,
            omit_derived_from_links=omit_derived_from_links,
            attach_derived_from_document=attach_derived_from_document,
        )
    ]

    # TODO: assemble pystac.STACObject and avoid file altogether?
    collection_href = [href for href in stac_hrefs if "collection.json" in href][0]
    collection = pystac.Collection.from_file(urlparse(collection_href).path)

    workspace_uris = {}

    for i, workspace_export in enumerate(workspace_exports):
        workspace: Workspace = workspace_repository.get_by_id(workspace_export.workspace_id)
        merge = workspace_export.merge

        if merge is None:
            merge = get_job_id(default="unknown-job")
        elif merge == "":  # TODO: puts it in root of workspace? move it there?
            merge = "."

        final_export = i >= len(workspace_exports) - 1
        remove_original = remove_exported_assets and final_export

        if enable_merge or workspace.merges_by_default:
            imported_collection = workspace.merge(collection, target=Path(merge), remove_original=remove_original)
            assert isinstance(imported_collection, pystac.Collection)

            for item in imported_collection.get_items(recursive=True):
                item_key = item.id
                for asset_key, asset in item.get_assets().items():
                    (workspace_uri,) = asset.extra_fields["alternate"].values()
                    workspace_uris.setdefault((item_key,asset_key), []).append(
                        (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                    )
        else:
            export_to_workspace = partial(
                _export_to_workspace,
                common_path=job_dir,
                target=workspace,
                merge=merge,
                remove_original=remove_original,
            )

            for stac_href in stac_hrefs:
                export_to_workspace(source_uri=stac_href)

            for item_key, item in result_items_metadata.items():
                for asset_key, asset in item["assets"].items():
                    workspace_uri = export_to_workspace(source_uri=asset["href"])
                    workspace_uris.setdefault((item_key,asset_key), []).append(
                        (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                    )

    for (item_key,asset_key), workspace_uris in workspace_uris.items():
        if remove_exported_assets:
            # the last workspace URI becomes the public_href; the rest become "alternate" hrefs
            result_metadata["items"][item_key]["assets"][asset_key][BatchJobs.ASSET_PUBLIC_HREF] = workspace_uris[-1][2]
            alternate = {
                f"{workspace_id}/{merge}": {"href": workspace_uri}
                for workspace_id, merge, workspace_uri in workspace_uris[:-1]
            }
        else:
            # the original href still applies; all workspace URIs become "alternate" hrefs
            alternate = {
                f"{workspace_id}/{merge}": {"href": workspace_uri}
                for workspace_id, merge, workspace_uri in workspace_uris
            }

        if alternate:
            result_metadata["items"][item_key]["assets"][asset_key]["alternate"] = alternate


def _export_to_workspaces(
    result: SaveResult,
    result_metadata: dict,
    *,
    result_assets_metadata: dict,
    job_dir: Path,
    remove_exported_assets: bool,
    enable_merge: bool,
    omit_derived_from_links: bool = False,
):
    workspace_repository: WorkspaceRepository = backend_config_workspace_repository
    workspace_exports = sorted(
        list(result.workspace_exports),
        key=lambda export: export.workspace_id + (export.merge or ""),  # arbitrary but deterministic order of hrefs
    )

    if not workspace_exports:
        return

    if isinstance(result, StacSaveResult):
        stac_hrefs_raw = get_files_from_stac_catalog(result.stac_root_local)
        stac_hrefs = [href for href in stac_hrefs_raw if href.endswith(".json")] + [result.stac_root_local]
    else:
        stac_hrefs = [
            f"file:{path}"
            for path in _write_exported_stac_collection(
                job_dir,
                result_metadata,
                asset_keys=list(result_assets_metadata.keys()),
                omit_derived_from_links=omit_derived_from_links,
            )
        ]

    # TODO: assemble pystac.STACObject and avoid file altogether?
    collection_href = [href for href in stac_hrefs if "collection.json" in href][0]
    assert collection_href is not None
    collection = pystac.Collection.from_file(urlparse(collection_href).path)

    workspace_uris = {}

    for i, workspace_export in enumerate(workspace_exports):
        workspace: Workspace = workspace_repository.get_by_id(workspace_export.workspace_id)
        merge = workspace_export.merge

        if merge is None:
            merge = get_job_id(default="unknown-job")
        elif merge == "":  # TODO: puts it in root of workspace? move it there?
            merge = "."

        final_export = i >= len(workspace_exports) - 1
        remove_original = remove_exported_assets and final_export

        if enable_merge or workspace.merges_by_default:
            imported_collection = workspace.merge(collection, target=Path(merge), remove_original=remove_original)
            assert isinstance(imported_collection, pystac.Collection)

            for item in imported_collection.get_items(recursive=True):
                for asset_key, asset in item.get_assets().items():
                    (workspace_uri,) = asset.extra_fields["alternate"].values()
                    workspace_uris.setdefault(asset_key, []).append(
                        (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                    )
        else:
            export_to_workspace = partial(
                _export_to_workspace,
                common_path=job_dir,
                target=workspace,
                merge=merge,
                remove_original=remove_original,
            )

            for stac_href in stac_hrefs:
                # FIXME: collection.json for this result will overwrite the one for another result so
                #  multiple export_workspace to the same workspace and merge within a single process graph will not work
                export_to_workspace(source_uri=stac_href)

            for asset_key, asset in result_assets_metadata.items():
                workspace_uri = export_to_workspace(source_uri=asset["href"])
                workspace_uris.setdefault(asset_key, []).append(
                    (workspace_export.workspace_id, workspace_export.merge, workspace_uri)
                )

    for asset_key, workspace_uris in workspace_uris.items():
        if remove_exported_assets:
            # the last workspace URI becomes the public_href; the rest become "alternate" hrefs
            result_metadata["assets"][asset_key][BatchJobs.ASSET_PUBLIC_HREF] = workspace_uris[-1][2]
            alternate = {
                f"{workspace_id}/{merge}": {"href": workspace_uri}
                for workspace_id, merge, workspace_uri in workspace_uris[:-1]
            }
        else:
            # the original href still applies; all workspace URIs become "alternate" hrefs
            alternate = {
                f"{workspace_id}/{merge}": {"href": workspace_uri}
                for workspace_id, merge, workspace_uri in workspace_uris
            }

        if alternate:
            result_metadata["assets"][asset_key]["alternate"] = alternate


def _export_to_workspace(
    common_path: str, source_uri: Union[str, Path], target: Workspace, merge: str, remove_original: bool
) -> str:
    uri_parts = urlparse(str(source_uri))

    if not uri_parts.scheme or uri_parts.scheme.lower() == "file":
        return target.import_file(common_path, Path(uri_parts.path), merge, remove_original)
    elif uri_parts.scheme == "s3":
        return target.import_object(common_path, source_uri, merge, remove_original)
    else:
        raise ValueError(f"unsupported scheme {uri_parts.scheme} for {source_uri}; supported are: file, s3")


def _write_exported_stac_collection(
    job_dir: Path,
    result_metadata: dict,
    *,
    asset_keys: List[str],
    omit_derived_from_links: bool = False,
) -> List[Path]:  # TODO: change to Set?
    def write_stac_item_file(asset_id: str, asset: dict) -> Path:
        item_file = get_abs_path_of_asset(Path(f"{asset_id}.json"), job_dir)

        properties = {"datetime": asset.get("datetime")}

        if properties["datetime"] is None:
            start_datetime = asset.get("start_datetime") or result_metadata.get("start_datetime") or "1970-01-01T00:00:00Z"
            properties["datetime"] = start_datetime

        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": asset_id,
            "geometry": asset.get("geometry"),
            "bbox": asset.get("bbox"),
            "properties": properties,
            "links": [],  # TODO
            "assets": {
                asset_id: dict_no_none(
                    **{
                        "href": f"{Path(asset['href']).name}",  # relative to possibly nested item file
                        "roles": asset.get("roles"),
                        "type": asset.get("type"),
                        "eo:bands": asset.get("bands"),
                        "raster:bands": to_jsonable(asset.get("raster:bands")),
                    }
                )
            },
        }

        item_file.parent.mkdir(parents=True, exist_ok=True)
        with open(item_file, "wt") as fi:
            json.dump(stac_item, fi, allow_nan=False)

        return item_file

    item_files = [
        write_stac_item_file(asset_key, result_metadata.get("assets", {})[asset_key]) for asset_key in asset_keys
    ]

    def item_link(item_file: Path) -> dict:
        relative_path = item_file.relative_to(job_dir)
        return {
            "href": f"./{relative_path}",
            "rel": "item",
            "type": "application/geo+json",
        }

    job_id = get_job_id(default="unknown-job")

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": job_id,
        "description": f"This is the STAC metadata for the openEO job {job_id!r}",  # TODO
        "license": "unknown",  # TODO
        "extent": {
            "spatial": {"bbox": [result_metadata.get("bbox", [-180, -90, 180, 90])]},
            "temporal": {"interval": [[result_metadata.get("start_datetime"), result_metadata.get("end_datetime")]]},
        },
        "links": (
            [item_link(item_file) for item_file in item_files]
            + [
                link
                for link in _get_tracker_metadata("", omit_derived_from_links=omit_derived_from_links).get("links", [])
                if link["rel"] == "derived_from"
            ]
        ),
    }

    collection_file = job_dir / "collection.json"
    with open(collection_file, "wt") as fc:
        json.dump(stac_collection, fc)

    return item_files + [collection_file]


def _write_exported_stac_collection_from_item(
    job_dir: Path,
    result_metadata: dict,
    *,
    item_metadata: dict,
    omit_derived_from_links: bool,
    attach_derived_from_document: bool,
) -> List[Path]:  # TODO: change to Set?
    job_id = get_job_id(default="unknown-job")

    def write_stac_item_file(item: dict) -> Path:
        assets = dict()
        asset_datetimes = set()
        asset_bboxes = set()
        asset_geometries = set()
        for (asset_key,asset) in item.get("assets").items():
            asset_bands = None
            if "bands" in asset:
                bands = asset["bands"]
                raster_bands = to_jsonable(asset.get("raster:bands",[]))
                asset_bands = list()
                for band in bands:
                    name = band["name"]
                    asset_band = dict_no_none(band)
                    for raster_band in raster_bands:
                        if raster_band["name"] == name:
                            asset_band.update(raster_band)
                    asset_bands.append(asset_band)
            asset_datetimes.add(asset.get("datetime"))
            asset_bboxes.add(asset.get("bbox"))
            asset_geometries.add(json.dumps(asset.get("geometry")))
            assets[asset_key] = dict_no_none({
                "href": f"{Path(urlparse(asset['href']).path).relative_to(job_dir)}",  # relative to top-level item file
                "type": asset.get("type"),
                "roles": asset.get("roles"),
                "bands": asset_bands,
            })
        assert len(asset_datetimes) == 1, asset_datetimes
        assert len(asset_bboxes) == 1, asset_bboxes
        assert len(asset_geometries) == 1, asset_geometries
        stac_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": item["id"],
            "geometry": item.get("geometry"),
            "bbox": item.get("bbox"),
            "properties": item.get("properties", {"datetime": result_metadata.get("start_datetime")}),
            "links": (
                _copy_auxiliary_links(
                    auxiliary_links=BadlyHashable(_get_tracker_metadata("").get("auxiliary_links", [])),
                    job_dir=job_dir,
                    for_export_workspace=True,
                )
                if attach_derived_from_document
                else []
            ),
            "assets": assets,
        }
        item_file = get_abs_path_of_asset(Path(f"{item['id']}.json"), job_dir)
        item_file.parent.mkdir(parents=True, exist_ok=True)
        with open(item_file, "wt") as fi:
            json.dump(stac_item, fi, allow_nan=False)

        return item_file

    item_files = [write_stac_item_file(item) for item in item_metadata.values()]

    derived_from_links = [
        link
        for link in _get_tracker_metadata(
            "", omit_derived_from_links=omit_derived_from_links or attach_derived_from_document
        ).get("links", [])
        if link["rel"] == "derived_from"
    ]

    def item_link(item_file: Path) -> dict:
        relative_path = item_file.relative_to(job_dir)
        return {
            "href": f"./{relative_path}",
            "rel": "item",
            "type": "application/geo+json",
        }

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": job_id,
        "description": f"This is the STAC metadata for the openEO job {job_id!r}",  # TODO
        "license": "unknown",  # TODO
        "extent": {
            "spatial": {"bbox": [result_metadata.get("bbox", [-180, -90, 180, 90])]},
            "temporal": {"interval": [[result_metadata.get("start_datetime"), result_metadata.get("end_datetime")]]},
        },
        "links": [item_link(item_file) for item_file in item_files] + derived_from_links,
    }

    collection_file = job_dir / "collection.json"  # TODO: file is reused for each result
    with open(collection_file, "wt") as fc:
        json.dump(stac_collection, fc)

    return item_files + [collection_file]


def _get_env_var_or_fail(env_var: str) -> str:
    """Get value from env var, but fail hard if it's empty."""
    val = os.environ.get(env_var, "").strip()
    if not val:
        raise RuntimeError(f"Empty env var {env_var!r}")
    return val


def _extract_and_install_udf_dependencies(process_graph: dict):
    udf_dep_map = collect_python_udf_dependencies(process_graph)
    logger.debug(f"Extracted {udf_dep_map=}")
    if len(udf_dep_map) > 1:
        logger.warning("Merging dependencies from multiple UDF runtimes/versions")
    udf_deps = set(d for ds in udf_dep_map.values() for d in ds)
    if udf_deps:

        def sleep_after_udf_dep_setup():
            delay = get_backend_config().udf_dependencies_sleep_after_install
            if delay:
                logger.info(f"Sleeping after UDF dependency setup ({delay}s)")
                time.sleep(delay)

        udf_deps_install_mode = get_backend_config().udf_dependencies_install_mode
        if udf_deps_install_mode == UDF_DEPENDENCIES_INSTALL_MODE.DISABLED:
            raise ValueError("No UDF dependency handling")
        elif udf_deps_install_mode == UDF_DEPENDENCIES_INSTALL_MODE.DIRECT:
            # Install UDF deps directly to target folder
            udf_python_dependencies_folder_path = _get_env_var_or_fail("UDF_PYTHON_DEPENDENCIES_FOLDER_PATH")
            logger.info(f"UDF dep handling with {udf_deps_install_mode=} {udf_python_dependencies_folder_path=}")
            install_python_udf_dependencies(
                dependencies=udf_deps,
                target=udf_python_dependencies_folder_path,
                timeout=20,
                run_context="batch_job.py direct mode",
            )
            sleep_after_udf_dep_setup()
        elif udf_deps_install_mode == UDF_DEPENDENCIES_INSTALL_MODE.ZIP:
            udf_python_dependencies_archive_path = _get_env_var_or_fail("UDF_PYTHON_DEPENDENCIES_ARCHIVE_PATH")
            logger.info(f"UDF dep handling with {udf_deps_install_mode=} {udf_python_dependencies_archive_path=}")
            build_python_udf_dependencies_archive(
                dependencies=udf_deps,
                target=udf_python_dependencies_archive_path,
                # TODO: guess format from file extension (or at least avoid this hardcoding)?
                format="zip",
                timeout=20,
            )
            sleep_after_udf_dep_setup()
        else:
            raise ValueError(f"Unsupported UDF dependencies install mode: {udf_deps_install_mode}")


def start_main():
    setup_logging(
        get_logging_config(
            root_handlers=[LOG_HANDLER_STDERR_JSON if ConfigParams().is_kube_deploy else LOG_HANDLER_FILE_JSON],
            context=LOGGING_CONTEXT_BATCH_JOB,
            root_level=OPENEO_LOGGING_THRESHOLD,
        ),
        capture_unhandled_exceptions=False,  # not needed anymore, as we have a try catch around everything
    )

    try:
        with TimingLogger(f"Starting batch job {os.getpid()=}", logger=logger):
            main(sys.argv)
    except BaseException as e:
        error_summary = GeoPySparkBackendImplementation.summarize_exception_static(e)
        logger.exception("OpenEO batch job failed: " + error_summary.summary)
        raise


if __name__ == "__main__":
    start_main()
