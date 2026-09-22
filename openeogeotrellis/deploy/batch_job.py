import dataclasses
import json
import logging
import os
import shutil
import stat
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from openeo.util import TimingLogger, dict_no_none, ensure_dir
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import SaveResult
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
from openeo_driver.workspacerepository import backend_config_workspace_repository
from py4j.protocol import Py4JError, Py4JJavaError
from pyspark import SparkConf, SparkContext
from pyspark.profiler import BasicProfiler

from openeogeotrellis._version import __version__
from openeogeotrellis.backend import (
    GeoPySparkBackendImplementation,
)
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.config.constants import UDF_DEPENDENCIES_INSTALL_MODE
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.deploy import load_custom_processes
from openeogeotrellis.deploy import batch_job_metadata
from openeogeotrellis.integrations.gdal import localize_s3_asset
from openeogeotrellis.integrations.hadoop import setup_kerberos_auth
from openeogeotrellis.integrations.s3proxy.s3_user_context import should_proxy_be_used
from openeogeotrellis.job_options import JobOptions
from openeogeotrellis.job_results import finalize
from openeogeotrellis.job_results.raster_metadata import get_abs_path_of_asset
from openeogeotrellis.job_results.settings import JobResultsSettings
from openeogeotrellis.load_stac import get_stac_item_collection_filename
from openeogeotrellis.udf import (
    UdfDependencyHandlingFailure,
    build_python_udf_dependencies_archive,
    collect_python_udf_dependencies,
    install_python_udf_dependencies,
)
from openeogeotrellis.util.runtime import get_job_id
from openeogeotrellis.utils import (
    S3ClientBuilder,
    add_permissions,
    add_permissions_with_failsafe,
    describe_path,
    get_jvm,
    log_memory,
    to_s3_url,
    wait_till_path_available,
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
            from openeogeotrellis.utils import S3ClientBuilder

            bucket = os.environ.get("SWIFT_BUCKET")
            s3_instance = S3ClientBuilder.from_bucket(bucket)

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
) -> None:
    dependencies = dependencies or []

    # TODO: migrate all raw job option usage to parsed job options
    job_options = job_specification.get("job_options", {})
    parsed_job_options: JobOptions = JobOptions.from_dict(job_options)

    stac11_mode = parsed_job_options.stac_version == "1.1"
    omit_derived_from_links = parsed_job_options.omit_derived_from_links or stac11_mode
    logger.info(f"{stac11_mode=} {job_options.get('stac-version')=}")

    settings = _build_job_results_settings(
        job_options=job_options,
        stac11_mode=stac11_mode,
        omit_derived_from_links=omit_derived_from_links,
        max_soft_errors_ratio=max_soft_errors_ratio,
    )

    # We actually expect type Path, but in reality paths as strings tend to
    # slip in anyway, so we better catch them and convert them.
    output_file = Path(output_file).absolute()
    metadata_file = Path(metadata_file).absolute()
    job_dir = Path(job_dir).absolute()
    hooks = GeoPySparkJobResultsHooks(job_dir=job_dir, output_file=output_file, dependencies=dependencies)

    try:
        logger.info(f"Job spec: {json.dumps(job_specification, indent=1)}")
        logger.debug(f"{job_dir=}, {output_file=}, {metadata_file=}")
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
            EVAL_ENV_KEY.JOB_DIR: job_dir,
        }
        job_option_whitelist = [
            "data_mask_optimization",
            "node_caching",
            EVAL_ENV_KEY.ALLOW_EMPTY_CUBES,
            EVAL_ENV_KEY.DO_EXTENT_CHECK,
            # TODO: this linking/allow-listing of job options and eval env keys feels quite cumbersome
            EVAL_ENV_KEY.STAC_API_FILTER_BY_GEOMETRY,
        ]
        env_values.update({k: job_options[k] for k in job_option_whitelist if k in job_options})
        env = EvalEnv(env_values)
        tracer = DryRunDataTracer()
        logger.debug("Starting process graph evaluation")
        pg_copy = deepcopy(process_graph)
        settings = dataclasses.replace(
            settings,
            provider={
                "name": "VITO",
                "description": "This data was processed on an openEO backend maintained by VITO.",
                "roles": ["processor"],
                "processing:facility": "openEO Geotrellis backend",
                "processing:software": {"Geotrellis backend": __version__},
                "processing:expression": {"format": "openeo", "expression": pg_copy},
            },
        )
        result = ProcessGraphDeserializer.evaluate(process_graph, env=env, do_dry_run=tracer)
        logger.info("Evaluated process graph, result (type {t}): {r!r}".format(t=type(result), r=result))
    except Exception:
        finalize.write_failure_metadata(metadata_file=metadata_file, settings=settings, hooks=hooks)
        raise

    finalize.finalize_job(
        result,
        tracer=tracer,
        process_graph=pg_copy,
        job_specification=job_specification,
        job_dir=job_dir,
        output_file=output_file,
        metadata_file=metadata_file,
        settings=settings,
        hooks=hooks,
        workspace_repository=backend_config_workspace_repository,
    )


def _build_job_results_settings(
    *, job_options: dict, stac11_mode: bool, omit_derived_from_links: bool, max_soft_errors_ratio: float
) -> JobResultsSettings:
    backend_config = get_backend_config()
    return JobResultsSettings(
        job_id=get_job_id(default="unknown-job"),
        stac11_mode=stac11_mode,
        omit_derived_from_links=omit_derived_from_links,
        detailed_asset_metadata=job_options.get("detailed_asset_metadata", True),
        concurrent_save_results=int(job_options.get("concurrent-save-results", 1)),
        remove_exported_assets=job_options.get("remove-exported-assets", False),
        export_workspace_enable_merge=job_options.get("export-workspace-enable-merge", True),
        max_soft_errors_ratio=max_soft_errors_ratio,
        institution=f"{backend_config.processing_facility} - {backend_config.capabilities_backend_version}",
        processing_facility="VITO - SPARK",  # TODO make configurable
        processing_software="openeo-geotrellis-" + __version__,
        provider={},  # filled in once the process graph is available, see run_job
        job_local_href_format=backend_config.job_local_href_format,
        s3_bucket_name=backend_config.s3_bucket_name,
        gdalinfo_from_file=backend_config.gdalinfo_from_file,
        gdalinfo_use_subprocess=backend_config.gdalinfo_use_subprocess,
        item_collection_glob=get_stac_item_collection_filename(pg_node_id="*"),
    )


class GeoPySparkJobResultsHooks:
    """
    The GeoPySpark-specific half of finalizing a batch job result: the JVM
    tracker, deployment-specific branching (kube vs. YARN, s3proxy, FUSE,
    CARD4L) and GeoPySpark error formatting.

    See `openeogeotrellis.job_results.settings.JobResultsHooks`.
    """

    def __init__(self, *, job_dir: Path, output_file: Path, dependencies: List[dict]):
        self._job_dir = job_dir
        self._output_file = output_file
        self._dependencies = dependencies
        self._is_kube_deploy = ConfigParams().is_kube_deploy
        self._fuse_mount_batchjob_s3_bucket = get_backend_config().fuse_mount_batchjob_s3_bucket
        self._swift_bucket = os.environ.get("SWIFT_BUCKET")

    def result_grid(self, result: SaveResult):
        return batch_job_metadata.result_grid(result)

    def summarize_exception(self, e: Exception) -> str:
        return batch_job_metadata.summarize_exception(e)

    def usage_metadata(self, *, omit_derived_from_links: bool = False) -> dict:
        return batch_job_metadata.get_tracker_metadata("", omit_derived_from_links=omit_derived_from_links)

    def prepare_result_options(self, result: SaveResult) -> None:
        result.options["use_s3proxy"] = should_proxy_be_used()
        result.options["s3_bucket"] = self._swift_bucket
        if result.options["use_s3proxy"]:
            if not result.options["s3_bucket"]:
                logger.warning("use_s3proxy is active but no S3 bucket is configured; disabling use_s3proxy")
                result.options["use_s3proxy"] = False
            else:
                result.options["s3_client"] = S3ClientBuilder.from_bucket(result.options["s3_bucket"])

    def after_assets_written(self, assets_metadata: List[dict], job_dir: Path) -> None:
        for asset in assets_metadata:
            href = str(asset["href"])
            url = urlparse(href)
            if url.scheme in ["", "file"]:
                # fusemount could have some delay to make files accessible, so poll a bit:
                asset_path = get_abs_path_of_asset(url.path, job_dir)
                wait_till_path_available(asset_path)
            add_permissions_with_failsafe(Path(asset["href"]), stat.S_IWGRP)
        logger.info(f"wrote {len(assets_metadata)} assets to {self._output_file}")

        if any(dependency["card4l"] for dependency in self._dependencies):  # TODO: clean this up
            logger.debug("awaiting Sentinel Hub CARD4L data...")

            s3_service = get_jvm().org.openeo.geotrellissentinelhub.S3Service()

            poll_interval_secs = 10
            max_delay_secs = 600

            card4l_source_locations = [
                dependency["source_location"] for dependency in self._dependencies if dependency["card4l"]
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

            batch_job_metadata.transform_stac_metadata(job_dir)

    def localize_asset(self, href: str, job_dir: Path) -> Optional[Path]:
        return localize_s3_asset(href, job_dir)

    def output_href(self, href: str) -> str:
        if self._is_kube_deploy:
            return to_s3_url(str(href).strip("/"))
        return href

    def publish_metadata_file(self, metadata_file: Path) -> None:
        add_permissions(metadata_file, stat.S_IWGRP)
        if self._is_kube_deploy and not self._fuse_mount_batchjob_s3_bucket:
            s3_instance = S3ClientBuilder.from_bucket(self._swift_bucket)
            # asset files are already uploaded by Scala code TODO: this is not generally true e.g. assets generated by Python
            s3_instance.upload_file(str(metadata_file), self._swift_bucket, str(metadata_file).strip("/"))

    def publish_auxiliary_file(self, path: Path, job_dir: Path, *, for_export_workspace: bool) -> str:
        """files should be downloadable from the web app driver"""
        # TODO: add proper cross-region support
        if self._is_kube_deploy and not for_export_workspace:
            job_bucket = get_backend_config().s3_bucket_name
            auxiliary_prefix = str(job_dir / path.name).strip("/")
            s3_instance = S3ClientBuilder.from_bucket(job_bucket)
            s3_instance.upload_file(str(path), job_bucket, auxiliary_prefix)
            downloadable_href = to_s3_url(auxiliary_prefix, job_bucket)
            logger.debug(f"uploaded {path} to {downloadable_href}")
        else:
            downloadable_file = job_dir / path.name
            shutil.copy(path, downloadable_file)
            add_permissions(downloadable_file, stat.S_IWGRP | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            logger.debug(f"copied {path} to {downloadable_file}")
            downloadable_href = f"file://{downloadable_file}"
        return downloadable_href


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
