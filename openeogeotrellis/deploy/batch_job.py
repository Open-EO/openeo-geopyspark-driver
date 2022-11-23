import json
import logging
import os
import shutil
import stat
import sys
from itertools import chain
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, SparkConf
from pyspark.profiler import BasicProfiler
from shapely.geometry import mapping, Polygon
from shapely.geometry.base import BaseGeometry

from openeo.util import ensure_dir, Rfc3339, TimingLogger, dict_no_none, repr_truncate
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.datacube import DriverDataCube, DriverMlModel, DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import ImageCollectionResult, JSONResult, MultipleFilesResult, SaveResult, NullResult, \
    MlModelResult
from openeo_driver.users import User
from openeo_driver.util.geometry import spatial_extent_union
from openeo_driver.util.logging import BatchJobLoggingFilter, user_id_trim, get_logging_config, setup_logging, \
    LOGGING_CONTEXT_BATCH_JOB
from openeo_driver.util.utm import area_in_square_meters
from openeo_driver.utils import EvalEnv, temporal_extent_union, generate_unique_id
from openeogeotrellis._version import __version__
from openeogeotrellis.backend import JOB_METADATA_FILENAME, GeoPySparkBackendImplementation
from openeogeotrellis.collect_unique_process_ids_visitor import CollectUniqueProcessIdsVisitor
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.deploy import load_custom_processes, build_gps_backend_deploy_metadata
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import kerberos, describe_path, log_memory, get_jvm, add_permissions, mdc_include

logger = logging.getLogger('openeogeotrellis.deploy.batch_job')
user_facing_logger = logging.getLogger('openeo-user-log')


OPENEO_BATCH_JOB_ID = os.environ.get("OPENEO_BATCH_JOB_ID","unknown-job")
# TODO: also trim batch_job id a bit before logging?
BatchJobLoggingFilter.set("job_id", OPENEO_BATCH_JOB_ID)


def _setup_user_logging(log_file: Path) -> None:
    file_handler = None
    if str(log_file.name) == "stdout":
        file_handler = logging.StreamHandler(sys.stdout)
    else:
        file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.ERROR)

    user_facing_logger.addHandler(file_handler)

    add_permissions(log_file, stat.S_IWGRP)


def _setup_java_logging(sc: SparkContext, user_id: str):
    jvm = get_jvm()
    mdc_include(sc, jvm, jvm.org.openeo.logging.JsonLayout.UserId(), user_id)
    mdc_include(sc, jvm, jvm.org.openeo.logging.JsonLayout.JobId(), OPENEO_BATCH_JOB_ID)


def _create_job_dir(job_dir: Path):
    logger.info("creating job dir {j!r} (parent dir: {p}))".format(j=job_dir, p=describe_path(job_dir.parent)))
    ensure_dir(job_dir)
    if not ConfigParams().is_kube_deploy:
        try:
            shutil.chown(job_dir, user=None, group='eodata')
        except LookupError as e:
            logger.warning(f"Could not change group of {job_dir} to eodata.")

    add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'rt', encoding='utf-8') as f:
        job_specification = json.load(f)

    return job_specification


def extract_result_metadata(tracer: DryRunDataTracer) -> dict:
    logger.info("Extracting result metadata from {t!r}".format(t=tracer))

    rfc3339 = Rfc3339(propagate_none=True)

    source_constraints = tracer.get_source_constraints()

    # Take union of extents
    temporal_extent = temporal_extent_union(*[
        sc["temporal_extent"] for _, sc in source_constraints if "temporal_extent" in sc
    ])
    extents = [sc["spatial_extent"] for _, sc in source_constraints if "spatial_extent" in sc]
    if(len(extents) > 0):
        spatial_extent = spatial_extent_union(*extents)
        bbox = [spatial_extent[b] for b in ["west", "south", "east", "north"]]
        if all(b is not None for b in bbox):
            polygon = Polygon.from_bounds(*bbox)
            geometry = mapping(polygon)
            area = area_in_square_meters(polygon, spatial_extent["crs"])
        else:
            bbox = None
            geometry = None
            area = None
    else:
        bbox = None
        geometry = None
        area = None


    start_date, end_date = [rfc3339.datetime(d) for d in temporal_extent]

    aggregate_spatial_geometries = tracer.get_geometries()  # TODO: consider "filter_spatial" geometries too?
    if aggregate_spatial_geometries:
        if len(aggregate_spatial_geometries) > 1:
            logger.warning("Multiple aggregate_spatial geometries: {c}".format(c=len(aggregate_spatial_geometries)))
        agg_geometry = aggregate_spatial_geometries[0]
        if isinstance(agg_geometry, BaseGeometry):
            bbox = agg_geometry.bounds
            geometry = mapping(agg_geometry)
            area = area_in_square_meters(agg_geometry, "EPSG:4326")
        elif isinstance(agg_geometry, DelayedVector):
            bbox = agg_geometry.bounds
            # Intentionally don't return the complete vector file. https://github.com/Open-EO/openeo-api/issues/339
            geometry = mapping(Polygon.from_bounds(*bbox))
            area = agg_geometry.area
        elif isinstance(agg_geometry, DriverVectorCube):
            bbox = agg_geometry.get_bounding_box()
            geometry = agg_geometry.get_bounding_box_geojson()
            area = agg_geometry.get_bounding_box_area()
        else:
            logger.warning(f"Result metadata: no bbox/area support for {type(agg_geometry)}")

    links = tracer.get_metadata_links()
    links = [link for k, v in links.items() for link in v]

    # TODO: dedicated type?
    # TODO: match STAC format?
    return {
        'geometry': geometry,
        'bbox': bbox,
        'area': {'value': area, 'unit': 'square meter'} if area else None,
        'start_datetime': start_date,
        'end_datetime': end_date,
        'links': links
    }


def _export_result_metadata(tracer: DryRunDataTracer, result: SaveResult, output_file: Path, metadata_file: Path,
                            unique_process_ids: Set[str], asset_metadata: Dict = None,
                            ml_model_metadata: Dict = None) -> None:
    metadata = extract_result_metadata(tracer)

    def epsg_code(gps_crs) -> Optional[int]:
        crs = get_jvm().geopyspark.geotrellis.TileLayer.getCRS(gps_crs)
        return crs.get().epsgCode().getOrElse(None) if crs.isDefined() else None

    bands = []
    if isinstance(result, GeopysparkDataCube):
        if result.cube.metadata.has_band_dimension():
            bands = result.metadata.bands
        max_level = result.pyramid.levels[result.pyramid.max_zoom]
        nodata = max_level.layer_metadata.no_data_value
        epsg = epsg_code(max_level.layer_metadata.crs)
        instruments = result.metadata.get("summaries", "instruments", default=[])
    elif isinstance(result, ImageCollectionResult) and isinstance(result.cube, GeopysparkDataCube):
        if result.cube.metadata.has_band_dimension():
            bands = result.cube.metadata.bands
        max_level = result.cube.pyramid.levels[result.cube.pyramid.max_zoom]
        nodata = max_level.layer_metadata.no_data_value
        epsg = epsg_code(max_level.layer_metadata.crs)
        instruments = result.cube.metadata.get("summaries", "instruments", default=[])
    else:
        bands = []
        nodata = None
        epsg = None
        instruments = []

    if not isinstance(result, NullResult):
        if asset_metadata == None:
            #old approach: need to construct metadata ourselves, from inspecting SaveResult
            metadata['assets'] = {
                output_file.name: {
                    'bands': bands,
                    'nodata': nodata,
                    'type': result.get_mimetype()
                }
            }
        else:
            #new approach: SaveResult has generated metadata already for us
            metadata['assets'] = asset_metadata

    metadata['epsg'] = epsg
    metadata['instruments'] = instruments
    metadata['processing:facility'] = 'VITO - SPARK'#TODO make configurable
    metadata['processing:software'] = 'openeo-geotrellis-' + __version__
    metadata['unique_process_ids'] = list(unique_process_ids)
    metadata = {**metadata, **_get_tracker_metadata("")}

    if ml_model_metadata is not None:
        metadata['ml_model_metadata'] = ml_model_metadata

    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)

    add_permissions(metadata_file, stat.S_IWGRP)

    logger.info("wrote metadata to %s" % metadata_file)

def _get_tracker(tracker_id=""):
    return get_jvm().org.openeo.geotrelliscommon.BatchJobMetadataTracker.tracker(tracker_id)

def _get_tracker_metadata(tracker_id="") -> dict:
    tracker = _get_tracker(tracker_id)
    t = tracker
    if(t is not None):
        tracker_results = t.asDict()
        pu = tracker_results.get("Sentinelhub_Processing_Units",None)
        usage = None
        if pu is not None:
            usage = {"sentinelhub":{"value":pu,"unit":"sentinelhub_processing_unit"}}

        pixels = tracker_results.get("InputPixels", None)
        if pixels is not None:
            usage = {"input_pixel":{"value":pixels/(1024*1024),"unit":"mega-pixel"}}

        links = tracker_results.get("links", None)
        all_links = None
        if links is not None:
            all_links = list(chain(*links.values()))
            all_links = [ {"href":link, "rel":"derived_from"} for link in all_links]

        return dict_no_none(usage=usage,links=all_links)


def _deserialize_dependencies(arg: str) -> List[dict]:
    return json.loads(arg)


def _get_sentinel_hub_credentials_from_spark_conf(conf: SparkConf) -> (str, str):
    return (conf.get('spark.openeo.sentinelhub.client.id.default'),
            conf.get('spark.openeo.sentinelhub.client.secret.default'))


def _get_vault_token(conf: SparkConf) -> Optional[str]:
    return conf.get('spark.openeo.vault.token')


def main(argv: List[str]) -> None:
    logger.info("batch_job.py argv: {a!r}".format(a=argv))
    logger.info("batch_job.py pid {p}; ppid {pp}; cwd {c}".format(p=os.getpid(), pp=os.getppid(), c=os.getcwd()))
    logger.warning("batch_job.py version info %r", build_gps_backend_deploy_metadata(
        packages=[
            "openeo",
            "openeo_driver",
            "openeo-geopyspark",
            # TODO list more packages like in openeo-deploy?
        ],
    ))

    if len(argv) < 10:
        raise Exception(
            f"usage: {argv[0]} "
            "<job specification input file> <job directory> <results output file name> <user log file name> "
            "<metadata file name> <api version> <dependencies> <user id> <max soft errors ratio> "
            "[Sentinel Hub client alias]"
        )

    job_specification_file = argv[1]
    job_dir = Path(argv[2])
    output_file = job_dir / argv[3]
    log_file = job_dir / argv[4]
    metadata_file = job_dir / argv[5]
    api_version = argv[6]
    dependencies = _deserialize_dependencies(argv[7])
    user_id = argv[8]
    BatchJobLoggingFilter.set("user_id", user_id)
    max_soft_errors_ratio = float(argv[9])
    sentinel_hub_client_alias = argv[10] if len(argv) >= 11 else None

    _create_job_dir(job_dir)

    _setup_user_logging(log_file)

    # Override default temp dir (under CWD). Original default temp dir `/tmp` might be cleaned up unexpectedly.
    temp_dir = Path(os.getcwd()) / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Using temp dir {t}".format(t=temp_dir))
    os.environ["TMPDIR"] = str(temp_dir)

    try:
        if ConfigParams().is_kube_deploy:
            from openeogeotrellis.utils import s3_client

            bucket = os.environ.get('SWIFT_BUCKET')
            s3_instance = s3_client()

            s3_instance.download_file(bucket, job_specification_file.strip("/"), job_specification_file )


        job_specification = _parse(job_specification_file)
        load_custom_processes()

        conf = (SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set(key='spark.kryo.registrator', value='geopyspark.geotools.kryo.ExpandedKryoRegistrator')
                .set("spark.kryo.classesToRegister", "org.openeo.geotrellisaccumulo.SerializableConfiguration,ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion"))

        with SparkContext(conf=conf) as sc:
            _setup_java_logging(sc, user_id)

            principal = sc.getConf().get("spark.yarn.principal")
            key_tab = sc.getConf().get("spark.yarn.keytab")

            default_sentinel_hub_credentials = _get_sentinel_hub_credentials_from_spark_conf(sc.getConf())
            vault_token = _get_vault_token(sc.getConf())

            kerberos(principal, key_tab)
            
            def run_driver(): 
                run_job(
                    job_specification=job_specification, output_file=output_file, metadata_file=metadata_file,
                    api_version=api_version, job_dir=job_dir, dependencies=dependencies, user_id=user_id,
                    max_soft_errors_ratio=max_soft_errors_ratio,
                    default_sentinel_hub_credentials=default_sentinel_hub_credentials,
                    sentinel_hub_client_alias=sentinel_hub_client_alias, vault_token=vault_token
                )
            
            if sc.getConf().get('spark.python.profile', 'false').lower() == 'true':
                # Including the driver in the profiling: a bit hacky solution but spark profiler api does not allow passing args&kwargs
                driver_profile = BasicProfiler(sc)
                driver_profile.profile(run_driver)
                # running the driver code and adding driver's profiling results as "RDD==-1"
                sc.profiler_collector.add_profiler(-1, driver_profile)
                # collect profiles into a zip file
                profile_dumps_dir = job_dir / 'profile_dumps'
                sc.dump_profiles(profile_dumps_dir)

                profile_zip = shutil.make_archive(base_name=str(profile_dumps_dir), format='gztar',
                                                  root_dir=profile_dumps_dir)
                add_permissions(Path(profile_zip), stat.S_IWGRP)

                shutil.rmtree(profile_dumps_dir,
                              onerror=lambda func, path, exc_info:
                              logger.warning(f"could not recursively delete {profile_dumps_dir}: {func} {path} failed",
                                             exc_info=exc_info))

                logger.info("Saved profiling info to: " + profile_zip)
            else:
                run_driver()

    except Exception as e:
        logger.exception(f"Error processing batch job: {repr_truncate(e, width=200)}")
        user_facing_logger.exception(f"Error processing batch job: {repr_truncate(e, width=200)}")
        if "Container killed on request. Exit code is 143" in str(e):
            user_facing_logger.error("Your batch job failed because workers used too much Python memory. The same task was attempted multiple times. Consider increasing executor-memoryOverhead or contact the developers to investigate.")
        raise e


@log_memory
def run_job(job_specification, output_file: Path, metadata_file: Path, api_version, job_dir, dependencies: List[dict],
            user_id: str = None, max_soft_errors_ratio: float = 0.0, default_sentinel_hub_credentials=None,
            sentinel_hub_client_alias='default', vault_token: str = None):
    logger.info(f"Job spec: {json.dumps(job_specification,indent=1)}")
    process_graph = job_specification['process_graph']
    job_options = job_specification.get("job_options", {})

    backend_implementation = GeoPySparkBackendImplementation()

    if default_sentinel_hub_credentials is not None:
        backend_implementation.set_default_sentinel_hub_credentials(*default_sentinel_hub_credentials)

    logger.info(f"Using backend implementation {backend_implementation}")
    correlation_id = generate_unique_id(prefix="c")
    logger.info(f"Correlation id: {correlation_id}")
    env_values = {
        'version': api_version or "1.0.0",
        'pyramid_levels': 'highest',
        'user': User(user_id=user_id),
        'require_bounds': True,
        'correlation_id': correlation_id,
        'dependencies': dependencies.copy(),  # will be mutated (popped) during evaluation
        'backend_implementation': backend_implementation,
        'max_soft_errors_ratio': max_soft_errors_ratio,
        'sentinel_hub_client_alias': sentinel_hub_client_alias,
        'vault_token': vault_token
    }
    job_option_whitelist = [
        "data_mask_optimization",
    ]
    env_values.update({k: job_options[k] for k in job_option_whitelist if k in job_options})
    env = EvalEnv(env_values)
    tracer = DryRunDataTracer()
    logger.info("Starting process graph evaluation")
    result = ProcessGraphDeserializer.evaluate(process_graph, env=env, do_dry_run=tracer)
    logger.info("Evaluated process graph, result (type {t}): {r!r}".format(t=type(result), r=result))

    if isinstance(result, DelayedVector):
        geojsons = (mapping(geometry) for geometry in result.geometries)
        result = JSONResult(geojsons)

    if isinstance(result, DriverDataCube):
        format_options = job_specification.get('output', {})
        format_options["batch_mode"] = True
        result = ImageCollectionResult(cube=result, format='GTiff', options=format_options)

    if not isinstance(result, SaveResult):  # Assume generic JSON result
        result = JSONResult(result)

    global_metadata_attributes = {
        "title" : job_specification.get("title",""),
        "description": job_specification.get("description", ""),
        "institution": "openEO platform - Geotrellis backend: " + __version__

    }

    assets_metadata = None
    ml_model_metadata = None
    if('write_assets' in dir(result)):
        result.options["batch_mode"] = True
        result.options["file_metadata"] = global_metadata_attributes
        if( result.options.get("sample_by_feature")):
            geoms = tracer.get_geometries("filter_spatial")
            if len(geoms) > 1:
                logger.warning("Multiple aggregate_spatial geometries: {c}".format(c=len(geoms)))
            elif len(geoms) == 0:
                logger.warning("sample_by_feature enabled, but no geometries found. They can be specified using filter_spatial.")
            else:
                result.options["geometries"] = geoms[0]
            if(result.options["geometries"] == None):
                logger.error("samply_by_feature was set, but no geometries provided through filter_spatial. Make sure to provide geometries.")
        assets_metadata = result.write_assets(str(output_file))
        if isinstance(result, MlModelResult):
            ml_model_metadata = result.get_model_metadata(str(output_file))
            logger.info("Extracted ml model metadata from %s" % output_file)
        for name,asset in assets_metadata.items():
            add_permissions(Path(asset["href"]), stat.S_IWGRP)
        logger.info(f"wrote {len(assets_metadata)} assets to {output_file}")
    elif isinstance(result, ImageCollectionResult):
        result.options["batch_mode"] = True
        result.save_result(filename=str(output_file))
        add_permissions(output_file, stat.S_IWGRP)
        logger.info("wrote image collection to %s" % output_file)
    elif isinstance(result, MultipleFilesResult):
        result.reduce(output_file, delete_originals=True)
        add_permissions(output_file, stat.S_IWGRP)
        logger.info("reduced %d files to %s" % (len(result.files), output_file))
    elif isinstance(result, NullResult):
        logger.info("skipping output file %s" % output_file)
    else:
        raise NotImplementedError("unsupported result type {r}".format(r=type(result)))

    if any(dependency['card4l'] for dependency in dependencies):  # TODO: clean this up
        logger.debug("awaiting Sentinel Hub CARD4L data...")

        s3_service = get_jvm().org.openeo.geotrellissentinelhub.S3Service()

        poll_interval_secs = 10
        max_delay_secs = 600

        card4l_source_locations = [dependency['source_location'] for dependency in dependencies if dependency['card4l']]

        for source_location in set(card4l_source_locations):
            uri_parts = urlparse(source_location)
            bucket_name = uri_parts.hostname
            request_group_id = uri_parts.path[1:]

            try:
                # TODO: incorporate index to make sure the files don't clash
                s3_service.download_stac_data(bucket_name, request_group_id, str(job_dir), poll_interval_secs,
                                              max_delay_secs)
                logger.info("downloaded CARD4L data in {b}/{g} to {d}"
                            .format(b=bucket_name, g=request_group_id, d=job_dir))
            except Py4JJavaError as e:
                java_exception = e.java_exception

                if (java_exception.getClass().getName() ==
                        'org.openeo.geotrellissentinelhub.S3Service$StacMetadataUnavailableException'):
                    logger.warning("could not find CARD4L metadata to download from s3://{b}/{r} after {d}s"
                                   .format(b=bucket_name, r=request_group_id, d=max_delay_secs))
                else:
                    raise e

        _transform_stac_metadata(job_dir)

    unique_process_ids = CollectUniqueProcessIdsVisitor().accept_process_graph(process_graph).process_ids

    _export_result_metadata(tracer=tracer, result=result, output_file=output_file, metadata_file=metadata_file,
                            unique_process_ids=unique_process_ids, asset_metadata=assets_metadata,
                            ml_model_metadata=ml_model_metadata)

    if ConfigParams().is_kube_deploy:
        import boto3
        from openeogeotrellis.utils import s3_client

        bucket = os.environ.get('SWIFT_BUCKET')
        s3_instance = s3_client()

        logger.info("Writing results to object storage")
        for file in os.listdir(job_dir):
            full_path = str(job_dir) + "/" + file
            s3_instance.upload_file(full_path, bucket, full_path.strip("/"))


def _transform_stac_metadata(job_dir: Path):
    def relativize(assets: dict) -> dict:
        def relativize_href(asset: dict) -> dict:
            absolute_href = asset['href']
            relative_path = urlparse(absolute_href).path.split("/")[-1]
            return dict(asset, href=relative_path)

        return {asset_name: relativize_href(asset) for asset_name, asset in assets.items()}

    def drop_links(metadata: dict) -> dict:
        result = metadata.copy()
        result.pop('links', None)
        return result

    stac_metadata_files = [job_dir / file_name for file_name in os.listdir(job_dir) if
                           file_name.endswith("_metadata.json") and file_name != JOB_METADATA_FILENAME]

    for stac_metadata_file in stac_metadata_files:
        with open(stac_metadata_file, 'rt', encoding='utf-8') as f:
            stac_metadata = json.load(f)

        relative_assets = relativize(stac_metadata.get('assets', {}))
        transformed = dict(drop_links(stac_metadata), assets=relative_assets)

        with open(stac_metadata_file, 'wt', encoding='utf-8') as f:
            json.dump(transformed, f, indent=2)


if __name__ == '__main__':
    handler = "stderr_json" if ConfigParams().is_kube_deploy else "file_json"
    setup_logging(get_logging_config(
        root_handlers=[handler],
        loggers={
            "openeo": {"level": "DEBUG"},
            "openeo_driver": {"level": "DEBUG"},
            "openeogeotrellis": {"level": "DEBUG"},
            "kazoo": {"level": "WARN"},
            "cropsar": {"level": "DEBUG"},
        },
        context=LOGGING_CONTEXT_BATCH_JOB))

    with TimingLogger("batch_job.py main", logger=logger):
        main(sys.argv)
