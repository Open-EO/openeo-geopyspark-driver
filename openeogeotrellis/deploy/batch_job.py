import json
import logging
import os
import shutil
import stat
import sys
from urllib.parse import urlparse
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, SparkConf
from pyspark.profiler import BasicProfiler
from shapely.geometry import mapping, Polygon
from shapely.geometry.base import BaseGeometry

from openeo.util import ensure_dir, Rfc3339, TimingLogger
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.datacube import DriverDataCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import ImageCollectionResult, JSONResult, MultipleFilesResult, SaveResult, NullResult
from openeo_driver.users import User
from openeo_driver.utils import EvalEnv, spatial_extent_union, temporal_extent_union, to_hashable
from openeogeotrellis.backend import JOB_METADATA_FILENAME, GeoPySparkBackendImplementation
from openeogeotrellis.deploy import load_custom_processes
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import kerberos, describe_path, log_memory, get_jvm
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis._version import __version__

LOG_FORMAT = '%(asctime)s:P%(process)s:%(levelname)s:%(name)s:%(message)s'

logger = logging.getLogger('openeogeotrellis.deploy.batch_job')
user_facing_logger = logging.getLogger('openeo-user-log')


def _setup_app_logging() -> None:
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    # TODO: logs show up twice in the output
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)

    logging.getLogger("openeo").setLevel(logging.INFO)
    logging.getLogger("openeo").addHandler(console_handler)
    logging.getLogger("openeo_driver").setLevel(logging.INFO)
    logging.getLogger("openeo_driver").addHandler(console_handler)
    logging.getLogger("openeogeotrellis").setLevel(logging.INFO)
    logging.getLogger("openeogeotrellis").addHandler(console_handler)


def _setup_user_logging(log_file: Path) -> None:
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.ERROR)

    user_facing_logger.addHandler(file_handler)

    _add_permissions(log_file, stat.S_IWGRP)


def _create_job_dir(job_dir: Path):
    logger.info("creating job dir {j!r} (parent dir: {p}))".format(j=job_dir, p=describe_path(job_dir.parent)))
    ensure_dir(job_dir)
    if not ConfigParams().is_kube_deploy:
        shutil.chown(job_dir, user=None, group='eodata')

    _add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group


def _add_permissions(path: Path, mode: int):
    # FIXME: maybe umask is a better/cleaner option
    if path.exists():
        current_permission_bits = os.stat(path).st_mode
        os.chmod(path, current_permission_bits | mode)
    else:
        for p in path.parent.glob('*'):
            current_permission_bits = os.stat(p).st_mode
            p.chmod(current_permission_bits | mode)


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
            geometry = mapping(Polygon.from_bounds(*bbox))
        else:
            bbox = None
            geometry = None
    else:
        bbox = None
        geometry = None


    start_date, end_date = [rfc3339.datetime(d) for d in temporal_extent]

    aggregate_spatial_geometries = tracer.get_geometries()
    if aggregate_spatial_geometries:
        if len(aggregate_spatial_geometries) > 1:
            logger.warning("Multiple aggregate_spatial geometries: {c}".format(c=len(aggregate_spatial_geometries)))
        agg_geometry = aggregate_spatial_geometries[0]
        if isinstance(agg_geometry, BaseGeometry):
            bbox = agg_geometry.bounds
            geometry = mapping(agg_geometry)
        elif isinstance(agg_geometry, DelayedVector):
            bbox = agg_geometry.bounds
            # Intentionally don't return the complete vector file. https://github.com/Open-EO/openeo-api/issues/339
            geometry = mapping(Polygon.from_bounds(*bbox))

    links = tracer.get_metadata_links()
    links = [ link for k,v in links.items() for link in v ]
    # TODO: dedicated type?
    # TODO: match STAC format?
    return {
        'geometry': geometry,
        'bbox': bbox,
        'start_datetime': start_date,
        'end_datetime': end_date,
        'links':links
    }


def _export_result_metadata(tracer: DryRunDataTracer, result: SaveResult, output_file: Path, metadata_file: Path,asset_metadata:Dict = None) -> None:
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
                    'media_type': result.get_mimetype()
                }
            }
        else:
            #new approach: SaveResult has generated metadata already for us
            metadata['assets'] = asset_metadata

    metadata['epsg'] = epsg
    metadata['instruments'] = instruments
    metadata['processing:facility'] = 'VITO - SPARK'#TODO make configurable
    metadata['processing:software'] = 'openeo-geotrellis-' + __version__

    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)

    _add_permissions(metadata_file, stat.S_IWGRP)

    logger.info("wrote metadata to %s" % metadata_file)


def _deserialize_dependencies(arg: str) -> dict:  # (collection_id, metadata_properties) -> (subfolder, card4l)
    if not arg or arg == 'no_dependencies':  # TODO: clean this up
        return {}

    def deserialize_properties(s: str) -> List[tuple]:
        if not s:
            return []

        def deserialize_property(p: str) -> tuple:
            return tuple(p.split("="))

        return [deserialize_property(p) for p in s.split("&")]

    tuples = [t.split(":") for t in arg.split(",")]
    return {(t[0], to_hashable(deserialize_properties(t[1]))): (t[2], t[3] == "True") for t in tuples}


def main(argv: List[str]) -> None:
    logger.info("argv: {a!r}".format(a=argv))
    logger.info("pid {p}; ppid {pp}; cwd {c}".format(p=os.getpid(), pp=os.getppid(), c=os.getcwd()))

    if len(argv) < 6:
        print("usage: %s "
              "<job specification input file> <job directory> <results output file name> <user log file name> "
              "<metadata file name> [api version] [dependencies]" % argv[0],
              file=sys.stderr)
        exit(1)

    job_specification_file = argv[1]
    job_dir = Path(argv[2])
    output_file = job_dir / argv[3]
    log_file = job_dir / argv[4]
    metadata_file = job_dir / argv[5]
    api_version = argv[6] if len(argv) >= 7 else None
    dependencies = _deserialize_dependencies(argv[7]) if len(argv) >= 8 else {}
    user_id = argv[8] if len(argv) >= 9 else None

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
            principal = sc.getConf().get("spark.yarn.principal")
            key_tab = sc.getConf().get("spark.yarn.keytab")

            kerberos(principal, key_tab)
            
            def run_driver(): 
                run_job(
                    job_specification=job_specification, output_file=output_file, metadata_file=metadata_file,
                    api_version=api_version, job_dir=job_dir, dependencies=dependencies, user_id=user_id
                )
            
            if sc.getConf().get('spark.python.profile', 'false').lower()=='true':
                # Including the driver in the profiling: a bit hacky solution but spark profiler api does not allow passing args&kwargs
                driver_profile=BasicProfiler(sc)
                driver_profile.profile(run_driver)
                # running the driver code and adding driver's profiling results as "RDD==-1"
                sc.profiler_collector.add_profiler(-1, driver_profile)
                # collect profiles into a zip file
                sc.dump_profiles(str(job_dir / 'profile_dumps'))
                profile_zip=shutil.make_archive(str(job_dir / 'profile_dumps'), 'gztar', str(job_dir / 'profile_dumps'))
                logger.info("Saving profiling info to: "+profile_zip)
            else:
                run_driver()
                
    except Exception as e:
        logger.exception("error processing batch job")
        user_facing_logger.exception("error processing batch job")
        raise e

@log_memory
def run_job(job_specification, output_file: Path, metadata_file: Path, api_version, job_dir, dependencies: dict, user_id:str=None):
    process_graph = job_specification['process_graph']

    backend_implementation = GeoPySparkBackendImplementation()
    logger.info(f"Using backend implementation {backend_implementation}")
    correlation_id = str(uuid.uuid4())
    logger.info(f"Correlation id: {correlation_id}")
    env = EvalEnv({
        'version': api_version or "1.0.0",
        'pyramid_levels': 'highest',
        'user': User(user_id=user_id),
        'correlation_id': correlation_id,
        'dependencies': dependencies,
        "backend_implementation": backend_implementation,
    })
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
    if('write_assets' in dir(result)):
        result.options["batch_mode"] = True
        result.options["file_metadata"] = global_metadata_attributes
        if( result.options.get("sample_by_feature")):
            geoms = tracer.get_geometries("filter_spatial")
            if len(geoms) > 1:
                logger.warning("Multiple aggregate_spatial geometries: {c}".format(c=len(geoms)))
            elif len(geoms) == 1:
                logger.warning("sample_by_feature enabled, but no geometries found. They can be specified using filter_spatial.")
            result.options["geometries"] = geoms[0]
            if(result.options["geometries"] == None):
                logger.error("samply_by_feature was set, but no geometries provided through filter_spatial. Make sure to provide geometries.")
        assets_metadata = result.write_assets(str(output_file))
        for name,asset in assets_metadata.items():
            _add_permissions(Path(asset["href"]), stat.S_IWGRP)
        logger.info("wrote image collection to %s" % output_file)

    elif isinstance(result, ImageCollectionResult):
        result.options["batch_mode"] = True
        result.save_result(filename=str(output_file))
        _add_permissions(output_file, stat.S_IWGRP)
        logger.info("wrote image collection to %s" % output_file)
    elif isinstance(result, JSONResult):
        with open(output_file, 'w') as f:
            json.dump(result.prepare_for_json(), f)
        _add_permissions(output_file, stat.S_IWGRP)
        logger.info("wrote JSON result to %s" % output_file)
    elif isinstance(result, MultipleFilesResult):
        result.reduce(output_file, delete_originals=True)
        _add_permissions(output_file, stat.S_IWGRP)
        logger.info("reduced %d files to %s" % (len(result.files), output_file))
    elif isinstance(result, NullResult):
        logger.info("skipping output file %s" % output_file)
    else:
        raise NotImplementedError("unsupported result type {r}".format(r=type(result)))

    if any(card4l for _, card4l in dependencies.values()):  # TODO: clean this up
        logger.debug("awaiting Sentinel Hub CARD4L data...")

        s3_service = get_jvm().org.openeo.geotrellissentinelhub.S3Service()
        bucket_name = ConfigParams().sentinel_hub_batch_bucket

        poll_interval_secs = 10
        max_delay_secs = 600

        card4l_dependencies = [(collection_id, request_group_id) for
                               (collection_id, metadata_properties), (request_group_id, card4l)
                               in dependencies.items() if card4l]

        for collection_id, request_group_id in card4l_dependencies:
            try:
                # FIXME: incorporate collection_id and metadata_properties to make sure the files don't clash
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

    _export_result_metadata(tracer=tracer, result=result, output_file=output_file, metadata_file=metadata_file,asset_metadata=assets_metadata)

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
    _setup_app_logging()

    with TimingLogger("batch_job.py main", logger=logger):
        main(sys.argv)
