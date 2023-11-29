import collections
import collections.abc
import contextlib
import datetime
import functools
import pkgutil

import grp
import json
import logging
import math
import os
import pwd
import resource
import stat
import tempfile

from epsel import on_first_time
from functools import partial
from pathlib import Path
from shapely.geometry.base import BaseGeometry
from typing import Callable, Optional, Tuple, Union, Iterable

import pytz
import dateutil.parser
from kazoo.client import KazooClient
from py4j.clientserver import ClientServer
from py4j.java_gateway import JVMView
from pyproj import CRS
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, Point

from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.util.geometry import GeometryBufferer
from openeo_driver.util.logging import (
    get_logging_config,
    setup_logging, BatchJobLoggingFilter,
    FlaskRequestCorrelationIdLogging,
    FlaskUserIdLogging,
    LOGGING_CONTEXT_BATCH_JOB,
    LOG_HANDLER_STDERR_JSON,
    LOG_HANDLER_FILE_JSON,
)
from openeogeotrellis.configparams import ConfigParams

logger = logging.getLogger(__name__)


def log_memory(function):
    def memory_logging_wrapper(*args, **kwargs):
        import faulthandler
        faulthandler.enable()

        return function(*args, **kwargs)

    return memory_logging_wrapper


def get_jvm() -> JVMView:
    import geopyspark
    pysc = geopyspark.get_spark_context()
    gateway = pysc._gateway
    assert isinstance(gateway, ClientServer), f"Java logging assumes ThreadLocals behave; got a {type(gateway)} instead"
    return gateway.jvm


def mdc_include(sc, jvm, mdc_key, mdc_value):
    jvm.org.slf4j.MDC.put(mdc_key, mdc_value)
    sc.setLocalProperty(mdc_key, mdc_value)


def mdc_remove(sc, jvm, *mdc_keys):
    for key in mdc_keys:
        jvm.org.slf4j.MDC.remove(key)
        sc.setLocalProperty(key, None)



def dict_merge_recursive(a: collections.abc.Mapping, b: collections.abc.Mapping, overwrite=False) -> collections.abc.Mapping:
    """
    Merge two dictionaries recursively

    :param a: first dictionary
    :param b: second dictionary
    :param overwrite: whether values of b can overwrite values of a
    :return: merged dictionary

    # TODO move this to utils module in openeo-python-driver or openeo-python-client?
    """
    # Start with shallow copy, we'll copy deeper parts where necessary through recursion.
    result = dict(a)
    for key, value in b.items():
        if key in result:
            if isinstance(value, collections.abc.Mapping) and isinstance(result[key], collections.abc.Mapping):
                result[key] = dict_merge_recursive(result[key], value, overwrite=overwrite)
            elif overwrite:
                result[key] = value
            elif result[key] == value:
                pass
            else:
                raise ValueError("Can not automatically merge values {a!r} and {b!r} for key {k!r}"
                                 .format(a=result[key], b=value, k=key))
        else:
            result[key] = value
    return result


def normalize_date(date_string: Union[str, None]) -> Union[str, None]:
    if date_string is not None:
        date = dateutil.parser.parse(date_string)
        if date.tzinfo is None:
            date = date.replace(tzinfo=pytz.UTC)
        return date.isoformat()
    return None


def normalize_temporal_extent(temporal_extent: Tuple[Union[str, None], Union[str, None]]) -> Tuple[str, str]:
    start, end = temporal_extent
    return (
        normalize_date(start or "2000-01-01"),  # TODO: better fallback start date?
        normalize_date(end or utcnow().isoformat())
    )


class UtcNowClock:
    """
    Helper class to have a mockable wrapper for datetime.datetime.utcnow
    (which is not straightforward to mock directly).
    """

    # TODO: just start using `time_machine` module for time mocking

    _utcnow = _utcnow_orig = datetime.datetime.utcnow

    @classmethod
    def utcnow(cls) -> datetime.datetime:
        return cls._utcnow()

    @classmethod
    @contextlib.contextmanager
    def mock(cls, now: Union[datetime.datetime, str]):
        """Context manager to mock the return value of `utcnow()`."""
        if isinstance(now, str):
            now = dateutil.parser.parse(now)
        cls._utcnow = lambda: now
        try:
            yield
        finally:
            cls._utcnow = cls._utcnow_orig


# Alias for general usage
utcnow = UtcNowClock.utcnow


def describe_path(path: Union[Path, str]) -> dict:
    path = Path(path)
    if path.exists() or path.is_symlink():
        st = os.stat(str(path))
        return {
            "path": str(path.absolute()),
            "mode": stat.filemode(st.st_mode),
            "uid": st.st_uid,
            "user": pwd.getpwuid(st.st_uid).pw_name,
            "gid": st.st_gid,
            "group": grp.getgrgid(st.st_gid).gr_name,
            "size": st.st_size
        }
    else:
        return {
            "path": str(path),
            "status": "does not exist"
        }


def to_projected_polygons(
    jvm: JVMView,
    geometry: Union[
        str,
        Path,
        DelayedVector,
        DriverVectorCube,
        GeometryCollection,
        Polygon,
        MultiPolygon,
    ],
    *,
    crs: Optional[str] = None,
    buffer_points=False,
    none_for_points=False,
) -> "jvm.org.openeo.geotrellis.ProjectedPolygons":
    """Construct ProjectedPolygon instance"""
    logger.info(f"to_projected_polygons with {type(geometry)} ({buffer_points=}, {none_for_points=})")
    if isinstance(geometry, (str, Path)):
        # Vector file
        if not (crs is None or isinstance(crs, str) and crs.upper() == "EPSG:4326"):
            raise ValueError(f"Expected default CRS (EPSG:4326) but got {crs!r}")
        return jvm.org.openeo.geotrellis.ProjectedPolygons.fromVectorFile(str(geometry))
    elif isinstance(geometry, DelayedVector):
        return to_projected_polygons(jvm, geometry.path, crs=crs)
    elif isinstance(geometry, DriverVectorCube):
        expected_crs = str(geometry.get_crs().to_proj4()).lower().replace("ellps","datum")
        provided_crs = CRS.from_user_input(crs).to_proj4().lower().replace("ellps","datum") if crs else None
        if crs and provided_crs != expected_crs:
            raise RuntimeError(f"Unexpected crs: {provided_crs!r} != {expected_crs!r}")
        # TODO: reverse this: make DriverVectorCube handling the reference implementation
        #       and GeometryCollection the legacy/deprecated way
        return to_projected_polygons(
            jvm,
            GeometryCollection(list(geometry.get_geometries())),
            crs=f"EPSG:{geometry.get_crs().to_epsg()}",
            buffer_points=buffer_points,
            none_for_points=none_for_points,
        )
    elif isinstance(geometry, GeometryCollection):
        # TODO Open-EO/openeo-python-driver#71 deprecate/eliminate this GeometryCollection handling
        # Multiple polygons
        geoms = geometry.geoms
        polygons_srs = crs or "EPSG:4326"
        if buffer_points:
            # TODO: buffer distance of 10m assumes certain resolution (e.g. sentinel2 pixels)
            # TODO: use proper distance for collection resolution instead of using a default distance?
            bufferer = GeometryBufferer.from_meter_for_crs(
                distance=10, crs=polygons_srs
            )
            geoms = (bufferer.buffer(g) if isinstance(g, Point) else g for g in geoms)
        elif none_for_points and any(isinstance(g, Point) for g in geoms):
            # Special case: if there is any point in the geometry: return None
            # to take a different code path in zonal_statistics.
            # TODO: can we eliminate this special case handling?
            return None
        polygon_wkts = [str(g) for g in geoms]
        return jvm.org.openeo.geotrellis.ProjectedPolygons.fromWkt(
            polygon_wkts, polygons_srs
        )
    elif isinstance(geometry, (Polygon, MultiPolygon)):
        # Single polygon
        polygon_wkts = [str(geometry)]
        polygons_srs = crs or "EPSG:4326"
        return jvm.org.openeo.geotrellis.ProjectedPolygons.fromWkt(
            polygon_wkts, polygons_srs
        )
    elif isinstance(geometry, Point):
        geometry = DriverVectorCube.from_geometry(geometry)
        return to_projected_polygons(
            jvm,
            geometry,
            crs=crs,
            buffer_points=buffer_points,
            none_for_points=none_for_points,
        )
    else:
        raise ValueError(geometry)


@contextlib.contextmanager
def zk_client(hosts: str = ",".join(ConfigParams().zookeepernodes), *, timeout=10.0):
    # TODO: move this to a more generic zookeeper module, e.g. `openeogeotrellis.integrations.zookeeper`?
    zk = KazooClient(hosts, timeout=timeout)
    zk.start()

    try:
        yield zk
    finally:
        zk.stop()
        zk.close()


def set_max_memory(max_total_memory_in_bytes: int):
    soft_limit, hard_limit = max_total_memory_in_bytes, max_total_memory_in_bytes
    resource.setrlimit(resource.RLIMIT_AS, (soft_limit, hard_limit))

    logger.info("set resource.RLIMIT_AS to {b} bytes".format(b=max_total_memory_in_bytes))


def s3_client():
    # TODO: move this to openeogeotrellis.integrations.s3?
    import boto3
    # TODO: Get these credentials/secrets from VITO TAP vault instead of os.environ
    aws_access_key_id = os.environ.get("SWIFT_ACCESS_KEY_ID",os.environ.get("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key=os.environ.get("SWIFT_SECRET_ACCESS_KEY",os.environ.get("AWS_SECRET_ACCESS_KEY"))
    swift_url = os.environ.get("SWIFT_URL")
    s3_client = boto3.client("s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=swift_url)
    return s3_client


def download_s3_dir(bucketName, directory):
    # TODO: move this to openeogeotrellis.integrations.s3?
    import boto3

    # TODO: Get these credentials/secrets from VITO TAP vault instead of os.environ
    aws_access_key_id = os.environ.get("SWIFT_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key = os.environ.get("SWIFT_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    swift_url = os.environ.get("SWIFT_URL")

    s3_resource = boto3.resource("s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=swift_url)
    bucket = s3_resource.Bucket(bucketName)

    for obj in bucket.objects.filter(Prefix = directory):
        if not os.path.exists("/" + os.path.dirname(obj.key)):
            os.makedirs("/" + os.path.dirname(obj.key))
        bucket.download_file(obj.key, "/{obj}".format(obj=obj.key))


def get_s3_file_contents(filename: Union[os.PathLike,str]) -> str:
    """Get contents of a text file from the S3 bucket.

        The bucket is set in ConfigParams().s3_bucket_name
    """
    # TODO: move this to openeogeotrellis.integrations.s3?
    s3_instance = s3_client()
    s3_file_object = s3_instance.get_object(
        Bucket=ConfigParams().s3_bucket_name, Key=str(filename).strip("/")
    )
    body = s3_file_object["Body"]
    return body.read().decode("utf8")


def get_s3_binary_file_contents(s3_url: str) -> bytes:
    """Get contents of a binary file from the S3 bucket."""
    # TODO: move this to openeogeotrellis.integrations.s3?

    # This only supports S3 URLs, does not support filenames with the
    # S3 bucket set in ConfigParams().
    if not s3_url.startswith("s3://"):
        raise ValueError(f"s3_url must be a URL that starts with 's3://' Value is: {s3_url=}")

    bucket, file_name = s3_url[5:].split("/", 1)
    logger.debug("Downloading contents from S3 object storage: {bucket=}, key={file_name}")

    s3_instance = s3_client()
    s3_file_object = s3_instance.get_object(Bucket=bucket, Key=file_name)
    body = s3_file_object["Body"]
    return body.read()


def to_s3_url(file_or_dir_name: Union[os.PathLike,str], bucketname: str = None) -> str:
    """Get a URL for S3 to the file or directory, in the correct format."""
    # TODO: move this to openeogeotrellis.integrations.s3?

    bucketname = bucketname or ConfigParams().s3_bucket_name

    # See also:
    # https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html
    #
    # file_or_dir_name, is actually the S3 key, and it should neither start nor
    # end with a slash in order to keep the S3 keys and S3 URLs uniform.
    #
    # 1) With / at the start we would get weird URLS with a // after bucketname,
    # like so: s3://my-bucket//path-to-file-or-dir
    #
    # 2) Allowing folders to end with a slash just creates confusion.
    # It keeps things simpler when S3 keys never include a slash at the end.
    file_or_dir_name = str(file_or_dir_name).strip("/")

    # Keep it robust: bucketname should not contain "/" at all but lets remove
    # the / just in case, because mistakes are easy to make.
    bucketname = bucketname.strip("/")
    return f"s3://{bucketname}/{file_or_dir_name}"


def lonlat_to_mercator_tile_indices(
        longitude: float, latitude: float, zoom: int,
        tile_size: int = 512, flip_y: bool = False
):
    """
    Conversion of lon-lat coordinates to (web)Mercator tile indices
    :param longitude:
    :param latitude:
    :param zoom: zoom level (0, 1, ...)
    :param tile_size: tile size in pixels
    :param flip_y: False: [0, 0] is lower left corner (TMS); True: [0, 0] is upper left (Google Maps/QuadTree style)
    :return: (tx, ty) mercator tile indices
    """
    # Lon-lat to Spherical Mercator "meters" (EPSG:3857/EPSG:900913)
    offset = 2 * math.pi * 6378137 / 2.0
    mx = longitude * offset / 180
    my = (math.log(math.tan((90 + latitude) * math.pi / 360)) / (math.pi / 180.0)) * offset / 180
    # Meters to pyramid pixels at zoom level
    resolution = 2 * math.pi * 6378137 / tile_size / (2 ** zoom)
    px = (mx + offset) / resolution
    py = (my + offset) / resolution
    # Pixels to TMS tile
    tx = int(math.ceil(px / tile_size) - 1)
    ty = int(math.ceil(py / tile_size) - 1)
    if flip_y:
        ty = (2 ** zoom - 1) - ty
    return tx, ty


@contextlib.contextmanager
def nullcontext():
    """
    Context manager that does nothing.

    Backport of Python 3.7 `contextlib.nullcontext`
    """
    yield


def single_value(xs):
    """
    If the values in the collection are the same, return that value.
    """

    xs = iter(xs)

    try:
        first = next(xs)

        if all(x == first for x in xs):
            return first
    except StopIteration:
        raise ValueError(f"no values in {xs}")

    raise ValueError(f"distinct values in {xs}")


def add_permissions(path: Path, mode: int):
    # TODO: accept PathLike etc as well
    # TODO: maybe umask is a better/cleaner option
    if path.exists():
        current_permission_bits = os.stat(path).st_mode
        os.chmod(path, current_permission_bits | mode)
    else:
        for p in path.parent.glob('*'):
            current_permission_bits = os.stat(p).st_mode
            p.chmod(current_permission_bits | mode)


def ensure_executor_logging(f) -> Callable:
    def setup_context_aware_logging(user_id: Optional[str], request_id: str):
        job_id = os.environ.get("OPENEO_BATCH_JOB_ID")
        in_batch_job_context = job_id is not None

        if in_batch_job_context:
            user_id = os.environ["OPENEO_USER_ID"]

            BatchJobLoggingFilter.set("user_id", user_id)
            BatchJobLoggingFilter.set("job_id", job_id)
        else:  # executors started from Flask, CLI ...
            # TODO: takes advantage of "global state" BatchJobLoggingFilter/LOGGING_CONTEXT_BATCH_JOB but introducing
            #  e.g. LOGGING_CONTEXT_EXECUTOR in openeo-python-driver is not right because it is not aware of
            #  implementation details like Spark executors.
            BatchJobLoggingFilter.set("user_id", user_id)
            BatchJobLoggingFilter.set("req_id", request_id)

        logging_config = get_logging_config(
            root_handlers=[LOG_HANDLER_STDERR_JSON if ConfigParams().is_kube_deploy else LOG_HANDLER_FILE_JSON],
            loggers={
                "openeo": {"level": "DEBUG"},
                "openeo_driver": {"level": "DEBUG"},
                "openeogeotrellis": {"level": "DEBUG"},
                "kazoo": {"level": "WARN"},
                "cropsar": {"level": "DEBUG"},
            },
            context=LOGGING_CONTEXT_BATCH_JOB,
            root_level=os.environ.get("OPENEO_LOGGING_THRESHOLD", "INFO"),
        )
        setup_logging(logging_config)

    decorator = on_first_time(partial(setup_context_aware_logging,
                                      user_id=FlaskUserIdLogging.get_user_id(),
                                      request_id=FlaskRequestCorrelationIdLogging.get_request_id()))

    return decorator(f)


def drop_empty_from_aggregate_polygon_result(result: dict):
    """
    Drop empty items from an AggregatPolygonResult JSON export
    :param result:
    :return:
    """
    # TODO: ideally this should not be necessary and be done automatically by the back-end
    return {k: v for (k, v) in result.items() if not all(x == [] for x in v)}


def temp_csv_dir(message: str = "n/a") -> str:
    """Create (temporary) work directory for CSV output"""
    # TODO: make this more generic for other file types too
    parent_dir = None
    for candidate in [
        # TODO: this should come from config instead of trying to detect it
        "/data/projects/OpenEO/timeseries",
        "/shared_pod_volume",
        # TODO: also allow unit tests to inject `tmp_path` based parent here?
        # TODO: also use batch job specific parent candidate?
    ]:
        if Path(candidate).exists():
            parent_dir = candidate
            break
    temp_dir = tempfile.mkdtemp(prefix="timeseries_", suffix="_csv", dir=parent_dir)
    try:
        os.chmod(temp_dir, 0o777)
    except PermissionError as e:
        logger.warning(
            f"Got permission error while setting up temp dir: {str(temp_dir)}, but will try to continue."
        )
    logger.info(f"Created temp csv dir {temp_dir!r}: {message}")
    return temp_dir


def json_write(
    path: Union[str, Path],
    data: dict,
    indent: Optional[int] = None,
) -> Path:
    """Helper to easily JSON-dump a data structure to a JSON file."""
    # TODO: move this up to openeo-python-driver or even openeo-python-client
    path = Path(path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open(mode="w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent)
    return path


def calculate_rough_area(geoms: Iterable[BaseGeometry]):
    """
    For every geometry, roughly estimate its area using its bounding box and return their sum.

    @param geoms: the geometries to estimate the area for
    @return: the sum of the estimated areas
    """
    total_area = 0
    for geom in geoms:
        if hasattr(geom, "geoms"):
            total_area += calculate_rough_area(geom.geoms)
        else:
            total_area += (geom.bounds[2] - geom.bounds[0]) * (geom.bounds[3] - geom.bounds[1])
    return total_area


class StatsReporter:
    """
    Context manager to collect stats using `collections.Counter`
    and report automatically these on exit.

    Usage example:

        with StatsReporter(report=print) as stats:
            stats["apple"] += 1
    """

    # TODO: move this to openeo_driver or even openeo client lib?
    def __init__(
        self,
        name: str = "stats",
        report: Union[Callable[[str], None], logging.Logger] = logger,
    ):
        self.name = name
        if isinstance(report, logging.Logger):
            report = report.info
        self.report = report
        self.stats = None

    def __enter__(self) -> collections.Counter:
        self.stats = collections.Counter()
        return self.stats

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.report(f"{self.name}: {json.dumps(self.stats)}")


@functools.lru_cache
def is_package_available(name: str) -> bool:
    return any(m.name == name for m in pkgutil.iter_modules())
