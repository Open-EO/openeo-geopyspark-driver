from __future__ import annotations
import collections
import collections.abc
import contextlib
import dataclasses
import datetime
import grp
import hashlib
import itertools
import json
import logging
import math
import os
import pwd
import resource
import shutil
import stat
import sys
import tempfile
import time
from functools import partial
from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple, Union, Dict, Any, TypeVar, Iterator

from openeogeotrellis.integrations.s3_client import S3ClientBuilder, eodata_s3_client

import dateutil.parser
import pyproj
import pytz
from epsel import on_first_time
from kazoo.client import KazooClient

from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.util.geometry import GeometryBufferer, reproject_bounding_box
from openeo_driver.util.logging import (
    LOG_HANDLER_FILE_JSON,
    LOG_HANDLER_STDERR_JSON,
    LOGGING_CONTEXT_BATCH_JOB,
    GlobalExtraLoggingFilter,
    FlaskRequestCorrelationIdLogging,
    FlaskUserIdLogging,
    get_logging_config,
    setup_logging,
)
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from py4j.clientserver import ClientServer
from py4j.java_gateway import JVMView
from shapely.geometry import GeometryCollection, MultiPolygon, Point, Polygon, box
from shapely.geometry.base import BaseGeometry

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.job_results import util as job_results_util
from openeogeotrellis.util.runtime import get_job_id

# TODO split up this kitchen sink module into more focused modules


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
        #expected_crs = str(geometry.get_crs().to_proj4()).lower().replace("ellps","datum")
        #provided_crs = CRS.from_user_input(crs).to_proj4().lower().replace("ellps","datum") if crs else None
        #if crs and provided_crs != expected_crs:
        #    raise RuntimeError(f"Unexpected crs: {provided_crs!r} != {expected_crs!r}")
        # TODO: reverse this: make DriverVectorCube handling the reference implementation
        #       and GeometryCollection the legacy/deprecated way
        epsg_code: Optional[int] = geometry.get_crs().to_epsg()
        if epsg_code is None:
            logger.error(f"CRS cannot be converted to EPSG code. Falling back to epsg:4326. CRS:\n{geometry.get_crs()!r}\ngeometry:\n{geometry!r}.")
            epsg_code = 4326
        return to_projected_polygons(
            jvm,
            GeometryCollection(list(geometry.get_geometries())),
            crs=f"EPSG:{epsg_code}",
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
    from openeogeotrellis.config.config import get_zookeeper_auth_data
    config = get_backend_config()
    auth_data = get_zookeeper_auth_data(config) or None
    zk = KazooClient(hosts, timeout=timeout, sasl_options=config.zookeeper_sasl_options, auth_data=auth_data)
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
    # TODO: replace all use cases with get_s3_client(bucket_name)
    # imply a region dependency
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


def get_s3_file_contents(filename: Union[os.PathLike, str], bucket: Optional[str] = None) -> str:
    """
    Get contents of a text file in an S3 bucket; the bucket defaults to ConfigParams().s3_bucket_name.
    """
    # TODO: move this to openeodriver.integrations.s3?
    _bucket = bucket or get_backend_config().s3_bucket_name
    s3_instance = S3ClientBuilder.from_bucket(_bucket)
    s3_file_object = s3_instance.get_object(
        Bucket=_bucket,
        Key=str(filename).strip("/"),
    )
    body = s3_file_object["Body"]
    return body.read().decode("utf8")


def stream_s3_binary_file_contents(s3_url: str) -> Iterable[bytes]:
    """Get contents of a binary file from the S3 bucket."""
    # TODO: move this to openeodriver.integrations.s3?

    # This only supports S3 URLs, does not support filenames with the
    # S3 bucket set in ConfigParams().
    if not s3_url.startswith("s3://"):
        raise ValueError(f"s3_url must be a URL that starts with 's3://' Value is: {s3_url=}")

    bucket, file_name = s3_url[5:].split("/", 1)
    logger.debug(f"Streaming contents from S3 object storage: {bucket=}, key={file_name}")

    s3_instance = S3ClientBuilder.from_bucket(bucket)
    s3_file_object = s3_instance.get_object(Bucket=bucket, Key=file_name)
    body = s3_file_object["Body"]
    return body.iter_chunks()


def download_s3_directory(s3_url: str, output_dir: str):
    """
    Downloads a directory from S3 object storage to the specified output directory.

    Args:
        s3_url (str): The URL of the S3 directory to download. Must start with 's3://'.
        output_dir (str): The local directory where the S3 directory will be downloaded to.

    Raises:
        ValueError: If the s3_url does not start with 's3://'.

    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"s3_url must be a URL that starts with 's3://' Value is: {s3_url=}")

    bucket, input_dir = s3_url[5:].split("/", 1)
    logger.debug(f"Downloading directory from S3 object storage: {bucket=}, key={input_dir}")

    s3_instance = S3ClientBuilder.from_bucket(bucket)

    bucket_keys = s3_instance.list_objects_v2(Bucket=bucket, MaxKeys=1000, Prefix=input_dir)
    for obj in bucket_keys["Contents"]:
        key = obj["Key"]
        output_dir_path = os.path.join(output_dir, os.path.dirname(key))
        os.makedirs(output_dir_path, exist_ok=True)
        if not key.endswith("/"):
            output_file_path = os.path.join(output_dir, key)
            s3_instance.download_file(Bucket=bucket, Key=key, Filename=output_file_path)




def to_s3_url(file_or_dir_name: Union[os.PathLike, str], bucketname: str = None) -> str:
    """Get a URL for S3 to the file or directory, in the correct format."""
    # TODO: move this to openeodriver.integrations.s3?
    bucketname = bucketname or get_backend_config().s3_bucket_name
    return job_results_util.to_s3_url(file_or_dir_name, bucketname)


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


def add_permissions(path: Path, mode: int, user=None, group=None):
    """
    Add permissions to a file or directory, and optionally change its ownership.
    """
    # TODO: accept PathLike etc as well
    # TODO: maybe umask is a better/cleaner option
    if str(path).lower().startswith("s3:/"):
        logger.warning(f"add_permissions called on S3 path {path!r}, which is not supported.")
        return
    if path.exists():
        current_permission_bits = os.stat(path).st_mode
        os.chmod(path, current_permission_bits | mode)
        if user is not None or group is not None:
            try:
                shutil.chown(path, user=user, group=group)
            except LookupError as e:
                logger.warning(f"Could not change user/group of {path} to {user}/{group}.")
            except PermissionError as e:
                logger.warning(f"Could not change user/group of {path} to {user}/{group}, no permissions.")


def add_permissions_with_failsafe(path: Path, mode: int, user=None, group=None):
    if path.exists():
        add_permissions(path, mode, user=user, group=group)
    else:
        # If the path does not exist, we set the permissions on all siblings in the parent directory.
        # TODO: This was originally implemented for tiffs with multiple dates (EP-3800). Check if this can be removed.
        for p in path.parent.glob('*'):
            current_permission_bits = os.stat(p).st_mode
            p.chmod(current_permission_bits | mode)


def set_permissions(path: Path, mode: int, user=None, group=None):
    """
    Set permissions to a file or directory, and optionally change its ownership.
    """
    if str(path).lower().startswith("s3:/"):
        logger.warning(f"set_permissions called on S3 path {path!r}, which is not supported.")
        return
    if not path.exists():
        raise FileNotFoundError
    os.chmod(path, mode)
    if user is not None or group is not None:
        try:
            shutil.chown(path, user=user, group=group)
        except LookupError as e:
            logger.warning(f"Could not change user/group of {path} to {user}/{group}.")
        except PermissionError as e:
            logger.warning(f"Could not change user/group of {path} to {user}/{group}, no permissions.")


def ensure_executor_logging(f) -> Callable:
    def setup_context_aware_logging(user_id: Optional[str], request_id: str):
        job_id = get_job_id()
        in_batch_job_context = job_id is not None

        if in_batch_job_context:
            user_id = os.environ["OPENEO_USER_ID"]

            GlobalExtraLoggingFilter.set("user_id", user_id)
            GlobalExtraLoggingFilter.set("job_id", job_id)
        else:  # executors started from Flask, CLI ...
            # TODO: This code path probably violates the GlobalExtraLoggingFilter constraint of
            #       only using it for global/immutable context data, and might start failing
            #       if GlobalExtraLoggingFilter starts being more picky about that constraint.
            GlobalExtraLoggingFilter.set("user_id", user_id)
            GlobalExtraLoggingFilter.set("req_id", request_id)

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
    from datetime import datetime
    timestamp = datetime.today().strftime('%Y%m%d_%H%M')
    temp_dir = tempfile.mkdtemp(prefix=f"{timestamp}_timeseries_", suffix="_csv", dir=parent_dir)
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


def json_default(obj: Any) -> Any:
    """default function for packing objects in JSON."""
    # This function could cover more cases like jupyter's implementation does:
    # https://github.com/jupyter/jupyter_client/blob/main/jupyter_client/jsonutil.py#L108

    if isinstance(obj, Path):
        return str(obj)

    raise TypeError("%r is not JSON serializable" % obj)


def parse_json_from_output(output_str: str) -> Dict[str, Any]:
    lines = output_str.split("\n")
    parsing_json = False
    json_str = ""
    # reverse order to get last possible json line
    for l in reversed(lines):
        if not parsing_json:
            if l.endswith("}"):
                parsing_json = True
        json_str = l + json_str
        if l.startswith("{"):
            break

    return json.loads(json_str)


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


def _make_set_for_key(
    data: Dict[str, Dict[str, Any]],
    key: str,
    func: callable = lambda x: x,
) -> set:
    """
    Create a set containing only the values for `key` from the dicts in data.values().

    Optionally apply func() to that value, for example to allow converting lists,
    which are not hashable and cannot be a set element, to tuples.
    """
    return {func(val.get(key)) for val in data.values() if key in val}


T = TypeVar("T")
U = TypeVar("U")


def map_optional(f: Callable[[T], U], optional: Optional[T]) -> Optional[U]:
    return None if optional is None else f(optional)


def to_jsonable_float(x: float) -> Union[float, str]:
    """Replaces nan, inf and -inf with its string representation to allow JSON serialization."""
    return x if math.isfinite(x) else str(x)


def to_jsonable(x):
    if isinstance(x, float):
        return to_jsonable_float(x)
    if isinstance(x, dict):
        return {to_jsonable(key): to_jsonable(value) for key, value in x.items()}
    elif isinstance(x, list):
        return [to_jsonable(elem) for elem in x]

    return x


def wait_till_path_available(path: Path):
    retry = 0
    max_tries = 20  # Almost 2 minutes
    while not os.path.exists(path):
        if retry < max_tries:
            retry += 1
            seconds = 5
            logger.info(f"Waiting for path to be available. Try {retry}/{max_tries} (sleep:{seconds}seconds): {path}")
            time.sleep(seconds)
        else:
            logger.warning(f"Path is not available after {max_tries} tries: {path}")
            return  # TODO: Throw error instead?


class FileChangeWatcher:
    """
    FileChangeWatcher will keep state to determine whether files have changed since a previous time.

    This is to be used whenever you need to take action on content changes of a file. The process is
    1) create a wacher: `watcher = FileChangeWatcher()`
    2) Get a callback or None for the file you want to process:
       `callback = watcher.get_file_reload_register_func_if_changed("Path(/cfg.ini"))`
    3a) If None then nothing has changed and you don't need to do anything
    3b1) if notnNone then Process the config file
    3b2) If processing is fine call the callback: `callback()`
       `
    """

    @dataclasses.dataclass(frozen=True)
    class _FileChangeFingerprint:
        """
        FileChangeFingerprint can help to detect changes in files. This is useful when there is a need to reload a file if
        its content has changed.

        This is based on wisdom from a blog: https://apenwarr.ca/log/20181113
        """
        mtime: float
        size: int
        inode_nr: int
        file_mode: int
        owner_uid: int
        owner_gid: int

        @classmethod
        def from_file(cls, file_path: Path) -> FileChangeWatcher._FileChangeFingerprint:
            if file_path.exists():
                fs = os.stat(file_path)
                return cls(
                    mtime=fs.st_mtime,
                    size=fs.st_size,
                    inode_nr=fs.st_ino,
                    file_mode=fs.st_mode,
                    owner_uid=fs.st_uid,
                    owner_gid=fs.st_gid,
                )
            else:
                # For missing files we take a fingerprint that is impossible for an existing file which can be used as
                # a sentinel
                return cls(
                    mtime=0.0,
                    size=0,
                    inode_nr=0,
                    file_mode=0,
                    owner_uid=0,
                    owner_gid=0,
                )

    def __init__(self):
        self._last_config_reload: dict[Path, FileChangeWatcher._FileChangeFingerprint] = {}

    def get_file_reload_register_func_if_changed(self, file_path: Path, resolve=True) -> Optional[Callable[[], None]]:
        """
        Checks whether a file has changed since the last registered reload time. If it did return a function that can be
        called after the file has been successfully reloaded.
        """
        if resolve:
            file_path = file_path.resolve()

        existing_fingerprint = self._last_config_reload.get(file_path)
        current_fingerprint = self._FileChangeFingerprint.from_file(file_path)

        if existing_fingerprint == current_fingerprint:
            return None

        def register_reload() -> None:
            self._last_config_reload[file_path] = current_fingerprint

        return register_reload


def to_tuple(scala_tuple):
    return tuple(scala_tuple.productElement(i) for i in range(scala_tuple.productArity()))


def unzip(*iterables: Iterable) -> Iterator:
    # iterables are typically of equal length
    return zip(*iterables)


def partition(pred: Callable[[T], bool], iterable: Iterable[T]) -> Tuple[Iterator[T], Iterator[T]]:
    """Use a predicate to partition entries into true entries and false entries."""

    t1, t2 = itertools.tee(iterable)
    return filter(pred, t1), itertools.filterfalse(pred, t2)


def md5_checksum(file: Path) -> str:
    """Computes the MD5 checksum of a (potentially large) file."""

    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class BadlyHashable:
    """
    Simplifies implementation by allowing unhashable types in a dict-based cache. The number of
    items in this cache is very small anyway.
    """

    def __init__(self, target):
        self.target = target

    def __eq__(self, other):
        equal = isinstance(other, BadlyHashable) and self.target == other.target
        return equal

    def __hash__(self):
        return 0

    def __repr__(self):
        return f"BadlyHashable({repr(self.target)})"


def equals_approximately(ref_geom: BaseGeometry, actual_geom: BaseGeometry, rel_area_tolerance: float) -> bool:
    """Geometries are approximately equal if (area of) difference is small."""

    area_difference = ref_geom.symmetric_difference(actual_geom).area
    return area_difference / ref_geom.area < rel_area_tolerance


# TODO: Enable this on dev and staging too, but with an feature flag to quickly disable it when necessary.
if sys.version_info >= (3, 10) and (ConfigParams().is_ci_context or "pytest" in sys.modules):
    from typeguard import typechecked

    assert typechecked
else:
    def typechecked(func):
        """
        no-op
        """
        return func
