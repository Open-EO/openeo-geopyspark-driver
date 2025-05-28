from __future__ import annotations
import collections
import collections.abc
import contextlib
import dataclasses
import datetime
import functools
import grp
import json
import logging
import math
import os
import pkgutil
import pwd
import resource
import stat
import tempfile
import time
from functools import partial
from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple, Union, Dict, Any, TypeVar, Iterator

import dateutil.parser
import pyproj
import pytz
from epsel import on_first_time
from kazoo.client import KazooClient

from openeo.util import rfc3339
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
from pyproj import CRS
from shapely.geometry import GeometryCollection, MultiPolygon, Point, Polygon, box
from shapely.geometry.base import BaseGeometry

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.util.runtime import get_job_id

# TODO split up this kitchen sink module into more focused modules


logger = logging.getLogger(__name__)

GDALINFO_SUFFIX = "_gdalinfo.json"

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
        normalize_date(end or rfc3339.now_utc()),
    )


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
    # TODO: replace all use cases with openeodriver.integrations.s3.get_s3_client because all remaining calls
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


def get_s3_file_contents(filename: Union[os.PathLike,str]) -> str:
    """Get contents of a text file from the S3 bucket.

        The bucket is set in ConfigParams().s3_bucket_name
    """
    # TODO: move this to openeodriver.integrations.s3?
    s3_instance = s3_client()
    s3_file_object = s3_instance.get_object(
        Bucket=get_backend_config().s3_bucket_name,
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

    s3_instance = s3_client()
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

    s3_instance = s3_client()
    bucket_keys = s3_instance.list_objects_v2(Bucket=bucket, MaxKeys=1000, Prefix=input_dir)
    for obj in bucket_keys["Contents"]:
        key = obj["Key"]
        output_dir_path = os.path.join(output_dir, os.path.dirname(key))
        os.makedirs(output_dir_path, exist_ok=True)
        if not key.endswith("/"):
            output_file_path = os.path.join(output_dir, key)
            s3_instance.download_file(Bucket=bucket, Key=key, Filename=output_file_path)


def to_s3_url(file_or_dir_name: Union[os.PathLike,str], bucketname: str = None) -> str:
    """Get a URL for S3 to the file or directory, in the correct format."""
    # TODO: move this to openeodriver.integrations.s3?

    bucketname = bucketname or get_backend_config().s3_bucket_name

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
    # TODO: Don't change permissions on s3 urls?
    # if str(path).lower().startswith("s3:/"):
    #     return
    if path.exists():
        current_permission_bits = os.stat(path).st_mode
        os.chmod(path, current_permission_bits | mode)
    else:
        for p in path.parent.glob('*'):
            current_permission_bits = os.stat(p).st_mode
            p.chmod(current_permission_bits | mode)


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


def reproject_cellsize(
        spatial_extent: dict,
        input_resolution: tuple,
        input_crs: str,
        to_crs: str,
) -> Tuple[float, float]:
    """
    :param spatial_extent: The spatial extent is needed, because conversion is often
    different when done at the poles compared to the equator.
    eg: When converting 1meter to degrees (in LatLon) at the North Pole, it can be way more degrees more in LatLon
    compared to the same conversion at the equator.
    :param input_resolution:
    :param input_crs:
    :param to_crs:
    """
    if "crs" not in spatial_extent:
        spatial_extent = spatial_extent.copy()
        spatial_extent["crs"] = "EPSG:4326"
    west, south = spatial_extent["west"], spatial_extent["south"]
    east, north = spatial_extent["east"], spatial_extent["north"]
    spatial_extent_shaply = box(west, south, east, north)
    if to_crs == "Auto42001" or input_crs == "Auto42001":
        # Find correct UTM zone
        utm_zone_crs = auto_utm_epsg_for_geometry(spatial_extent_shaply, spatial_extent["crs"])
        if to_crs == "Auto42001":
            to_crs = utm_zone_crs
        if input_crs == "Auto42001":
            input_crs = utm_zone_crs

    p = spatial_extent_shaply.representative_point()
    transformer = pyproj.Transformer.from_crs(spatial_extent["crs"], input_crs, always_xy=True)
    x, y = transformer.transform(p.x, p.y)

    cell_bbox = {
        "west": x,
        "east": x + input_resolution[0],
        "south": y,
        "north": y + input_resolution[1],
        "crs": input_crs
    }
    cell_bbox_reprojected = reproject_bounding_box(cell_bbox, from_crs=cell_bbox["crs"], to_crs=to_crs)

    cell_width_reprojected = abs(cell_bbox_reprojected["east"] - cell_bbox_reprojected["west"])
    cell_height_reprojected = abs(cell_bbox_reprojected["north"] - cell_bbox_reprojected["south"])

    return cell_width_reprojected, cell_height_reprojected


def health_check_extent(extent):
    crs = extent.get("crs", "EPSG:4326")
    is_utm = crs == "Auto42001" or crs.startswith("EPSG:326")

    if extent["west"] > extent["east"] or extent["south"] > extent["north"]:
        logger.warning(f"health_check_extent extent with surface<0: {extent}")
        return False

    if is_utm:
        # This is an extent that has the highest sensible values for northern and/or southern hemisphere UTM zones
        utm_bounds = {
            "west": 166021.44,
            "south": -10000000,
            "east": 833978.56,
            "north": 10000000,
        }
        width = utm_bounds["east"] - utm_bounds["west"]
        horizontal_tolerance = 5  # UTM zone has quite some horizontal tolerance
        utm_bounds["west"] = utm_bounds["west"] - width * horizontal_tolerance
        utm_bounds["east"] = utm_bounds["east"] + width * horizontal_tolerance
        if (
            extent["west"] < utm_bounds["west"]
            or extent["east"] > utm_bounds["east"]
            or extent["south"] < utm_bounds["south"]
            or extent["north"] > utm_bounds["north"]
        ):
            logger.warning(f"health_check_extent dangerous extent: {extent}")
            return False
    elif crs == "EPSG:4326":
        horizontal_tolerance = 1.1
        if (
            extent["west"] < -180 * horizontal_tolerance
            or extent["east"] > 180 * horizontal_tolerance
            or extent["south"] < -90
            or extent["north"] > 90
        ):
            logger.warning(f"health_check_extent dangerous extent: {extent}")
            return False

    return True


def parse_approximate_isoduration(s):
    """
    Parse the ISO8601 duration as years,months,weeks,days, hours,minutes,seconds.
    Approximate, because it does not care about leap years, months with different number of days, etc.
    Examples: "PT1H30M15.460S", "P5DT4M", "P2WT3H", "P1D"
    Based on: https://stackoverflow.com/questions/36976138/is-there-an-easy-way-to-convert-iso-8601-duration-to-timedelta
    """

    def get_isosplit(s_arg, split):
        if split in s_arg:
            n, s_arg = s_arg.split(split, 1)
        else:
            n = '0'
        return float(n.replace(',', '.')), s_arg  # to handle like "P0,5Y"

    s = s.split('P', 1)[-1]  # Remove prefix
    # M can mean month or minute, so we split the day and time part:
    if 'T' in s:
        s_date0, s_time0 = s.split('T', 1)
    else:
        s_date0 = s
        s_time0 = ''
    s_date, s_time = s_date0, s_time0
    s_yr, s_date = get_isosplit(s_date, 'Y')  # Step through letter dividers
    s_mo, s_date = get_isosplit(s_date, 'M')
    s_wk, s_date = get_isosplit(s_date, 'W')
    s_dy, s_date = get_isosplit(s_date, 'D')

    s_hr, s_time = get_isosplit(s_time, 'H')
    s_mi, s_time = get_isosplit(s_time, 'M')
    s_sc, s_time = get_isosplit(s_time, 'S')
    n_yr = s_yr * 365  # approx days for year, month, week
    n_mo = s_mo * 30.4  # Average days per month
    n_wk = s_wk * 7
    dt = datetime.timedelta(days=n_yr + n_mo + n_wk + s_dy, hours=s_hr, minutes=s_mi,
                            seconds=s_sc)
    return dt


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
