import collections
import contextlib
import datetime
import grp
import logging
import math
import os
import pwd
import resource
import stat
from pathlib import Path
from typing import Union, Tuple

import pytz
import dateutil.parser
from kazoo.client import KazooClient
from py4j.java_gateway import JavaGateway, JVMView
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

from openeo_driver.delayed_vector import DelayedVector
from openeogeotrellis.configparams import ConfigParams

logger = logging.getLogger("openeo")


def log_memory(function):
    def memory_logging_wrapper(*args, **kwargs):
        import faulthandler
        faulthandler.enable()
        enable_logging = True
        try:
            from spark_memlogger import memlogger
        except ImportError:
            enable_logging = False

        if enable_logging:
            ml = memlogger.MemLogger(5,api_version_auto_timeout_ms=60000)
            try:
                try:
                    ml.start()
                except:
                    logger.warning("Error while configuring memory logging, will not be available!", exc_info=True)
                return function(*args, **kwargs)
            finally:
                ml.stop()
        else:
            return function(*args, **kwargs)

    return memory_logging_wrapper


def get_jvm() -> JVMView:
    import geopyspark
    pysc = geopyspark.get_spark_context()
    gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
    jvm = gateway.jvm
    return jvm


def kerberos(principal, key_tab, jvm: JVMView = None):
    if jvm is None:
        jvm = get_jvm()

    if 'HADOOP_CONF_DIR' not in os.environ:
        logger.warning('HADOOP_CONF_DIR is not set. Kerberos based authentication will probably not be set up correctly.')

    hadoop_auth = jvm.org.apache.hadoop.conf.Configuration().get('hadoop.security.authentication')
    if hadoop_auth != 'kerberos':
        logger.warning('Hadoop client does not have hadoop.security.authentication=kerberos.')

    currentUser = jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
    if currentUser.hasKerberosCredentials():
        return
    logger.info("Kerberos currentUser={u!r} isSecurityEnabled={s!r}".format(
        u=currentUser.toString(), s=jvm.org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled()
    ))
    # print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getAuthenticationMethod().toString())

    if principal is not None and key_tab is not None:
        jvm.org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principal, key_tab)
        jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().setAuthenticationMethod(
            jvm.org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS)
    # print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().toString())
    # loginUser = jvm.org.apache.hadoop.security.UserGroupInformation.getLoginUser()
    # print(loginUser.toString())
    # print(loginUser.hasKerberosCredentials())
    # currentUser.addCredentials(loginUser.getCredentials())
    # print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().hasKerberosCredentials())


def dict_merge_recursive(a: dict, b: dict, overwrite=False) -> dict:
    """
    Merge two dictionaries recursively

    :param a: first dictionary
    :param b: second dictionary
    :param overwrite: whether values of b can overwrite values of a
    :return: merged dictionary

    # TODO move this to utils module in openeo-python-driver or openeo-python-client?
    """
    # Start with shallow copy, we'll copy deeper parts where necessary through recursion.
    result = a.copy()
    for key, value in b.items():
        if key in result:
            if isinstance(value, collections.Mapping) and isinstance(result[key], collections.Mapping):
                result[key] = dict_merge_recursive(result[key], value, overwrite=overwrite)
            elif overwrite:
                result[key] = value
            elif result[key] == value:
                pass
            else:
                raise ValueError("Can not automatically merge {a!r} and {b!r}".format(a=result[key], b=value))
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


def to_projected_polygons(jvm, *args):
    """Construct ProjectedPolygon instance"""
    if len(args) == 1 and isinstance(args[0], (str, Path)):
        # Vector file
        return jvm.org.openeo.geotrellis.ProjectedPolygons.fromVectorFile(str(args[0]))
    elif len(args) == 1 and isinstance(args[0], DelayedVector):
        return to_projected_polygons(jvm, args[0].path)
    elif 1 <= len(args) <= 2 and isinstance(args[0], GeometryCollection):
        # Multiple polygons
        polygon_wkts = [str(x) for x in args[0].geoms]
        polygons_srs = args[1] if len(args) >= 2 else 'EPSG:4326'
        return jvm.org.openeo.geotrellis.ProjectedPolygons.fromWkt(polygon_wkts, polygons_srs)
    elif 1 <= len(args) <= 2 and isinstance(args[0], (Polygon, MultiPolygon)):
        # Single polygon
        polygon_wkts = [str(args[0])]
        polygons_srs = args[1] if len(args) >= 2 else 'EPSG:4326'
        return jvm.org.openeo.geotrellis.ProjectedPolygons.fromWkt(polygon_wkts, polygons_srs)
    else:
        raise ValueError(args)


@contextlib.contextmanager
def zk_client(hosts: str = ','.join(ConfigParams().zookeepernodes)):
    zk = KazooClient(hosts)
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

def kube_client():
    from kubernetes import client, config
    config.load_incluster_config()
    api_instance = client.CustomObjectsApi()
    return api_instance

def s3_client():
    import boto3
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
    swift_url = os.environ.get("SWIFT_URL")
    s3_client = boto3.client("s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=swift_url)
    return s3_client

def download_s3_dir(bucketName, directory):
    import boto3

    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
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

def truncate_job_id_k8s(job_id):
    return job_id.split('-')[1][:10]

def truncate_user_id_k8s(user_id):
    return user_id.split('@')[0][:20]

