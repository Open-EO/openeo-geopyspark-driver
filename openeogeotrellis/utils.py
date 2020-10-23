import collections
import grp
import logging
import os
from pathlib import Path
import pwd
import stat
from typing import Union
import contextlib

from dateutil.parser import parse
from py4j.java_gateway import JavaGateway
import pytz
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from kazoo.client import KazooClient
from .configparams import ConfigParams

from openeo_driver.delayed_vector import DelayedVector

logger = logging.getLogger("openeo")

def log_memory(function):
    def memory_logging_wrapper(*args, **kwargs):
        try:
            from spark_memlogger import memlogger
        except ImportError:
            return function(*args, **kwargs)

        ml = memlogger.MemLogger(5,api_version_auto_timeout_ms=60000)
        try:
            try:
                ml.start()
            except:
                logger.warning("Error while configuring memory logging, will not be available!", exc_info=True)
            return function(*args, **kwargs)
        finally:
            ml.stop()

    return memory_logging_wrapper

def kerberos():
    import geopyspark as gps

    if 'HADOOP_CONF_DIR' not in os.environ:
        logger.warning('HADOOP_CONF_DIR is not set. Kerberos based authentication will probably not be set up correctly.')

    sc = gps.get_spark_context()
    gateway = JavaGateway(gateway_parameters=sc._gateway.gateway_parameters)
    jvm = gateway.jvm

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

    principal = sc.getConf().get("spark.yarn.principal")
    sparkKeytab = sc.getConf().get("spark.yarn.keytab")
    if principal is not None and sparkKeytab is not None:
        jvm.org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principal, sparkKeytab)
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


def normalize_date(date_string):
    if date_string is not None:
        date = parse(date_string)
        if date.tzinfo is None:
            date = date.replace(tzinfo=pytz.UTC)
        return date.isoformat()
    return None


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
        polygon_wkts = [str(x) for x in args[0]]
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


def set_max_memory(spark_conf):
    from py4j.protocol import Py4JJavaError
    import resource

    try:
        max_total_memory_in_bytes = spark_conf._jconf.getSizeAsBytes("spark.driver.memoryOverhead")
        soft_limit, hard_limit = max_total_memory_in_bytes, max_total_memory_in_bytes
        resource.setrlimit(resource.RLIMIT_AS, (soft_limit, hard_limit))

        print("set resource.RLIMIT_AS to %d bytes" % max_total_memory_in_bytes)
    except Py4JJavaError as e:
        java_exception_class_name = e.java_exception.getClass().getName()

        if java_exception_class_name != 'java.util.NoSuchElementException':
            raise e
