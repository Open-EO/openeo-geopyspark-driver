import collections
import logging
import os
import pytz
from dateutil.parser import parse

from py4j.java_gateway import JavaGateway

logger = logging.getLogger("openeo")


def kerberos():
    import geopyspark as gps

    if 'HADOOP_CONF_DIR' not in os.environ:
        logger.warn('HADOOP_CONF_DIR is not set. Kerberos based authentication will probably not be set up correctly.')

    sc = gps.get_spark_context()
    gateway = JavaGateway(gateway_parameters=sc._gateway.gateway_parameters)
    jvm = gateway.jvm

    hadoop_auth = jvm.org.apache.hadoop.conf.Configuration().get('hadoop.security.authentication')
    if hadoop_auth != 'kerberos':
        logger.warn('Hadoop client does not have hadoop.security.authentication=kerberos.')

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
