import logging
import os

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
