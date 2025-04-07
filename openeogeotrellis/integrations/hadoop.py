import logging
import os

from py4j.java_gateway import JVMView

from openeogeotrellis.utils import get_jvm

logger = logging.getLogger(__name__)


def setup_kerberos_auth(principal, key_tab, jvm: JVMView = None):
    logger.info("Doing setup_kerberos_auth")
    if jvm is None:
        jvm = get_jvm()

    if "HADOOP_CONF_DIR" not in os.environ:
        logger.warning(
            "HADOOP_CONF_DIR is not set. Kerberos based authentication will probably not be set up correctly."
        )

    hadoop_auth = jvm.org.apache.hadoop.conf.Configuration().get("hadoop.security.authentication")
    if hadoop_auth != "kerberos":
        logger.warning("Hadoop client does not have hadoop.security.authentication=kerberos.")

    currentUser = jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser()
    if currentUser.hasKerberosCredentials():
        return
    logger.info(
        "Kerberos currentUser={u!r} isSecurityEnabled={s!r}".format(
            u=currentUser.toString(), s=jvm.org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled()
        )
    )
    # print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getAuthenticationMethod().toString())

    if principal is not None and key_tab is not None:
        jvm.org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(principal, key_tab)
        jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().setAuthenticationMethod(
            jvm.org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS
        )
    # print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().toString())
    # loginUser = jvm.org.apache.hadoop.security.UserGroupInformation.getLoginUser()
    # print(loginUser.toString())
    # print(loginUser.hasKerberosCredentials())
    # currentUser.addCredentials(loginUser.getCredentials())
    # print(jvm.org.apache.hadoop.security.UserGroupInformation.getCurrentUser().hasKerberosCredentials())
