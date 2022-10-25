import argparse
from datetime import datetime, timedelta
import logging
import kazoo.client
from py4j.java_gateway import JavaGateway, JVMView

import openeogeotrellis.backend
from openeogeotrellis.backend import GpsBatchJobs, GpsSecondaryServices
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.service_registry import ZooKeeperServiceRegistry

logging.basicConfig(level=logging.INFO)
openeogeotrellis.backend.logger.setLevel(logging.DEBUG)
kazoo.client.log.setLevel(logging.WARNING)

_log = logging.getLogger(__name__)


def remove_batch_jobs_before(upper: datetime, jvm: JVMView) -> None:
    _log.info("removing batch jobs before {d}...".format(d=upper))

    # TODO: how to cope with unneeded arguments?
    batch_jobs = GpsBatchJobs(catalog=None, jvm=jvm, principal="", key_tab="", vault=None)
    batch_jobs.delete_jobs_before(upper)


def remove_secondary_services_before(upper: datetime) -> None:
    _log.info("removing secondary services before {d}...".format(d=upper))

    secondary_services = GpsSecondaryServices(ZooKeeperServiceRegistry())
    secondary_services.remove_services_before(upper)


def main():
    _log.info("ConfigParams(): {c}".format(c=ConfigParams()))

    parser = argparse.ArgumentParser(usage="OpenEO Cleaner", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--py4j-jarpath", default="venv/share/py4j/py4j0.10.7.jar", help='Path to the Py4J jar')
    parser.add_argument("--py4j-classpath", default="geotrellis-extensions-2.2.0-SNAPSHOT.jar",
                        help='Classpath used to launch the Java Gateway')
    parser.add_argument("--py4j-maximum-heap-size", default="1G",
                        help='Maximum heap size for the Java Gateway JVM')

    args = parser.parse_args()

    java_opts = [
        "-client",
        f"-Xmx{args.py4j_maximum_heap_size}",
        "-Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService"
    ]

    java_gateway = JavaGateway.launch_gateway(jarpath=args.py4j_jarpath,
                                              classpath=args.py4j_classpath,
                                              javaopts=java_opts,
                                              die_on_exit=True)

    max_date = datetime.today() - timedelta(days=60)

    remove_batch_jobs_before(max_date, java_gateway.jvm)
    remove_secondary_services_before(max_date)


if __name__ == '__main__':
    main()
