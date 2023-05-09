import argparse
import logging
from datetime import datetime, timedelta
from typing import Optional, List

import kazoo.client
from py4j.java_gateway import JavaGateway, JVMView, find_jar_path

import openeogeotrellis.backend
from openeo.util import TimingLogger
from openeogeotrellis.backend import GpsBatchJobs, GpsSecondaryServices
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.service_registry import ZooKeeperServiceRegistry

logging.basicConfig(level=logging.INFO)
openeogeotrellis.backend.logger.setLevel(logging.DEBUG)
kazoo.client.log.setLevel(logging.WARNING)

_log = logging.getLogger("openeogeotrellis.cleaner")


def remove_batch_jobs_before(
    upper: datetime,
    jvm: JVMView,
    *,
    user_ids: Optional[List[str]] = None,
    dry_run: bool = True,
    include_ongoing: bool = True,
    include_done: bool = True,
    user_limit: Optional[int] = 1000,
) -> None:
    with TimingLogger(title=f"Removing batch jobs before {upper}", logger=_log):
        # TODO: how to cope with unneeded arguments?
        batch_jobs = GpsBatchJobs(
            catalog=None, jvm=jvm, principal="", key_tab="", vault=None
        )
        batch_jobs.delete_jobs_before(
            upper,
            user_ids=user_ids,
            dry_run=dry_run,
            include_ongoing=include_ongoing,
            include_done=include_done,
            user_limit=user_limit,
        )


def remove_secondary_services_before(upper: datetime) -> None:
    with TimingLogger(title=f"Removing secondary services before {upper}", logger=_log):
        secondary_services = GpsSecondaryServices(ZooKeeperServiceRegistry())
        secondary_services.remove_services_before(upper)


def main():
    _log.info("ConfigParams(): {c}".format(c=ConfigParams()))

    parser = argparse.ArgumentParser(usage="OpenEO Cleaner", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    default_py4j_jarpath = find_jar_path() or "venv/share/py4j/py4j0.10.7.jar"
    parser.add_argument(
        "--py4j-jarpath", default=default_py4j_jarpath, help="Path to the Py4J jar"
    )
    parser.add_argument("--py4j-classpath", default="geotrellis-extensions-2.2.0-SNAPSHOT.jar",
                        help='Classpath used to launch the Java Gateway')
    parser.add_argument("--py4j-maximum-heap-size", default="1G",
                        help='Maximum heap size for the Java Gateway JVM')
    parser.add_argument(
        "--user",
        action="append",
        help="Only clean up for specified user id. Can be specified multiple times",
    )
    parser.add_argument(
        "--min-age",
        type=int,
        default=30,
        help="Minimum age in days for jobs to clean up",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--skip-done",
        action="store_true",
        help="Don't clean up `done` jobs",
    )
    parser.add_argument(
        "--skip-ongoing",
        action="store_true",
        help="Don't clean up `ongoing` jobs",
    )
    parser.add_argument(
        "--jobs-per-user-limit",
        type=int,
        default=1000,
        help="Maximum number of jobs per user to consider for deletion. Allows keeping the overall execution time reasonable when there are users with excessive number of jobs.",
    )

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

    max_date = datetime.today() - timedelta(days=args.min_age)
    _log.info(f"Cleaner: min age {args.min_age} days -> before {max_date}")
    if args.user:
        user_ids = args.user
        _log.info(f"Cleaner: for users {user_ids}")
    else:
        user_ids = None

    remove_batch_jobs_before(
        upper=max_date,
        jvm=java_gateway.jvm,
        user_ids=user_ids,
        dry_run=args.dry_run,
        include_ongoing=not args.skip_ongoing,
        include_done=not args.skip_done,
        user_limit=args.jobs_per_user_limit,
    )
    remove_secondary_services_before(max_date)


if __name__ == '__main__':
    main()
