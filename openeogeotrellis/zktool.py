"""
Some tools for inspection and management of the ZooKeeper job registry.
These are just ad-hoc, short-term stop-gap measures to implement some quick wins
trying to fight the ZooKeeper load issue.

TODO #236/#498/#632 remove when not necessary anymore (ZkJobRegistry is gone).


Usage instructions:

Use kubectl with port forwarding to access the desired ZooKeeper.

e.g. if the prod config is at ~/.kube/config-prod.yaml:

    KUBECONFIG=~/.kube/config-prod.yaml kubectl port-forward -n zookeeper-prod zookeeper-0 2181:2181


"""

import datetime
import logging
from typing import List, Union

from kazoo.client import KazooClient

from openeo.util import TimingLogger
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.utils import StatsReporter

_log = logging.getLogger("openeogeotrellis.zkpruner")


def main():
    # Note this "localhost:2181" assumes kubectl port-forwarding is set up as described above.
    zk_client = KazooClient(hosts="localhost:2181")
    zk_job_registry = ZkJobRegistry(
        root_path="/openeo/jobs",
        zk_client=zk_client,
    )

    stats(zk_job_registry=zk_job_registry)
    # prune(
    #     zk_job_registry=zk_job_registry,
    #     dry_run=False,
    #     user_ids=["cdse-ci-service-account"],
    #     per_user_limit=10,
    #     min_age_in_days=30,
    #     include_done=True,
    #     include_ongoing=True,
    # )


def stats(zk_job_registry: ZkJobRegistry):
    """
    Collect ZkJobRegistry stats: count users and jobs under "ongoing" and "done" paths.
    """
    with StatsReporter(report=_log.info) as stats, zk_job_registry:
        for mode, path_builder in [
            ("ongoing", zk_job_registry._ongoing),
            ("done", zk_job_registry._done),
        ]:
            user_ids = set(zk_job_registry._zk.get_children(path=path_builder()))
            _log.info(f"Collected {len(user_ids)=} {mode!r} user ids")
            stats[f"{mode} user ids"] = len(user_ids)

            user_jobs_max = 0
            for user_id in user_ids:
                jobs = len(zk_job_registry._zk.get_children(path=path_builder(user_id=user_id)))
                stats[f"{mode} jobs"] += jobs
                stats["total jobs"] += jobs
                if jobs > user_jobs_max:
                    user_jobs_max = jobs
                    _log.info(f"New max jobs per user ({mode}): {user_jobs_max} by {user_id}")


def prune(
    zk_job_registry: ZkJobRegistry,
    user_ids: Union[List[str], None] = None,
    per_user_limit: int = 100,
    dry_run: bool = True,
    min_age_in_days: int = 90,
    include_done: bool = True,
    include_ongoing: bool = False,
):
    with StatsReporter(report=_log.info) as stats, zk_job_registry:
        with TimingLogger(title="Collect jobs", logger=_log):
            jobs = zk_job_registry.get_all_jobs_before(
                upper=datetime.datetime.now() - datetime.timedelta(days=min_age_in_days),
                per_user_limit=per_user_limit,
                include_done=include_done,
                include_ongoing=include_ongoing,
                user_ids=user_ids,
            )
        _log.info(f"collected {len(jobs)=}")
        stats["collected jobs"] = len(jobs)
        for job in jobs:
            job_info = {k: job.get(k) for k in ["user_id", "job_id", "status", "created", "updated"]}
            _log.info(f"To prune: {job_info}")

        if dry_run:
            _log.info("Dry run, not pruning")
        with TimingLogger(title="Prune jobs", logger=_log):
            for job in jobs:
                user_id = job["user_id"]
                job_id = job["job_id"]
                if dry_run:
                    stats["dry-run pruning skips"] += 1
                else:
                    _log.info(f"Pruning {user_id=} {job_id=}")
                    zk_job_registry.delete(user_id=user_id, job_id=job_id)
                    stats["pruned jobs"] += 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with TimingLogger(title="zkpruner", logger=_log):
        main()
