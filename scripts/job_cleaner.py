import argparse
import json
import logging
import os
from datetime import datetime, timedelta
from openeo.util import TimingLogger
from openeo_driver.jobregistry import ElasticJobRegistry, get_ejr_credentials_from_env
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.configparams import ConfigParams

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger("openeogeotrellis.cleaner")

def main():
    assert os.environ.get("OPENEO_EJR_OIDC_CLIENT_CREDENTIALS") is not None, "OPENEO_EJR_OIDC_CLIENT_CREDENTIALS is not set"
    assert os.environ.get("OPENEO_EJR_API") is not None, "OPENEO_EJR_API is not set"
    batch_job_output_root = ConfigParams().batch_job_output_root
    assert os.path.exists(batch_job_output_root), f"{batch_job_output_root=} does not exist"

    # Parse command line arguments.
    parser = argparse.ArgumentParser(usage="OpenEO Cleaner", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--max_count",
        type=int,
        default=1000,
        help="Maximum number of jobs to retrieve for deletion",
    )
    parser.add_argument(
        "--ejr-backend-id",
        type=str,
        default="mep-dev",
        help="EJR backend ID to connect to. One of mep-dev, mep-integrationtests, mep-prod",
    )
    parser.add_argument(
        "--max_age_days",
        type=int,
        default=90,
        help="Maximum age of jobs to keep, in days. Anything older will be deleted.",
    )
    # batch size
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of jobs to process in each batch",
    )
    parser.add_argument(
        "--failed-to-delete-file-path",
        type=str,
        default=None,
        help="Optional path to file where failed job IDs will be logged. For example: /data/projects/OpenEO/{ejr_backend_id}_failed_to_delete_jobs.jsonl",
    )
    args = parser.parse_args()
    max_count: int = args.max_count
    ejr_backend_id: str = args.ejr_backend_id
    max_age_days: int = args.max_age_days
    batch_size: int = args.batch_size
    failed_to_delete_path: str = args.failed_to_delete_file_path
    ejr_api: str = os.environ.get("OPENEO_EJR_API")
    dry_run = False
    user_ids = None
    failed_job_ids = []

    upper = datetime.today() - timedelta(days=max_age_days)

    _log.info(f"Starting job cleanup: {max_count=} {ejr_backend_id=} {max_age_days=} days ({upper=}) {batch_size=}")

    # 1. Set up Elastic job registry.
    registry = ElasticJobRegistry(
        api_url=ejr_api,
        backend_id=ejr_backend_id,
    )
    ejr_creds = get_ejr_credentials_from_env(strict=True)
    registry.setup_auth_oidc_client_credentials(credentials=ejr_creds)

    batch_jobs = GpsBatchJobs(
        catalog=None,
        jvm=None,
        principal="",
        key_tab="",
        vault=None,
        elastic_job_registry=registry,
    )
    processed = 0
    while processed < max_count:
        remaining = max_count - processed
        current_batch_size = min(batch_size, remaining)

        # 2. Retrieve jobs to delete (next batch)
        with TimingLogger(title=f"Retrieving up to {current_batch_size} batch jobs before {upper}", logger=_log):
            with TimingLogger(
                    title=f"Collecting jobs to delete: {upper=} {user_ids=} batch_size={current_batch_size}",
                    logger=_log,
            ):
                jobs_before = registry.get_all_started_jobs_before(
                    upper,
                    user_ids=user_ids,
                    max_count=current_batch_size,
                )

            _log.info(f"Collected {len(jobs_before)} jobs to delete")

        # No more jobs to process
        if not jobs_before:
            _log.info("No more jobs found to delete")
            break

        # 3. Delete jobs in this batch
        successful_deletions = 0
        with TimingLogger(title=f"Deleting {len(jobs_before)} jobs", logger=_log):
            for job_info in jobs_before:
                job_id = job_info["job_id"]
                user_id = job_info["user_id"]
                updated = job_info["updated"]  # e.g. 2026-01-21T13:46:25Z

                if dry_run:
                    _log.info(f"[DRY RUN] Would delete {job_id=} from {user_id=}, last updated at {updated=}")
                    continue

                _log.info(f"Deleting {job_id=} from {user_id=}, last updated at {updated=}")
                try:
                    batch_jobs.cleanup_job_resources(
                        job_id,
                        user_id,
                        propagate_errors=True,
                        delete_dependency_sources=False,
                    )
                    registry.delete_job(
                        job_id=job_id,
                        user_id=user_id,
                        verify_deletion=False,
                    )
                    successful_deletions += 1
                except Exception as e:
                    # If something goes wrong when deleting the batch job directory, it requires manual intervention.
                    _log.error(f"Error deleting job {job_id}: {e}", exc_info=e)
                    failed_job_ids.append(job_id)
                    if failed_to_delete_path:
                        # Write failed job_id to file immediately
                        with open(failed_to_delete_path, "a") as f:
                            f.write(json.dumps({"job_id": job_id}) + "\n")

        processed += successful_deletions
        _log.info(f"Processed {processed}/{max_count} jobs so far (successful deletions in this batch: {successful_deletions}/{len(jobs_before)})")

    if failed_job_ids:
        # Exit with special code to indicate some jobs failed to delete.
        _log.error(f"Failed to delete {len(failed_job_ids)} jobs. See {failed_to_delete_path} for details.")
        exit(2)
    else:
        _log.info("All jobs processed successfully.")

if __name__ == '__main__':
    main()
