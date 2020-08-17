from datetime import datetime, timedelta
import logging

from openeogeotrellis.backend import GpsBatchJobs

logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    max_date = datetime.today() - timedelta(days=60)

    batch_jobs = GpsBatchJobs()
    batch_jobs.delete_jobs_before(max_date)
