from datetime import datetime, timedelta
import logging
import kazoo.client

from openeogeotrellis.backend import GpsBatchJobs, GpsSecondaryServices
from openeogeotrellis.layercatalog import get_layer_catalog
from openeogeotrellis.service_registry import ZooKeeperServiceRegistry

logging.basicConfig(level=logging.INFO)
kazoo.client.log.setLevel(logging.WARNING)

_log = logging.getLogger(__name__)


def remove_batch_jobs_before(upper: datetime) -> None:
    _log.info("removing batch jobs before {d}...".format(d=upper))

    batch_jobs = GpsBatchJobs(get_layer_catalog(get_opensearch=None))
    batch_jobs.delete_jobs_before(upper)


def remove_secondary_services_before(upper: datetime) -> None:
    _log.info("removing secondary services before {d}...".format(d=upper))

    secondary_services = GpsSecondaryServices(ZooKeeperServiceRegistry())
    secondary_services.remove_services_before(upper)


if __name__ == '__main__':
    max_date = datetime.today() - timedelta(days=60)

    remove_batch_jobs_before(max_date)
    remove_secondary_services_before(max_date)
