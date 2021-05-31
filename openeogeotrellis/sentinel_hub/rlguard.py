from enum import Enum
import json
from kazoo.client import KazooClient
import logging
import os

from .repository import ZooKeeperRepository

ZOOKEEPER_KEY_BASE = "/openeo/rlguard"
ZOOKEEPER_HOSTS = os.environ.get("ZOOKEEPER_HOSTS", "127.0.0.1:2181")
zk = KazooClient(hosts=ZOOKEEPER_HOSTS)
zk.start()
repository = ZooKeeperRepository(zk, ZOOKEEPER_KEY_BASE)


class SyncerDownException(Exception):
    pass


class PolicyType(Enum):
    PROCESSING_UNITS = "PU"
    REQUESTS = "RQ"


class OutputFormat(Enum):
    IMAGE_TIFF_DEPTH_32 = "tiff32"
    APPLICATION_OCTET_STREAM = "octet"
    OTHER = None


def calculate_processing_units(
    batch_processing: bool,
    width: int,
    height: int,
    n_input_bands_without_datamask: int,
    output_format: OutputFormat,
    n_data_samples: int = 1,
    s1_orthorectification: bool = False,
) -> float:
    # https://docs.sentinel-hub.com/api/latest/api/overview/processing-unit/
    pu = 1.0

    # Processing with batch processing API will result in a multiplication factor of 1/3.
    # Thus, three times more data can be processed comparing to process API for the same amount of PUs.
    if batch_processing:
        pu = pu / 3.0

    # The multiplication factor is calculated by dividing requested output (image) size (width x height) by 512 x 512.
    # The minimum value of this multiplication factor is 0.01. This corresponds to an area of 0.25 km2 for
    # Sentinel-2 data at 10 m spatial resolution.
    pu *= max((width * height) / (512.0 * 512.0), 0.01)

    # The multiplication factor is calculated by dividing the requested number of input bands by 3.
    # An exception is requesting dataMask which is not counted.
    pu *= n_input_bands_without_datamask / 3.0

    # Requesting 32 bit float TIFF will result in a multiplication factor of 2 due to larger memory consumption and data traffic.
    # Requesting application/octet-stream will result in a multiplication factor of 1.4 due to additional integration costs
    # (This is used for integration with external tools such as xcube.).
    if output_format == OutputFormat.IMAGE_TIFF_DEPTH_32:
        pu *= 2.0
    elif output_format == OutputFormat.APPLICATION_OCTET_STREAM:
        pu *= 1.4

    # The multiplication factor equals the number of data samples per pixel.
    pu *= n_data_samples

    # Requesting orthorectification (for S1 GRD data) will result in a multiplication factor of 2 due to additional
    # processing requirements (This rule is not applied at the moment.).
    # if s1_orthorectification:
    #     pu *= 2.

    # The minimal weight for a request is 0.001 PU.
    pu = max(pu, 0.001)
    return pu


def apply_for_request(processing_units: float) -> float:
    """
    Decrements & fetches the Redis counters, calculates the delay and returns it.

    If syncer service is down (detected by self-expiring key not being in Redis), raises
    `SyncerDownException`. If this exception is caught, worker should handle retries in
    conventional way (ideally exponential backoff, limited to the time it takes for the
    offending bucket to refill itself from 0 to full).
    """
    # figure out the types of the buckets so we know how much to decrement them:
    policy_refills = repository.get_policy_refills()
    policy_types = repository.get_policy_types()
    syncer_alive = repository.is_syncer_alive()

    logging.debug(f"Policy types: {policy_types}")
    logging.debug(f"Policy bucket refills: {policy_refills}ns")

    if not syncer_alive:
        raise SyncerDownException("Syncer service is down - revert to manual retries.")

    # decrement buckets according to their type:
    buckets_types_items = policy_types.items()
    new_remaining = []
    for policy_id, policy_type in buckets_types_items:
        new_value = repository.increment_counter(
            policy_id,
            -float(processing_units) if policy_type == PolicyType.PROCESSING_UNITS.value else -1.0
        )

        new_remaining.append(new_value)
    new_remaining = dict(zip([policy_id for policy_id, _ in buckets_types_items], new_remaining))

    logging.debug(f"Bucket values after decrementing them: {new_remaining}")
    wait_times_ns = [-new_remaining[policy_id] * float(policy_refills[policy_id]) for policy_id in new_remaining.keys()]
    logging.debug(f"Wait times in s for each policy: {[0 if ns < 0 else ns / 1000000000. for ns in wait_times_ns]}")
    delay_ns = max(wait_times_ns)
    if delay_ns < 0:
        return 0
    return delay_ns / 1000000000.0
