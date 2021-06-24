import json
import sys
import traceback
from sys import argv

from openeogeotrellis.utils import zk_client

from rlguard import apply_for_request, calculate_processing_units, OutputFormat, SyncerDownException
from rlguard.repository import ZooKeeperRepository


def calculate_delay(request_params):
    request_params['output_format'] = OutputFormat(request_params['output_format'])

    with zk_client() as zk:
        zookeeper_repository = ZooKeeperRepository(zk, key_base="/openeo/rlguard")

        pu = calculate_processing_units(**request_params)
        return apply_for_request(pu, zookeeper_repository)


if __name__ == '__main__':
    if len(argv) < 2:
        print(f"Usage: {argv[0]} <request params JSON>", file=sys.stderr)
        exit(1)

    try:
        delay = calculate_delay(json.loads(argv[1]))
        print(json.dumps({'delay_s': delay}))
    except SyncerDownException as e:
        print(json.dumps({'error': traceback.format_exc()}))
        # If this happens, retries should be handled manually, with exponential backoff (but limited to the
        # time it takes to fill the offending bucket from empty to full).
