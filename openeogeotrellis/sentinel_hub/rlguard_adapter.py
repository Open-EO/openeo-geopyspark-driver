import json
import logging
import sys
import traceback
from sys import argv

import kazoo.client

from openeogeotrellis.utils import zk_client

from rlguard import apply_for_request, calculate_processing_units, OutputFormat, SyncerDownException
from rlguard.repository import ZooKeeperRepository

logging.basicConfig(level=logging.DEBUG)
kazoo.client.log.setLevel(logging.WARNING)


def request(request_params) -> (float, float):
    request_params['output_format'] = OutputFormat(request_params['output_format'])

    pu = calculate_processing_units(**request_params)

    with zk_client() as zk:
        zookeeper_repository = ZooKeeperRepository(zk, key_base="/openeo/rlguard")
        delay = apply_for_request(pu, zookeeper_repository)

        return pu, delay


def main():
    if len(argv) < 2:
        print(f"Usage: {argv[0]} <request params JSON>", file=sys.stderr)
        exit(1)

    request_params = json.loads(argv[1])

    try:
        pu, delay = request(request_params)
        print(json.dumps({'processing_units': pu, 'delay_s': delay}))
    except SyncerDownException:
        print(json.dumps({'error': traceback.format_exc()}))
        # If this happens, retries should be handled manually, with exponential backoff (but limited to the
        # time it takes to fill the offending bucket from empty to full).


if __name__ == '__main__':
    main()
