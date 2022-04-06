import json
import logging
import sys
import traceback
from sys import argv
from typing import Optional

import kazoo.client

from openeogeotrellis.utils import zk_client

from rlguard import apply_for_request, calculate_processing_units, OutputFormat, SyncerDownException
from rlguard.repository import ZooKeeperRepository

REQUEST_DELAY = 'delay'
REQUEST_ACCESS_TOKEN = 'access_token'

logging.basicConfig(level=logging.DEBUG)
kazoo.client.log.setLevel(logging.WARNING)


def zookeeper_repository(zk) -> ZooKeeperRepository:
    return ZooKeeperRepository(zk, key_base="/openeo/rlguard")


def calculate_delay(request_params) -> float:
    request_params['output_format'] = OutputFormat(request_params['output_format'])

    pu = calculate_processing_units(**request_params)

    with zk_client() as zk:
        delay = apply_for_request(pu, zookeeper_repository(zk))

    return delay


def fetch_access_token() -> Optional[dict]:
    with zk_client() as zk:
        return zookeeper_repository(zk).get_access_token()


def main():
    if len(argv) < 2:
        print(f"Usage: {argv[0]} <request JSON>", file=sys.stderr)
        exit(1)

    request = json.loads(argv[1])
    request_id = request.get('request_id')

    if request_id in [REQUEST_DELAY, None]:
        try:
            request_params = request['arguments'] if request_id else request
            delay = calculate_delay(request_params)
            print(json.dumps({'delay_s': delay}))
        except SyncerDownException:
            print(json.dumps({'error': traceback.format_exc()}))
            # If this happens, retries should be handled manually, with exponential backoff (but limited to the
            # time it takes to fill the offending bucket from empty to full).
    elif request_id == REQUEST_ACCESS_TOKEN:
        print(json.dumps({'access_token': fetch_access_token()}))
    else:
        raise ValueError(f'unsupported request_id "{request_id}"')


if __name__ == '__main__':
    main()
