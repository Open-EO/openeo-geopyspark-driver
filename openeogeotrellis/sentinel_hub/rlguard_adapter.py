import json
import sys
from os import environ
from sys import argv

# TODO: get this from a common package instead
from .rlguard import apply_for_request, calculate_processing_units, OutputFormat


def calculate_delay(request_params):
    request_params['output_format'] = OutputFormat(request_params['output_format'])

    pu = calculate_processing_units(**request_params)
    return apply_for_request(pu)


if __name__ == '__main__':
    if not environ.get('ZOOKEEPER_HOSTS'):
        print(f"Error: environment variable ZOOKEEPER_HOSTS is not set", file=sys.stderr)
        exit(1)

    if len(argv) < 2:
        print(f"Usage: {argv[0]} <request params JSON>", file=sys.stderr)
        exit(1)

    delay = calculate_delay(json.loads(argv[1]))
    print(json.dumps({'delay_s': delay}))
