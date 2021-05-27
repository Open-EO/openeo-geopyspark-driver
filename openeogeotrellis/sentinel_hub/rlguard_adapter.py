import json
import os
import sys
from sys import argv

from openeogeotrellis.configparams import ConfigParams

# TODO: import of module rlguard depends on this
os.environ['ZOOKEEPER_HOSTS'] = ",".join(ConfigParams().zookeepernodes)


def calculate_delay(request_params):
    # TODO: build a common package and impor from there instead
    from .rlguard import apply_for_request, calculate_processing_units, OutputFormat

    request_params['output_format'] = OutputFormat(request_params['output_format'])

    pu = calculate_processing_units(**request_params)
    return apply_for_request(pu)


if __name__ == '__main__':
    if len(argv) < 2:
        print(f"Usage: {argv[0]} <request params JSON>", file=sys.stderr)
        exit(1)

    delay = calculate_delay(json.loads(argv[1]))
    print(json.dumps({'delay_s': delay}))
