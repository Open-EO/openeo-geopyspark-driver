"""
Simple CLI utility to dump the versions of provided libraries
(as available in current environment)
in a format that's compatible with ContainerImageRecord
"""

import argparse
import logging
import json

from openeo_driver.utils import get_package_version


_log = logging.getLogger(__name__)


def main(argv=None) -> None:
    cli = argparse.ArgumentParser()
    cli.add_argument(
        "libraries",
        nargs="*",
        default=["openeo", "numpy", "xarray"],
        help="Libraries to dump version for. Can be given as separate arguments or comma-separated.",
    )
    args = cli.parse_args(argv)

    libraries = {lib.strip() for libs in args.libraries for lib in libs.split(",")}
    _log.info(f"Looking up versions of {libraries=}")

    data = {}
    for lib in libraries:
        version = get_package_version(lib)
        if version:
            data[lib] = version
        else:
            _log.warning(f"No version found for {lib!r}")

    # TODO: options for YAML or single-line JSON output?
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
