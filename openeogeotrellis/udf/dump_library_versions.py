"""
Simple CLI utility to dump the versions of provided libraries
(as available in current environment)
in a format that's compatible with `ContainerImageRecord.parse_udf_runtime_libraries`
"""

import argparse
import json
import logging

from openeo_driver.utils import get_package_version

_log = logging.getLogger(__name__)


class OUTPUT_FORMAT:
    JSON_VERBOSE = "json-verbose"
    JSON_ONELINE = "json-compact"


def main(argv=None) -> None:
    cli = argparse.ArgumentParser()
    cli.add_argument(
        "libraries",
        nargs="*",
        default=["openeo", "numpy", "xarray"],
        help="Libraries to dump version for. Can be given as separate arguments or comma-separated.",
    )
    cli.add_argument(
        "--format",
        choices=[OUTPUT_FORMAT.JSON_VERBOSE, OUTPUT_FORMAT.JSON_ONELINE],
        default=OUTPUT_FORMAT.JSON_VERBOSE,
        help="Output format (default: %(default)r)",
    )
    cli_args = cli.parse_args(argv)

    libraries = sorted({lib.strip() for libs in cli_args.libraries for lib in libs.split(",")})
    _log.info(f"Looking up versions of {libraries=}")

    data = {}
    for lib in libraries:
        version = get_package_version(lib)
        if version:
            data[lib] = version
        else:
            _log.warning(f"No version found for {lib!r}")

    # TODO: options for YAML or single-line JSON output?

    if cli_args.format == OUTPUT_FORMAT.JSON_ONELINE:
        json_kwargs = {"separators": (",", ":")}
    elif cli_args.format == OUTPUT_FORMAT.JSON_VERBOSE:
        json_kwargs = {"indent": 2}
    else:
        raise ValueError(cli_args.format)

    print(json.dumps(data, **json_kwargs))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
