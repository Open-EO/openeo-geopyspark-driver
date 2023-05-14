#!/usr/bin/env python
"""

Script to download necessary JARS for running openeo-geopyspark-driver application (and tests)

To be used instead of `geopyspark install-jar`

"""

# Note that this script only uses stdlib modules,
# to be widely usable without need for pre-existing virtual envs
import argparse
import logging
import os
import subprocess
import urllib.request
from pathlib import Path

logger = logging.getLogger("get-jars")

ROOT_DIR = Path(__file__).parent.parent.absolute()
DEFAULT_JAR_DIR = ROOT_DIR / "jars"


def download_jar(jar_dir: Path, url: str, force: bool = False) -> Path:
    """Download JAR at given URL to JAR dir."""
    target = jar_dir / (url.split("/")[-1])
    if not target.exists() or force:
        logger.info(f"Downloading {url!r} to {target}")
        urllib.request.urlretrieve(url, target)
        assert target.exists()
        logger.info(f"Downloaded: {target} ({target.stat().st_size} bytes).")
    else:
        logger.info(f"Already exists: {target} ({target.stat().st_size} bytes).")
    return target


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "jar_dir",
        nargs="?",
        default=DEFAULT_JAR_DIR,
        type=Path,
        help="Path to JAR directory.",
    )
    parser.add_argument(
        "-f",
        "--force-download",
        action="store_true",
        help="Force download instead of skipping existing jars.",
    )

    cli_args = parser.parse_args()
    jar_dir: Path = cli_args.jar_dir
    force_download = cli_args.force_download

    logger.info(f"Using target JAR dir: {jar_dir}")
    jar_dir.mkdir(parents=True, exist_ok=True)

    for url in [
        # TODO: list these URLs in a simple text/CSV file so it can be consumed by other tools too?
        "https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.4.0_2.12-SNAPSHOT/geotrellis-extensions-2.4.0_2.12-apply_array.jar",
        "https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo_2.12.jar",
        "https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/openeo-logging/2.4.0_2.12-SNAPSHOT/openeo-logging-2.4.0_2.12-SNAPSHOT.jar",
    ]:
        download_jar(jar_dir, url=url, force=force_download)

    logger.info(f"Listing of {jar_dir}:")
    for f in jar_dir.iterdir():
        logger.info(f"{f.stat().st_size:16d} {f}")


if __name__ == '__main__':
    main()
