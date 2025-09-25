#!/usr/bin/env python
"""

Script to download necessary JARS for running openeo-geopyspark-driver application (and tests)

To be used instead of `geopyspark install-jar`

"""

# Note that this script only uses stdlib modules,
# to be widely usable without need for pre-existing virtual envs
import argparse
import logging
import json
import os
import subprocess
import urllib.request
from pathlib import Path

logger = logging.getLogger("get-jars")

ROOT_DIR = Path(__file__).parent.parent.absolute()
DEFAULT_JAR_DIR = ROOT_DIR / "jars"
JARS_SOURCE_FILE = ROOT_DIR / "scripts/jars.json"


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
    parser.add_argument(
        "-p",
        "--python-version",
        choices=["3.8", "3.11"],
        default="3.8",
        help="Python version to download jars for (default: current Python version).",
    )

    cli_args = parser.parse_args()
    logger.info(f"{cli_args=}")
    jar_dir: Path = cli_args.jar_dir
    force_download = cli_args.force_download
    python_version = cli_args.python_version

    logger.info(f"Using target JAR dir: {jar_dir}")
    jar_dir.mkdir(parents=True, exist_ok=True)

    jar_urls = []
    with open(JARS_SOURCE_FILE, "r") as f:
        jar_sources = json.load(f)
        jar_urls = jar_sources[f"python{python_version}"]
        assert isinstance(jar_urls, list)
        assert all(isinstance(u, str) for u in jar_urls)
        logger.info(f"Found {len(jar_urls)} JAR URLs in {JARS_SOURCE_FILE}:")
        for u in jar_urls:
            logger.info(f" - {u}")

    for url in jar_urls:
        download_jar(jar_dir, url=url, force=force_download)

    logger.info(f"Listing of {jar_dir}:")
    for f in jar_dir.iterdir():
        if f.is_symlink():
            # When developing it can be handy to symlink the generated jar.
            logger.info(f"Symlink: {f}")
        else:
            logger.info(f"{f.stat().st_size:16d} {f}")


if __name__ == '__main__':
    main()
