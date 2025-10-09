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


class JarVariants:
    def __init__(self, jars_json_path: Path):
        logger.info(f"Loading JAR variants from {jars_json_path}")
        with jars_json_path.open(mode="r", encoding="utf8") as f:
            data = json.load(f)
        self._default = data["default"]
        self._variants = data["variants"]
        assert self._default in self._variants
        for variant, jars in self._variants.items():
            assert (
                isinstance(jars, list) and len(jars) > 0 and all(isinstance(j, str) for j in jars)
            ), f"Must be non-empty list of strings: {jars!r}"
        logger.info(f"Loaded variants {self.get_variants()} with default {self._default!r}")

    def default_variant(self) -> str:
        return self._default

    def get_variants(self) -> list[str]:
        return sorted(self._variants.keys())

    def get_jars(self, variant: str) -> list[str]:
        return self._variants[variant]


def main():
    logging.basicConfig(level=logging.INFO)

    jar_variants = JarVariants(JARS_SOURCE_FILE)

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
        "--variant",
        choices=jar_variants.get_variants(),
        default=jar_variants.default_variant(),
        help="Variant of JAR set to download (default: %(default)r).",
    )
    # TODO: eliminate this deprecated "python-version" option
    parser.add_argument(
        "-p",
        "--python-version",
        choices=["3.8", "3.11"],
        default=None,
        help="[DEPRECATED] Python version to download jars for.",
    )

    cli_args = parser.parse_args()
    logger.info(f"{cli_args=}")
    jar_dir: Path = cli_args.jar_dir.absolute()
    force_download = cli_args.force_download
    python_version = cli_args.python_version
    variant = cli_args.variant

    logger.info(f"Using target JAR dir: {jar_dir}")
    jar_dir.mkdir(parents=True, exist_ok=True)

    if python_version:
        logger.warning("The --python-version option is deprecated, use --variant instead.")
        variant = f"python{python_version}"

    logger.info(f"Looking up JAR URLs for variant {variant!r}")
    jar_urls = jar_variants.get_jars(variant)
    logger.info(f"Found {len(jar_urls)} JAR URLs for {variant!r} in {JARS_SOURCE_FILE}:")
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
