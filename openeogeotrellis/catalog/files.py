import argparse
import gzip
import json
import logging
import sys
import zipfile
from pathlib import Path
from typing import List, Union

from openeo.util import TimingLogger
from openeo_driver.utils import read_json

from openeogeotrellis.util.datastructures import dict_merge_recursive

from .common_name import merge_layers_with_common_name
from .enrich import CatalogDict, enrich_catalog_metadata

logger = logging.getLogger(__name__)


def read_catalog_file(path: Union[str, Path]) -> CatalogDict:
    path = Path(path)
    try:
        if path.is_file() and path.name.lower().endswith(".json"):
            return {coll["id"]: coll for coll in read_json(path)}
        elif path.is_file() and path.name.lower().endswith(".json.gz"):
            with gzip.open(path, mode="rt", encoding="utf-8") as f:
                return {coll["id"]: coll for coll in json.load(fp=f)}
        elif path.is_file() and path.name.lower().endswith(".zip"):
            catalog = {}
            with zipfile.ZipFile(path, mode="r") as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".json"):
                        with zf.open(name, mode="r") as f:
                            data = json.load(f)
                        if isinstance(data, dict):
                            # File with single collection
                            catalog[data["id"]] = data
                        elif isinstance(data, list):
                            # File with list of collections
                            catalog.update({coll["id"]: coll for coll in data})
                        else:
                            logger.warning(f"Skipping catalog source {path!r}/{name!r}: unexpected {type(data)=}")
            return catalog
        else:
            raise ValueError(f"Unsupported catalog format {path=}")
    except Exception as e:
        raise ValueError(f"Failed to read layer catalog from {path=}: {e=}") from e


@TimingLogger(title="load_catalog_files", logger=logger.info)
def load_catalog_files(
    catalog_files: List[str],
    enrich_metadata: bool,
    *,
    default_opensearch_endpoint: Union[str, None] = None,
) -> CatalogDict:
    """
    Build layer catalog from JSON files (possibly compressed)
    """
    metadata: CatalogDict = {}

    logger.debug(f"load_catalog_files: {catalog_files=}")
    for path in catalog_files:
        logger.debug(f"load_catalog_files: reading {path}")
        metadata = dict_merge_recursive(metadata, read_catalog_file(path), overwrite=True)
        logger.debug(f"load_catalog_files: collected {len(metadata)} collections")

    logger.debug(f"load_catalog_files: {enrich_metadata=}")
    if enrich_metadata:
        metadata = enrich_catalog_metadata(metadata, default_opensearch_endpoint=default_opensearch_endpoint)

    metadata = merge_layers_with_common_name(metadata)

    return metadata


def dump_layer_catalog():
    """CLI tool to dump layer catalog with enrichment"""
    cli = argparse.ArgumentParser()
    cli.add_argument("--enrich", action="store_true", help="Enable metadata enrichment.")
    cli.add_argument(
        "--catalog-file",
        action="append",
        required=True,
        help="Path to catalog JSON file. Can be specified multiple times.",
    )
    cli.add_argument(
        "--default-opensearch-endpoint",
        help="OpenSearch endpoint to use for collections that don't specify one explicitly.",
    )
    cli.add_argument(
        "--container",
        choices=["list", "dict"],
        default="list",
        help="Top level container to list the collections in: a list like in openEO API, or a dict, keyed on collection id.",
    )
    cli.add_argument("--verbose", action="store_true")
    arguments = cli.parse_args()

    logging.basicConfig(level=logging.DEBUG if arguments.verbose else logging.DEBUG)

    metadata = load_catalog_files(
        catalog_files=arguments.catalog_file,
        enrich_metadata=arguments.enrich,
        default_opensearch_endpoint=arguments.default_opensearch_endpoint,
    )
    if arguments.container == "list":
        metadata = list(metadata.values())
    json.dump(metadata, fp=sys.stdout, indent=2)


if __name__ == "__main__":
    dump_layer_catalog()
