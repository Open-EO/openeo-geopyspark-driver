import functools
import logging
from typing import Dict, List, Optional, Union

import requests

from openeo.util import deep_get
from openeo_driver.util.http import requests_with_retry

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.opensearch import OpenSearch, OpenSearchOscars, OpenSearchCreodias, OpenSearchCdse
from openeogeotrellis.utils import dict_merge_recursive

logger = logging.getLogger(__name__)

# Type annotation aliases (mirrors layercatalog.py)
CollectionId = str
CollectionMetadataDict = Dict[str, Union[str, dict, list]]
CatalogDict = Dict[CollectionId, CollectionMetadataDict]


def enrich_catalog_metadata(metadata: CatalogDict) -> CatalogDict:
    """
    Enrich catalog metadata from external sources (OpenSearch, STAC, Sentinel Hub).

    For each collection in *metadata*, the function inspects the ``_vito.data_source``
    block and fetches additional metadata from the appropriate upstream source:

    * **OpenSearch** (OSCARS / Creodias / CDSE) – when ``opensearch_collection_id`` is set.
    * **STAC** – when ``type == "stac"`` and a ``url`` is provided.
    * **Sentinel Hub** – when ``type == "sentinel-hub"``, using the EuroDataCube STAC index.

    The enrichment metadata is merged *below* the existing catalog entries (i.e. catalog
    values take precedence) via :func:`dict_merge_recursive`.

    :param metadata: Catalog dict keyed on collection id.
    :return: Enriched catalog dict (may be a new dict object).
    """
    enrichment_metadata: CatalogDict = {}
    sh_collection_metadatas = None

    @functools.lru_cache
    def opensearch_instance(endpoint: str, variant: Optional[str] = None) -> OpenSearch:
        endpoint = endpoint.lower()

        if "oscars" in endpoint or "terrascope" in endpoint or "vito.be" in endpoint or variant == "oscars":
            return OpenSearchOscars(endpoint=endpoint)
        elif "creodias" in endpoint or variant == "creodias":
            return OpenSearchCreodias(endpoint=endpoint)
        elif "dataspace.copernicus.eu" in endpoint or variant == "cdse":
            return OpenSearchCdse(endpoint=endpoint)
        else:
            raise ValueError(endpoint)

    for cid, collection_metadata in metadata.items():
        data_source = deep_get(collection_metadata, "_vito", "data_source", default={})
        os_cid = data_source.get("opensearch_collection_id")
        os_endpoint = data_source.get("opensearch_endpoint") or get_backend_config().default_opensearch_endpoint
        os_variant = data_source.get("opensearch_variant")
        data_source_type = data_source.get("type")

        if os_cid and os_endpoint and os_variant != "disabled":
            logger.debug(f"Enrich {cid=} ({data_source_type=}): {os_cid=} {os_endpoint=}")
            try:
                enrichment_metadata[cid] = opensearch_instance(
                    endpoint=os_endpoint, variant=os_variant
                ).get_metadata(collection_id=os_cid)
            except Exception as e:
                logger.warning(f"Failed to enrich collection metadata of {cid}: {e}", exc_info=True)

        elif data_source_type == "stac":
            stac_url = data_source.get("url")
            logger.debug(f"Enrich {cid=} ({data_source_type=}): {stac_url=}")
            try:
                enrichment_metadata[cid] = _enrichment_metadata_from_stac(
                    stac_url,
                    band_aliases=data_source.get("enrichment_band_aliases"),
                )
            except Exception as e:
                logger.warning(f"Failed to enrich collection metadata of {cid}: {e}", exc_info=True)

        elif data_source_type == "sentinel-hub":
            sh_stac_endpoint = "https://collections.eurodatacube.com/stac/index.json"
            logger.debug(f"Enrich {cid=} ({data_source_type=}): {sh_stac_endpoint=}")

            # TODO: improve performance by only fetching necessary STACs
            if sh_collection_metadatas is None:
                sh_collections_session = requests_with_retry()
                sh_collections_resp = sh_collections_session.get(sh_stac_endpoint, timeout=60)
                sh_collections_resp.raise_for_status()
                sh_collection_metadatas = {
                    c["id"]: sh_collections_session.get(c["link"], timeout=60).json()
                    for c in sh_collections_resp.json()
                }

            enrichment_id = data_source.get("enrichment_id")

            # DEM collections have the same datasource_type "dem" so they need an explicit enrichment_id
            if enrichment_id:
                sh_metadata = sh_collection_metadatas[enrichment_id]
            else:
                sh_cid = data_source.get("dataset_id")

                # PLANETSCOPE doesn't have one so don't try to enrich it
                if sh_cid is None:
                    continue

                sh_metadatas = [m for _, m in sh_collection_metadatas.items() if m["datasource_type"] == sh_cid]

                if len(sh_metadatas) == 0:
                    logger.warning(f"No STAC data available for collection with id {sh_cid}")
                    continue
                elif len(sh_metadatas) > 1:
                    logger.warning(f"{len(sh_metadatas)} candidates for STAC data for collection with id {sh_cid}")
                    continue

                sh_metadata = sh_metadatas[0]

            enrichment_metadata[cid] = sh_metadata
            if not data_source.get("endpoint"):
                endpoint = enrichment_metadata[cid]["providers"][0]["url"]
                endpoint = endpoint if endpoint.startswith("http") else "https://{}".format(endpoint)
                data_source["endpoint"] = endpoint
            data_source["dataset_id"] = data_source.get("dataset_id") or enrichment_metadata[cid]["datasource_type"]

    if enrichment_metadata:
        metadata = dict_merge_recursive(enrichment_metadata, metadata, overwrite=True)

    return metadata


def _enrichment_metadata_from_stac(stac_url: str, *, band_aliases: Optional[Dict[str, List[str]]] = None) -> dict:
    """Fetch and normalise STAC collection metadata for enrichment purposes."""
    resp = requests.get(url=stac_url, timeout=10)
    resp.raise_for_status()
    metadata: dict = resp.json()

    # Normalize band metadata a bit (as there are multiple ways to specify bands in STAC):
    bands_from_cube_dimensions = deep_get(metadata, "cube:dimensions", "bands", "values", default=None)
    if bands := deep_get(metadata, "summaries", "bands", default=None):
        bands_from_summaries_bands = [b["name"] for b in bands]
    else:
        bands_from_summaries_bands = None
    if bands := deep_get(metadata, "summaries", "eo:bands", default=None):
        bands_from_summaries_eobands = [b["name"] for b in bands]
    else:
        bands_from_summaries_eobands = None

    if bands_from_cube_dimensions is None and (bands_from_summaries_bands or bands_from_summaries_eobands):
        metadata.setdefault("cube:dimensions", {})["bands"] = {
            "type": "bands",
            "values": bands_from_summaries_bands or bands_from_summaries_eobands,
        }

    # Inject band aliases (which is non-standard STAC at the moment)
    if band_aliases:
        if bands := deep_get(metadata, "summaries", "bands", default=None):
            for band in bands:
                if aliases := band_aliases.get(band["name"]):
                    band.setdefault("aliases", []).extend(aliases)
        if bands := deep_get(metadata, "summaries", "eo:bands", default=None):
            for band in bands:
                if aliases := band_aliases.get(band["name"]):
                    band.setdefault("aliases", []).extend(aliases)

    return metadata