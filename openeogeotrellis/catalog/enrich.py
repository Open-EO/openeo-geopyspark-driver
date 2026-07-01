import collections
import functools
import logging
from typing import Callable, Dict, List, Optional, Union

import requests
from openeo.util import deep_get

from openeogeotrellis.catalog import DATA_SOURCE_PROPERTIES
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.opensearch import OpenSearch, OpenSearchCdse, OpenSearchCreodias, OpenSearchOscars
from openeogeotrellis.util.logging import TrackingIter
from openeogeotrellis.utils import dict_merge_recursive

logger = logging.getLogger(__name__)

# Type annotation aliases (mirrors layercatalog.py)
CollectionId = str
CollectionMetadataDict = Dict[str, Union[str, dict, list]]
CatalogDict = Dict[CollectionId, CollectionMetadataDict]
LinksList = List[dict]
LinksFilter = Callable[[LinksList], LinksList]


def enrich_catalog_metadata(
    metadata: CatalogDict,
    *,
    upstream_links_filter: Optional[LinksFilter] = None,
) -> CatalogDict:
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
    :param upstream_links_filter: optional filter function for upstream links, before merging with local links
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

    collection_iterator = TrackingIter()
    enrichment_stats = collections.defaultdict(int)

    for cid, collection_metadata in collection_iterator(metadata.items()):
        enrichment_stats[f"collection"] += 1
        data_source = deep_get(collection_metadata, "_vito", "data_source", default={})
        data_source_type = data_source.get("type")
        enrichment_stats[f"{data_source_type=}"] += 1

        os_cid = data_source.get("opensearch_collection_id")
        os_endpoint = data_source.get("opensearch_endpoint") or get_backend_config().default_opensearch_endpoint
        os_variant = data_source.get("opensearch_variant")

        needs_enrichment = data_source.get(DATA_SOURCE_PROPERTIES.ENRICH, True)
        enrichment_stats[f"data_source.enrich={needs_enrichment}"] += 1
        if not needs_enrichment:
            enrichment_stats["skip enrich from feature flag"] += 1
            logger.debug(f"Skipping enrichment for {cid=}: enrichment disabled via feature flag")
            continue

        if os_cid and os_endpoint and os_variant != "disabled":
            logger.debug(f"Enrich {cid=} ({data_source_type=}): {os_cid=} {os_endpoint=}")
            try:
                enrichment_stats["opensearch get_metadata"] += 1
                enrichment_metadata[cid] = opensearch_instance(endpoint=os_endpoint, variant=os_variant).get_metadata(
                    collection_id=os_cid
                )
            except Exception as e:
                enrichment_stats["opensearch fail"] += 1
                logger.warning(f"Failed to enrich collection metadata of {cid}: {e}", exc_info=True)

        elif (
            # Support multiple source types, as long as there is a STAC URL reference
            (data_source_type == "stac" and (stac_url := data_source.get("url")))
            or (data_source_type == "sentinel-hub" and (stac_url := data_source.get("stac_metadata_url")))
        ):
            logger.debug(f"Enrich {cid=} ({data_source_type=}): {stac_url=}")
            try:
                enrichment_stats["_enrichment_metadata_from_stac"] += 1
                enrichment_stats[f"_enrichment_metadata_from_stac for {data_source_type=}"] += 1
                enrichment_metadata[cid] = _enrichment_metadata_from_stac(
                    stac_url,
                    band_aliases=data_source.get("enrichment_band_aliases"),
                    links_filter=upstream_links_filter,
                )
            except Exception as e:
                enrichment_stats["stac fail"] += 1
                logger.warning(f"Failed to enrich collection metadata of {cid}: {e}", exc_info=True)

        else:
            logger.info(f"No enrichment implementation for {data_source_type=}")
            enrichment_stats["no enrichment implementation"] += 1
            enrichment_stats[f"no enrichment for {data_source_type=}"] += 1

    logger.info(f"Enrichment stats: {collection_iterator=!s} {dict(enrichment_stats)=}")

    if enrichment_metadata:
        # Collect links from both sources before merging, so we can combine them afterwards.
        merged_links: Dict[CollectionId, list] = {}
        for cid in enrichment_metadata:
            enrichment_links_raw = enrichment_metadata[cid].get("links")
            local_links_raw = (metadata.get(cid) or {}).get("links")
            enrichment_links: list = enrichment_links_raw if isinstance(enrichment_links_raw, list) else []
            local_links: list = local_links_raw if isinstance(local_links_raw, list) else []
            if enrichment_links or local_links:
                merged_links[cid] = enrichment_links + local_links

        metadata = dict_merge_recursive(enrichment_metadata, metadata, overwrite=True)

        for cid, links in merged_links.items():
            if cid in metadata:
                metadata[cid]["links"] = links

    return metadata


def _enrichment_metadata_from_stac(
    stac_url: str,
    *,
    band_aliases: Optional[Dict[str, List[str]]] = None,
    links_filter: Optional[LinksFilter] = None,
) -> dict:
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

    if links_filter:
        metadata["links"] = links_filter(metadata.get("links", []))

    return metadata
