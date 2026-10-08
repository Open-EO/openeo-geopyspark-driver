import logging
from copy import deepcopy

from openeo.util import deep_get

from .enrich import CatalogDict

logger = logging.getLogger(__name__)


def merge_layers_with_common_name(metadata: CatalogDict) -> CatalogDict:
    """Merge collections with same common name. Updates metadata dict in place."""
    common_names = set(str(m["common_name"]) for m in metadata.values() if "common_name" in m)
    logger.debug(f"Creating merged collections for common names: {common_names}")
    for common_name in common_names:
        merged = {
            "id": common_name,
            "_vito": {"data_source": {
                "type": "merged_by_common_name",
                "common_name": common_name,
                "merged_collections": [],
            }},
            "providers": [],
            "links": [],
            "extent": {"spatial": {"bbox": []}, "temporal": {"interval": []}},
        }

        merge_sources = [m for m in metadata.values() if m.get("common_name") == common_name]
        # Give priority to (reference/override) values in the "virtual:merge-by-common-name" placeholder entry
        merge_sources = sorted(
            merge_sources,
            key=(lambda m: deep_get(m, "_vito", "data_source", "type", default=None) == "virtual:merge-by-common-name"),
            reverse=True,
        )
        eo_bands = {}
        logger.info(f"Merging {common_name} from {[m['id'] for m in merge_sources]}")
        for to_merge in merge_sources:
            if not deep_get(to_merge, "_vito", "data_source", "type", default="").startswith("virtual:"):
                merged["_vito"]["data_source"]["merged_collections"].append(to_merge["id"])
            # Fill some fields with first hit
            for field in ["title", "description", "keywords", "version", "license", "cube:dimensions", "summaries"]:
                if field not in merged and field in to_merge:
                    merged[field] = deepcopy(to_merge[field])
            # Fields to take union
            for field in ["providers", "links"]:
                if isinstance(to_merge.get(field), list):
                    merged[field] += deepcopy(to_merge[field])

            # Take union of bands
            for band_dim in [k for k, v in to_merge.get("cube:dimensions", {}).items() if v["type"] == "bands"]:
                if band_dim not in merged["cube:dimensions"]:
                    merged["cube:dimensions"][band_dim] = deepcopy(to_merge["cube:dimensions"][band_dim])
                else:
                    for b in to_merge["cube:dimensions"][band_dim]["values"]:
                        if b not in merged["cube:dimensions"][band_dim]["values"]:
                            merged["cube:dimensions"][band_dim]["values"].append(b)
            for b in deep_get(to_merge, "summaries", "eo:bands", default=[]):
                band_name = b["name"]
                if band_name not in eo_bands:
                    eo_bands[band_name] = b
                else:
                    # Merge some things
                    aliases = set(eo_bands[band_name].get("aliases", [])) | set(b.get("aliases", []))
                    if aliases:
                        eo_bands[band_name]["aliases"] = list(aliases)

            # Union of extents
            # TODO: make sure first bbox/interval is overall extent
            merged["extent"]["spatial"]["bbox"].extend(deep_get(to_merge, "extent", "spatial", "bbox", default=[]))
            merged["extent"]["temporal"]["interval"].extend(
                deep_get(to_merge, "extent", "temporal", "interval", default=[])
            )

        # Adapt band order under `eo:bands`, based on `cube:dimensions`
        band_dims = [k for k, v in merged.get("cube:dimensions", {}).items() if v["type"] == "bands"]
        if band_dims:
            (band_dim,) = band_dims
            merged["summaries"]["eo:bands"] = [eo_bands[b] for b in merged["cube:dimensions"][band_dim]["values"]]
            # TODO #1109 also merge/handle "common" bands under summaries (instead of legacy eo:bands)

        metadata[common_name] = merged

    return metadata
