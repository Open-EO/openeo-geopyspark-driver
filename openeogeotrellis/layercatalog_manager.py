"""
Library to simplify management of the layercatalog.json file,
reducing cumbersome and error-prone handling of boilerplate code
e.g. repeating band aspects in multiple places, auto42001 CRS constructs,
...

The main building blocks are:
- `BandMetadata`: describes a single band (name, wavelength, scale, unit, aliases, ...),
  consolidating fields from multiple STAC extensions (eo, raster) in one place
- `build_terrascope_stac_collection_metadata`: builds a full openEO collection metadata dict
  from a list of `BandMetadata` objects and an upstream Terrascope STAC URL
- `LayerCatalog`: reads and writes the layercatalog.json file and keeps its entries up to date

Note: this is just some initial attempt at easing the management of
the layercatalog.json file. There is still a lot of room for improvement.
Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/1175
"""

import dataclasses
import functools
import json
import logging
from pathlib import Path
from typing import List, Optional, Union

import requests

from openeogeotrellis.catalog_enrich import enrich_catalog_metadata

_log = logging.getLogger(__name__)

CRS_AUTO_42001 = {
    "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
    "type": "GeodeticCRS",
    "name": "AUTO 42001 (Universal Transverse Mercator)",
    "datum": {
        "type": "GeodeticReferenceFrame",
        "name": "World Geodetic System 1984",
        "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
            {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"},
        ],
    },
    "area": "World",
    "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180},
    "id": {"authority": "OGC", "version": "1.3", "code": "Auto42001"},
}


class LayerCatalog:
    def __init__(self, collections: List[dict]):
        self._collections = collections.copy()

    @classmethod
    def load_json_file(cls, path: Union[str, Path]) -> "LayerCatalog":
        _log.info(f"Loading layer catalog from {path=}")
        with open(path, mode="r", encoding="utf-8") as f:
            collections = json.load(f)
        _log.info(f"Found {len(collections)=}")
        return cls(collections=collections)

    def write_json_file(self, path: Union[str, Path]) -> None:
        _log.info(f"Writing layer catalog to {path=} ({len(self._collections)=})")
        with open(path, mode="w", encoding="utf-8") as f:
            json.dump(self._collections, f, indent=2, ensure_ascii=False)

    def enrich(self) -> None:
        """
        Enrich collection metadata in-place from external sources (OpenSearch, STAC, Sentinel Hub).

        Applies the same enrichment as the runtime :func:`~openeogeotrellis.catalog_enrich.enrich_catalog_metadata`,
        but during the manual layercatalog management step so that the result can be persisted to the JSON file.
        """
        metadata_dict = {c["id"]: c for c in self._collections}
        enriched = enrich_catalog_metadata(metadata_dict)
        # Preserve original ordering, then append any newly added collections
        seen = set()
        result = []
        for c in self._collections:
            cid = c["id"]
            result.append(enriched.get(cid, c))
            seen.add(cid)
        for cid, c in enriched.items():
            if cid not in seen:
                result.append(c)
        self._collections = result

    def index_of(self, id: str) -> Union[int, None]:
        for i, collection in enumerate(self._collections):
            if collection.get("id") == id:
                return i
        return None

    def set_collection_metadata(self, metadata: dict):
        collection_id = metadata["id"]
        _log.info(f"Setting collection metadata for {collection_id=}")
        index = self.index_of(collection_id)
        if index is not None:
            self._collections[index] = metadata
        else:
            self._collections.append(metadata)


def dict_no_none(*args, **kwargs) -> dict:
    """
    Helper to build a dict containing given key-value pairs where the value is not None.
    """
    return {k: v for k, v in dict(*args, **kwargs).items() if v is not None}


@dataclasses.dataclass(frozen=True, kw_only=True)
class BandMetadata:
    """
    Container for various kinds of band metadata,
    spanning various STAC extensions: eo, raster
    and ad-hoc/custom properties.
    """

    name: str
    description: Optional[str] = None

    # eo:common_name
    eo_common_name: Optional[str] = None

    # eo:center_wavelength:  The center wavelength of the band, in micrometers (μm).
    eo_center_wavelength: Optional[float] = None

    # raster:scale
    raster_scale: Optional[float] = None

    # raster:offset
    raster_offset: Optional[float] = None

    aliases: Optional[List[str]] = None

    # originally raster:bands>unit, but moved to STAC core
    unit: Optional[str] = None

    # TODO how does gsd at band level work in STAC?
    gsd: Optional[float] = None

    # eo:full_width_half_max: bandwidth in micrometers (μm)
    eo_full_width_half_max: Optional[float] = None

    # data type, e.g. "int16"
    data_type: Optional[str] = None

    # openeo:gsd — structured GSD as {"value": [x, y], "unit": "m"} for eo:bands summaries
    openeo_gsd: Optional[dict] = None

    classification_classes: Optional[List[dict]] = None

    def to_summaries_raster_bands(self) -> dict:
        """summaries > raster:bands entry"""
        return dict_no_none(
            name=self.name,
            scale=self.raster_scale,
            offset=self.raster_offset,
            unit=self.unit,
            **{"classification:classes": self.classification_classes} if self.classification_classes else {},
        )

    def to_summaries_eo_bands(self) -> dict:
        """summaries > eo:bands entry"""
        return dict_no_none(
            name=self.name,
            description=self.description,
            **{"common_name": self.eo_common_name} if self.eo_common_name is not None else {},
            center_wavelength=self.eo_center_wavelength,
            full_width_half_max=self.eo_full_width_half_max,
            aliases=self.aliases,
            gsd=self.gsd,
            offset=self.raster_offset,
            scale=self.raster_scale,
            type=self.data_type,
            unit=self.unit,
            **{"openeo:gsd": self.openeo_gsd} if self.openeo_gsd is not None else {},
        )

    def to_summaries_bands(self) -> dict:
        """summaries > bands entry"""
        return dict_no_none(
            {
                "name": self.name,
                "description": self.description,
                "aliases": self.aliases,
                "raster:scale": self.raster_scale,
                "raster:offset": self.raster_offset,
                "unit": self.unit,
                "eo:common_name": self.eo_common_name,
                "eo:center_wavelength": self.eo_center_wavelength,
                "eo:full_width_half_max": self.eo_full_width_half_max,
                "gsd": self.gsd,
            }
        )


@functools.lru_cache
def _get_upstream_stac_metadata(stac_url: str) -> dict:
    _log.info(f"Fetching upstream STAC metadata from {stac_url=}")
    response = requests.get(stac_url)
    response.raise_for_status()
    metadata = response.json()
    assert metadata["type"] == "Collection"
    assert metadata["stac_version"] in {"1.0.0", "1.1.0"}
    assert "stac_extensions" in metadata
    return metadata


def build_terrascope_stac_collection_metadata(
    id: str,
    *,
    description: Optional[str] = None,
    description_prefix: Optional[str] = None,
    stac_url: str,
    bands: List[BandMetadata],
    load_stac_feature_flags: Optional[dict] = None,
    x_dim: Optional[dict] = None,
    y_dim: Optional[dict] = None,
    name: Optional[str] = None,
    common_name: Optional[str] = None,
    provider_backend: Optional[str] = None,
    deprecated: Optional[bool] = None,
    experimental: Optional[bool] = None,
    keywords: Optional[List[str]] = None,
    is_utm: Optional[bool] = None,
    realign: Optional[bool] = None,
    consider_as_singular_time_step: Optional[bool] = None,
    title: Optional[str] = None,
    properties: Optional[dict] = None,
    license: Optional[str] = None,
    include_eo_bands: bool = True,
    links: Optional[List[dict]] = None,
    sci_doi: Optional[str] = None,
    sci_citation: Optional[str] = None,
    mission: Optional[str] = None,
    extra_summaries: Optional[dict] = None,
) -> dict:
    """
    Generic openEO collection metadata generator.
    """
    data_source = {
        "type": "stac",
        "url": stac_url,
    }
    if provider_backend:
        data_source["provider:backend"] = provider_backend
    if is_utm:
        data_source["is_utm"] = is_utm
    if realign is not None:
        data_source["realign"] = realign
    if consider_as_singular_time_step is not None:
        data_source["consider_as_singular_time_step"] = consider_as_singular_time_step
    if load_stac_feature_flags:
        data_source["load_stac_feature_flags"] = load_stac_feature_flags

    if not x_dim:
        x_dim = {"type": "spatial", "axis": "x", "reference_system": CRS_AUTO_42001, "step": 10}

    if not y_dim:
        y_dim = {"type": "spatial", "axis": "y", "reference_system": CRS_AUTO_42001, "step": 10}

    upstream_metadata = _get_upstream_stac_metadata(stac_url)

    if not description:
        description = (description_prefix or "") + upstream_metadata.get("description", "")

    summaries = upstream_metadata.get("summaries", {})
    summaries["raster:bands"] = [b.to_summaries_raster_bands() for b in bands]
    if include_eo_bands and bands:
        summaries["eo:bands"] = [b.to_summaries_eo_bands() for b in bands]
    elif "eo:bands" in summaries:
        del summaries["eo:bands"]
    summaries["bands"] = [b.to_summaries_bands() for b in bands]
    if extra_summaries:
        summaries.update(extra_summaries)

    cube_dimensions_bands_values = [b.name for b in bands]
    vito = {"data_source": data_source}
    if properties:
        vito["properties"] = properties

    return dict_no_none(
        {
            "id": id,
            "name": name,
            "common_name": common_name,
            "mission": mission,
            "title": title or upstream_metadata.get("title", id),
            "description": description,
            "experimental": experimental,
            "keywords": keywords or upstream_metadata.get("keywords") or None,
            "license": license or upstream_metadata.get("license", "other"),
            "deprecated": deprecated,
            "providers": upstream_metadata.get("providers", []),
            "extent": upstream_metadata.get("extent", None),
            "links": links,
            "sci:doi": sci_doi or upstream_metadata.get("sci:doi") or None,
            "sci:citation": sci_citation or upstream_metadata.get("sci:citation") or None,
            "_vito": vito,
            "summaries": summaries,
            "cube:dimensions": {
                "x": x_dim,
                "y": y_dim,
                "t": {"type": "temporal"},
                "bands": {"type": "bands", "values": cube_dimensions_bands_values},
            },
        }
    )
