"""
Library to simplify management of the layercatalog.json file,
reducing cumbersome and error-prone handling of boilerplate code
e.g. repeating band aspects in multiple places, auto42001 CRS constructs,
...

The main building blocks are:
- `BandMetadata`: describes a single band (name, wavelength, scale, unit, aliases, ...),
  consolidating fields from multiple STAC extensions (eo, raster) in one place
- `build_stac_collection_metadata`: builds a full openEO collection metadata dict
  from a list of `BandMetadata` objects and an upstream STAC URL
- `LayerCatalog`: reads and writes the layercatalog.json file and keeps its entries up to date

Note: this is just some initial attempt at easing the management of
the layercatalog.json file. There is still a lot of room for improvement.
Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/1175
"""

import argparse
import collections
import copy
import dataclasses
import difflib
import functools
import json
import logging
from pathlib import Path
from typing import List, Optional, Union, Tuple, Iterable, Callable, Any, Set, Dict, Iterator, FrozenSet

import requests

from openeo.util import dict_no_none, TimingLogger
from openeo.utils.version import ComparableVersion
from openeo_driver.util.compat import function_has_argument

from openeogeotrellis.catalog import DATA_SOURCE_PROPERTIES
from openeogeotrellis.catalog.enrich import enrich_catalog_metadata, LinksFilter, CollectionId
from openeogeotrellis.util.compat import function_supports_kwargs

_log = logging.getLogger(__name__)


class MetadataException(Exception):
    pass


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

GUESS_BANDS_FROM_UPSTREAM = "guess_bands_from_upstream"


class LayerCatalog:
    def __init__(self, collections: Iterable[dict] = ()):
        self._collections = list(collections)
        self._original_collection_ids = frozenset(c["id"] for c in self._collections)
        self._managed_collection_ids = set()

    @classmethod
    def load_json_file(cls, path: Union[str, Path]) -> "LayerCatalog":
        _log.info(f"Loading layer catalog from {path=}")
        with open(path, mode="r", encoding="utf-8") as f:
            collections = json.load(f)
        _log.info(f"Found {len(collections)=}")
        return cls(collections=collections)

    def write_json_file(
        self,
        path: Union[str, Path],
        indent: Union[int, None] = 2,
        separators: Union[Tuple[str, str], None] = None,
        sort_keys: bool = False,
    ) -> None:
        _log.info(f"Writing layer catalog to {path=} ({len(self._collections)=})")
        with open(path, mode="w", encoding="utf-8") as f:
            json.dump(
                self._collections, f, indent=indent, separators=separators, ensure_ascii=False, sort_keys=sort_keys
            )
            f.write("\n")


    def index_of(self, id: str) -> Union[int, None]:
        for i, collection in enumerate(self._collections):
            if collection.get("id") == id:
                return i
        return None

    def set_collection_metadata(self, metadata: dict):
        collection_id = metadata["id"]
        self._managed_collection_ids.add(collection_id)
        _log.info(f"Setting collection metadata for {collection_id=}")
        index = self.index_of(collection_id)
        if index is not None:
            # Overwrite metadata, but try to preserve original key order to mimimize diff noise.
            self._collections[index] = sort_dict_like_other(metadata, other=self._collections[index])
        else:
            self._collections.append(metadata)

    def get_unmanaged_collection_ids(self) -> Set[str]:
        return self._original_collection_ids.difference(self._managed_collection_ids)

    def get_stats(self) -> dict:
        enrich_flag_stats = collections.Counter(
            c.get("_vito", {}).get("data_source", {}).get(DATA_SOURCE_PROPERTIES.ENRICH) for c in self._collections
        )
        enrichment_mode_stats = collections.Counter(
            c.get("_vito", {}).get("management_info", {}).get("enrichment_mode") for c in self._collections
        )

        return {
            "enrich_flag_stats": enrich_flag_stats,
            "enrichment_mode_stats": enrichment_mode_stats,
        }



def sort_dict_like_other(d: dict, other: Union[dict, list]) -> dict:
    """
    Sort the items in a dictionary (by forcing the insertion order)
    based on an exiting dictionary or list of keys.
    Useful to minimize diff noise in JSON.
    """
    # TODO: move to more generic utility package
    # Weight map: start with order of other
    weights = {k: i for i, k in enumerate(other)}
    # Append remaining keys in original order
    weights.update({k: len(weights) + i for i, k in enumerate(d) if k not in weights})
    return dict(sorted(d.items(), key=lambda item: weights.get(item[0])))


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

    # From common STAC metadata (https://github.com/radiantearth/stac-spec/blob/master/commons/common-metadata.md#data-values)
    # or legacy raster extension
    nodata: Union[int, str, None] = None

    # openeo:gsd — structured GSD as {"value": [x, y], "unit": "m"} for eo:bands summaries
    openeo_gsd: Optional[dict] = None

    classification_classes: Optional[List[dict]] = None

    def to_common_bands(self) -> dict:
        """Common bands metadata (e.g. at collection top-level)"""
        return dict_no_none(
            name=self.name,
            description=self.description,
        )

    def to_summaries_raster_bands(self) -> dict:
        """summaries > raster:bands entry"""
        return dict_no_none(
            name=self.name,
            scale=self.raster_scale,
            offset=self.raster_offset,
            data_type=self.data_type,
            nodata=self.nodata,
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
            data_type=self.data_type,
            nodata=self.nodata,
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
                "data_type": self.data_type,
                "nodata": self.nodata,
                "unit": self.unit,
                "eo:common_name": self.eo_common_name,
                "eo:center_wavelength": self.eo_center_wavelength,
                "eo:full_width_half_max": self.eo_full_width_half_max,
                "gsd": self.gsd,
            }
        )


@functools.lru_cache
def get_upstream_stac_metadata(stac_url: str) -> dict:
    _log.info(f"Fetching upstream STAC metadata from {stac_url=}")
    response = requests.get(stac_url)
    response.raise_for_status()
    metadata = response.json()
    assert metadata["type"] == "Collection"
    assert metadata["stac_version"] in {"1.0.0", "1.1.0"}
    assert "stac_extensions" in metadata
    return metadata


# Legacy alias
_get_upstream_stac_metadata = get_upstream_stac_metadata


class ENRICHMENT_MODE:
    NONE = "none"
    LEGACY_AT_RUNTIME = "legacy_at_runtime"
    LEGACY_AT_BUILD_TIME = "legacy_at_build_time"


def build_stac_collection_metadata(
    id: str,
    *,
    stac_url: str,
    description: Optional[str] = None,
    description_prefix: Optional[str] = None,
    bands: Union[List[BandMetadata], str],
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
    contacts: Optional[List[dict]] = None,
    extra_summaries: Optional[dict] = None,
    enrichment_mode: str = ENRICHMENT_MODE.LEGACY_AT_RUNTIME,
    upstream_links_filter: Optional[LinksFilter] = None,
    debug_enrichment: bool = False,
    stac_version: str = "1.0.0",
    stac_extensions: Union[None, List[str], Callable[[List[str]], List[str]]] = None,
) -> dict:
    """
    Generic openEO collection metadata generator.

    :param upstream_links_filter: optional filter function for upstream links,
        before merging with local links. Only used in `LEGACY_AT_BUILD_TIME` mode.
        This is a pretty awkward API, caused by ad-hoc link handling in legacy enrichment approach.
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

    stac_version = upstream_metadata.get("stac_version", stac_version)

    if callable(stac_extensions):
        # Allow manipulation (or direct pass-through) of upstream extensions
        upstream_stac_extensions = upstream_metadata.get("stac_extensions", [])
        stac_extensions = stac_extensions(upstream_stac_extensions)

    if not description:
        description = (description_prefix or "") + upstream_metadata.get("description", "")

    if bands == GUESS_BANDS_FROM_UPSTREAM:
        bands = extract_band_metadata_list(upstream_metadata)
    elif isinstance(bands, list):
        pass
    else:
        raise ValueError(bands)

    if ComparableVersion("1.1.0").or_higher(stac_version):
        # Per https://github.com/radiantearth/stac-spec/issues/1346
        toplevel_bands = [b.to_common_bands() for b in bands]
    else:
        toplevel_bands = None

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
    vito = {
        "data_source": data_source,
        "management_info": {
            "enrichment_mode": enrichment_mode,
        },
    }
    if properties:
        vito["properties"] = properties

    metadata = dict_no_none(
        {
            "stac_version": stac_version,
            "stac_extensions": stac_extensions,
            "type": "Collection",
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
            "bands": toplevel_bands,
            "contacts": contacts,
        }
    )

    if enrichment_mode == ENRICHMENT_MODE.LEGACY_AT_RUNTIME:
        metadata["_vito"]["data_source"][DATA_SOURCE_PROPERTIES.ENRICH] = True
    elif enrichment_mode == ENRICHMENT_MODE.LEGACY_AT_BUILD_TIME:
        orig_metadata = copy.deepcopy(metadata)
        metadata = _legacy_enrich_collection_metadata(metadata, upstream_links_filter=upstream_links_filter)
        if debug_enrichment:
            diff_lines = dict_compare(orig_metadata, metadata, name1="original", name2="enriched")
            _log.debug(f"Line-by-line diff of metadata enrichment ({len(diff_lines)=}):\n" + "\n".join(diff_lines))
        metadata["_vito"]["data_source"][DATA_SOURCE_PROPERTIES.ENRICH] = False
    elif enrichment_mode == ENRICHMENT_MODE.NONE:
        metadata["_vito"]["data_source"][DATA_SOURCE_PROPERTIES.ENRICH] = False
    else:
        raise ValueError(f"Unknown {enrichment_mode=}")

    return metadata


# Deprecated legacy alias
build_terrascope_stac_collection_metadata = build_stac_collection_metadata


def _legacy_enrich_collection_metadata(
    collection_metadata: dict, *, upstream_links_filter: Optional[LinksFilter] = None
) -> dict:
    """Legacy metadata enrichment"""
    # Wrap (and unwrap) collection metadata in catalog structure expected by legacy enrichment logic
    cid = collection_metadata["id"]
    catalog = {cid: copy.deepcopy(collection_metadata)}
    enriched_catalog = enrich_catalog_metadata(catalog, upstream_links_filter=upstream_links_filter)
    enriched_collection_metadata = enriched_catalog[cid]

    # Remove some fields from the metadata
    # TODO: really necessary to exclude these?
    # TODO: Better work with allow-list than cat-and-mouse ignore-list?
    # TODO: larger scope? Configurable?
    for key in {"assets", "item_assets", "auth:schemes", "storage:schemes"}:
        if key in enriched_collection_metadata:
            del enriched_collection_metadata[key]

    return enriched_collection_metadata


class _BandMetadataCollector:
    def __init__(self):
        self._collected_band_metadata: List[dict] = []

    def register(self, name: str, **kwargs):
        """Register band metadata by name (add to existing, or create new entry)"""
        metadata = self.get_by_name(name=name, auto_create=True)
        # TODO: check for overwrites?
        metadata.update(kwargs)

    def get_band_names(self) -> List[str]:
        return [b["name"] for b in self._collected_band_metadata]

    def get_by_name(self, name: str, auto_create: bool = True) -> dict:
        matches = [b for b in self._collected_band_metadata if b["name"] == name]
        if matches:
            assert len(matches) == 1
            return matches[0]
        elif auto_create:
            data = {"name": name}
            self._collected_band_metadata.append(data)
            return data
        else:
            raise LookupError(f"Band {name} not found")

    def collect_from_stac_collection_metadata(self, metadata: dict):
        """
        Extract/guess band metadata from raw STAC collection metadata,
        Based on and trying consolidation of information from:
        - toplevel "bands"
        - "summaries" > "bands"
        - "summaries" > "eo:bands"
        - "item_assets" > ... > "bands"
        - "cube:dimensions" > "bands" dimension
        """

        if "bands" in metadata:
            for band in metadata["bands"]:
                self.register(name=band["name"], description=band.get("description"))

        if "summaries" in metadata:
            if "bands" in metadata["summaries"]:
                for band in metadata["summaries"]["bands"]:
                    self.register(
                        name=band["name"],
                        eo_common_name=band.get("eo:common_name"),
                        description=band.get("description"),
                        gsd=band.get("gsd"),
                        raster_offset=band.get("raster:offset"),
                        raster_scale=band.get("raster:scale"),
                        data_type=band.get("data_type"),
                        nodata=band.get("nodata"),
                        unit=band.get("unit"),
                    )
            elif "eo:bands" in metadata["summaries"]:
                for band in metadata["summaries"]["eo:bands"]:
                    self.register(
                        name=band["name"],
                        eo_common_name=band.get("common_name"),
                        description=band.get("description"),
                        gsd=band.get("gsd"),
                        data_type=band.get("data_type"),
                        nodata=band.get("nodata"),
                        unit=band.get("unit"),
                    )

        if "item_assets" in metadata:
            for asset_key, asset in metadata["item_assets"].items():
                if "bands" in asset:
                    for band in asset["bands"]:
                        self.register(
                            name=band["name"],
                            description=band.get("description"),
                            raster_scale=band.get("raster:scale") or asset.get("raster:scale"),
                            raster_offset=band.get("raster:offset") or asset.get("raster:offset"),
                            data_type=band.get("data_ype") or asset.get("data_type"),
                            nodata=band.get("nodata") or asset.get("nodata"),
                            classification_classes=band.get("classification:classes"),
                        )

        if "cube:dimensions" in metadata:
            for _, dim in metadata["cube:dimensions"].items():
                if dim.get("type") == "bands":
                    for b in dim.get("values"):
                        self.register(name=b)

        # Set GSD from summaries (if not set already)
        if "summaries" in metadata and "gsd" in metadata["summaries"] and len(metadata["summaries"]["gsd"]) == 1:
            gsd = metadata["summaries"]["gsd"][0]
            for name in self.get_band_names():
                if "gsd" not in self.get_by_name(name=name, auto_create=False):
                    self.register(name=name, gsd=gsd)

        return self

    def get_band_metadata_list(self) -> List[BandMetadata]:
        return [BandMetadata(**b) for b in self._collected_band_metadata]


def extract_band_metadata_list(metadata: dict) -> List[BandMetadata]:
    """Extract/guess band metadata from raw STAC collection metadata"""
    collector = _BandMetadataCollector()
    return collector.collect_from_stac_collection_metadata(metadata).get_band_metadata_list()


def apply_raster_scale_and_offset_to_band_metadata(bands: List[BandMetadata]) -> List[BandMetadata]:
    """
    Convert list of band metadata to reflect the automatic application
    of `raster:scale` and `raster:offset` to the raster data at data load time,
    (remove original `raster:scale`, `raster:offset`, update data_type, nodata, ...)
    """
    # TODO: how to make sure this is aligned with the actual implementation in openeo-geotrellis-extension?
    has_scaling = any(b.raster_scale not in {1, None} for b in bands)
    has_fractional_offset = any(isinstance(b.raster_offset, float) and not b.raster_offset.is_integer() for b in bands)
    to_float = has_scaling or has_fractional_offset

    def convert(band: BandMetadata) -> BandMetadata:
        data = dataclasses.asdict(band)
        # Remove `raster:scale` and `raster:offset` fields
        data["raster_scale"] = None
        data["raster_offset"] = None
        if to_float:
            data["data_type"] = "float32"
            # TODO: possible to set `nodata`? e.g. "nan"?
            data["nodata"] = None

        if (
            band.raster_scale not in {1, None} or band.raster_offset not in {0.0, None}
        ) and band.classification_classes:
            # TODO: how to combine auto-scaling and classification classes?
            _log.warning(
                f"Band {band.name!r} with both scaling (scale {band.raster_scale}, offset {band.raster_offset}) and classification classes {band.classification_classes}."
            )
            data["classification_classes"] = None

        return BandMetadata(**data)

    return [convert(b) for b in bands]


def dict_compare(d1: dict, d2: dict, name1: str = "left", name2: str = "right") -> List[str]:
    """
    Compare two dictionaries by serializing as JSON and doing a line-by-line diff
    """
    # TODO: move to more generic utility package
    s1 = json.dumps(d1, indent=2, sort_keys=True).splitlines()
    s2 = json.dumps(d2, indent=2, sort_keys=True).splitlines()

    diff = difflib.unified_diff(s1, s2, fromfile=name1, tofile=name2, lineterm="")
    return list(diff)


@dataclasses.dataclass(frozen=True)
class _BuildItem:
    collection_id: str
    build: Callable
    kwargs: dict
    labels: FrozenSet[str]


class CollectionMetadataBuilderRegistry:
    """
    Registry for collection metadata builder functions.
    """

    # TODO: support labels (e.g. to flag collections for "prod", "dev", ...)

    def __init__(self):
        self._builders: List[_BuildItem] = []
        self._stats = collections.defaultdict(int)

    def register(
        self,
        collection_id: CollectionId,
        builder: Callable,
        *,
        label: Optional[str] = None,
        labels: Iterable[str] = (),
        kwargs: Optional[dict] = None,
    ):
        """
        Register a metadata builder for given collection id,
        with optional kwargs to call it with
        """
        kwargs = kwargs or {}
        labels = frozenset([label] if label else []) | frozenset(labels)
        _log.debug(f"Registering {collection_id=} {builder=} with {kwargs=} and {labels=}")
        if forbidden_kwargs := {"collection_id", "labels"}.intersection(kwargs):
            raise MetadataException(f"Forbidden {forbidden_kwargs} in {kwargs=}")
        self._builders.append(_BuildItem(collection_id=collection_id, build=builder, kwargs=kwargs, labels=labels))
        self._stats["register"] += 1

    def decorator(
        self,
        collection_id: CollectionId,
        *,
        label: Optional[str] = None,
        labels: Iterable[str] = (),
        **kwargs,
    ):
        """
        Decorator to compactly register a builder function
        optionally with kwargs to use at build time
        """

        def wrapper(build: Callable):
            self.register(collection_id=collection_id, builder=build, label=label, labels=labels, kwargs=kwargs)
            self._stats["decorated"] += 1
            return build

        return wrapper

    def get_builders(
        self,
        *,
        collection_id_filter: Optional[Callable[[CollectionId], bool]] = None,
        label_filter: Optional[Callable[[FrozenSet[str]], bool]] = None,
        check_duplicates: bool = True,
    ) -> List[_BuildItem]:
        """
        Get builders with optional filtering based on collection id or labels
        """
        builders: List[_BuildItem] = self._builders
        if collection_id_filter:
            builders = [b for b in builders if collection_id_filter(b.collection_id)]
        if label_filter:
            builders = [b for b in builders if label_filter(b.labels)]
        _log.info(f"Selected subset of {len(builders)} from {len(self._builders)} builders")

        if check_duplicates:
            cid_histogram = collections.Counter(b.collection_id for b in builders)
            duplicated = sorted(f"{cid} ({count})" for cid, count in cid_histogram.items() if count > 1)
            if duplicated:
                raise MetadataException("Selection with duplicate collections: " + ", ".join(duplicated))
        return builders

    def call_builders(
        self,
        *,
        collection_id_filter: Optional[Callable[[CollectionId], bool]] = None,
        label_filter: Optional[Callable[[FrozenSet[str]], bool]] = None,
    ) -> Iterator[dict]:
        """
        Iterate through all or a subset of the builders and call them with their associated args/kwargs
        to produce metadata

        :param collection_id_filter: optional filter to select a subset based on collection id
        :return:
        """
        builders = self.get_builders(collection_id_filter=collection_id_filter, label_filter=label_filter)
        for i, builder in enumerate(builders):
            collection_id = builder.collection_id
            _log.info(f"[{i+1}/{len(builders)}] Building metadata for {collection_id=}")

            # Pass extra args if build accepts them
            kwargs = builder.kwargs
            for arg, value in {
                "collection_id": collection_id,
                "labels": builder.labels,
            }.items():
                if function_has_argument(builder.build, arg) or function_supports_kwargs(builder.build):
                    assert arg not in builder.kwargs, f"{arg=} should not be in {kwargs=}"
                    kwargs = dict(kwargs, **{arg: value})

            # Call the builder now
            metadata = builder.build(**kwargs)
            if collection_id != metadata.get("id"):
                raise MetadataException(f"Generated {metadata.get('id')=} does not match {collection_id=}")
            yield metadata
            self._stats["build call"] += 1

    def get_stats(self) -> dict:
        return self._stats


class LayerCatalogManagerCliApp:
    """
    Generic command line app for layer catalog management
    """

    def __init__(self, *, registry: CollectionMetadataBuilderRegistry):
        self.collection_metadata_builder_registry = registry

    def main(self, argv: Optional[Iterable[str]] = None) -> None:
        argument_parser = self.build_argument_parser()
        args = argument_parser.parse_args(argv)
        self.setup_logging(args=args)
        with TimingLogger(title=f"LayerCatalogManagerCliApp.main: {args.subcommand}", logger=_log):
            args.handler(args=args)

    def build_argument_parser(self) -> argparse.ArgumentParser:
        cli = argparse.ArgumentParser()
        # Generic cli args
        cli.add_argument("-v", "--verbose", action="store_true")
        # Subcommands
        subparsers = cli.add_subparsers(dest="subcommand", required=True)

        # Subcommand: generate
        parser_generate = subparsers.add_parser("generate", help="Generate layer catalog")
        parser_generate.set_defaults(handler=self.handle_generate)
        parser_generate.add_argument(
            "catalog_path",
            help="Layer catalog JSON file to load (if available) and to overwrite (unless a different output path is specified)",
        )
        parser_generate.add_argument(
            "--output",
            dest="output_catalog_path",
            default=None,
            help="Output layer catalog JSON file (to specify when to write to a different file than input)",
        )
        parser_generate.add_argument(
            "-k",
            dest="cid_substring",
            help="Only update collections with id matching this given substring.",
        )

        # Subcommand: list
        parser_list = subparsers.add_parser("list", help="List managed openEO collection ids")
        parser_list.set_defaults(handler=self.handle_list)
        parser_list.add_argument(
            "-l", dest="show_extended", action="store_true", help="Show additional build info per collection"
        )

        # TODO other possible subcommands:
        #       health check (e.g. check for unmanaged collections, check for run-time enrichment, ...)

        return cli

    def setup_logging(self, args: argparse.Namespace) -> None:
        # TODO: option to not do rich logging (or detect when inappropriate)?
        try:
            # Use fancy logging if `rich` is available in environment.
            import rich.logging
            import rich.console

            log_handlers = [rich.logging.RichHandler(console=rich.console.Console(stderr=True))]
        except ImportError:
            log_handlers = []
        logging.basicConfig(
            level=logging.DEBUG if args.verbose else logging.INFO,
            handlers=log_handlers,
        )

    def handle_generate(self, args: argparse.Namespace) -> Path:
        catalog_path = Path(args.catalog_path)
        output_path = Path(args.output_catalog_path or args.catalog_path)
        cid_substring = args.cid_substring

        if cid_substring:
            collection_id_filter = lambda cid: cid_substring in cid
        else:
            collection_id_filter = None

        # Note: loading from the JSON file is strictly not necessary
        # as everything will be overwritten with freshly generated metadata.
        # However, it helps with preserving the original collection order
        # and consequently minimizing the git diff to review.
        if catalog_path.exists():
            layer_catalog = LayerCatalog.load_json_file(catalog_path)
        else:
            layer_catalog = LayerCatalog()

        built_metadatas = self.collection_metadata_builder_registry.call_builders(
            collection_id_filter=collection_id_filter
        )
        for metadata in built_metadatas:
            layer_catalog.set_collection_metadata(metadata=metadata)

        layer_catalog.write_json_file(output_path)

        # Some stats
        _log.info(f"{self.collection_metadata_builder_registry.get_stats()=}")
        _log.info(f"{layer_catalog.get_stats()=}")
        unmanaged_collection_ids = layer_catalog.get_unmanaged_collection_ids()
        _log.log(
            level=logging.WARNING if unmanaged_collection_ids else logging.INFO,
            msg=f"{len(unmanaged_collection_ids)} Unmanaged collections: {sorted(unmanaged_collection_ids)}",
        )

        return output_path

    def handle_list(self, args: argparse.Namespace) -> None:
        # TODO: add collection counter
        # TODO: (option to) render in Markdown format
        for item in sorted(
            self.collection_metadata_builder_registry.get_builders(),
            key=lambda item: item.collection_id,
        ):
            if args.show_extended:
                print(item.collection_id, f"labels={sorted(item.labels)}", f"kwargs={item.kwargs}")
            else:
                print(item.collection_id)
