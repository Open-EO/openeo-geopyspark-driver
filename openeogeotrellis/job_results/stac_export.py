"""
STAC collection/item files written for workspace export, pre-1.1 (assets
keyed by asset key) and STAC 1.1 (assets keyed by item + asset key).
"""
import json
from pathlib import Path
from typing import Callable, List
from urllib.parse import urlparse

from openeo.util import dict_no_none

from .raster_metadata import get_abs_path_of_asset
from .util import BadlyHashable, to_jsonable


def write_exported_stac_collection(
    job_dir: Path,
    result_metadata: dict,
    *,
    asset_keys: List[str],
    omit_derived_from_links: bool = False,
    job_id: str,
    usage_metadata: Callable[..., dict],
) -> List[Path]:  # TODO: change to Set?
    def write_stac_item_file(asset_id: str, asset: dict) -> Path:
        item_file = get_abs_path_of_asset(Path(f"{asset_id}.json"), job_dir)

        properties = {"datetime": asset.get("datetime")}

        if properties["datetime"] is None:
            start_datetime = (
                asset.get("start_datetime") or result_metadata.get("start_datetime") or "1970-01-01T00:00:00Z"
            )
            properties["datetime"] = start_datetime

        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": asset_id,
            "geometry": asset.get("geometry"),
            "bbox": asset.get("bbox"),
            "properties": properties,
            "links": [],  # TODO
            "assets": {
                asset_id: dict_no_none(
                    **{
                        "href": f"{Path(asset['href']).name}",  # relative to possibly nested item file
                        "roles": asset.get("roles"),
                        "type": asset.get("type"),
                        "eo:bands": asset.get("bands"),
                        "raster:bands": to_jsonable(asset.get("raster:bands")),
                    }
                )
            },
        }

        item_file.parent.mkdir(parents=True, exist_ok=True)
        with open(item_file, "wt") as fi:
            json.dump(stac_item, fi, allow_nan=False)

        return item_file

    item_files = [
        write_stac_item_file(asset_key, result_metadata.get("assets", {})[asset_key]) for asset_key in asset_keys
    ]

    def item_link(item_file: Path) -> dict:
        relative_path = item_file.relative_to(job_dir)
        return {
            "href": f"./{relative_path}",
            "rel": "item",
            "type": "application/geo+json",
        }

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": job_id,
        "description": f"This is the STAC metadata for the openEO job {job_id!r}",  # TODO
        "license": "unknown",  # TODO
        "extent": {
            "spatial": {"bbox": [result_metadata.get("bbox", [-180, -90, 180, 90])]},
            "temporal": {"interval": [[result_metadata.get("start_datetime"), result_metadata.get("end_datetime")]]},
        },
        "links": (
            [item_link(item_file) for item_file in item_files]
            + [
                link
                for link in usage_metadata("", omit_derived_from_links=omit_derived_from_links).get("links", [])
                if link["rel"] == "derived_from"
            ]
        ),
    }

    collection_file = job_dir / "collection.json"
    with open(collection_file, "wt") as fc:
        json.dump(stac_collection, fc)

    return item_files + [collection_file]


def write_exported_stac_collection_from_item(
    job_dir: Path,
    result_metadata: dict,
    *,
    item_metadata: dict,
    omit_derived_from_links: bool,
    attach_derived_from_document: bool,
    job_id: str,
    usage_metadata: Callable[..., dict],
    copy_auxiliary_links: Callable[..., List[dict]],
) -> List[Path]:  # TODO: change to Set?
    item_assets = dict()

    def intersect_band_array(list1, list2):
        band_result = []
        for item1 in list1:
            if isinstance(item1, dict) and "name" in item1:
                for item2 in list2:
                    if isinstance(item1, dict) and "name" in item1 and item1["name"] == item2["name"]:
                        band_result.append(intersect_dicts(item1, item2))
        return band_result

    def intersect_dicts(dict1, dict2):
        result = {}
        for key in dict1:
            if key in dict2:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    # Recursively intersect nested dictionaries
                    nested_result = intersect_dicts(dict1[key], dict2[key])
                    if nested_result:  # Only add if the nested result is not empty
                        result[key] = nested_result
                elif isinstance(dict1[key], list) and isinstance(dict2[key], list) and key == "bands":
                    result[key] = intersect_band_array(dict1[key], dict2[key])
                elif dict1[key] == dict2[key]:
                    # Retain the key-value pair if values are equal
                    result[key] = dict1[key]
        return result

    def write_stac_item_file(item_key: str, item: dict) -> Path:
        assets = dict()
        for asset_key, asset in item.get("assets").items():
            asset_bands = None
            if "bands" in asset:
                bands = asset["bands"]
                raster_bands = to_jsonable(asset.get("raster:bands", []))
                asset_bands = list()
                for band in bands:
                    name = band["name"]
                    asset_band = dict_no_none(band)
                    for raster_band in raster_bands:
                        if raster_band["name"] == name:
                            asset_band.update(raster_band)
                    asset_bands.append(asset_band)
            assets[asset_key] = dict_no_none(
                {
                    "href": f"{Path(urlparse(asset['href']).path).relative_to(job_dir)}",  # relative to top-level item file
                    "type": asset.get("type"),
                    "roles": asset.get("roles"),
                    "bands": asset_bands,
                }
            )
            item_asset = dict_no_none(
                {
                    "type": asset.get("type"),
                    "roles": asset.get("roles"),
                    "bands": asset_bands,
                }
            )
            if asset_key not in item_assets:
                item_assets[asset_key] = item_asset
            else:
                item_assets[asset_key] = intersect_dicts(item_assets[asset_key], item_asset)

        properties = item.get("properties", {"datetime": result_metadata.get("start_datetime")})
        properties["proj:bbox"] = result_metadata.get("bbox", item.get("bbox"))
        properties["proj:geometry"] = result_metadata.get("geometry", item.get("geometry"))
        result_item = result_metadata.get("items").get(item_key)
        if result_item:
            properties["proj:bbox"] = result_item.get("proj:bbox")
            properties["proj:shape"] = result_item.get("proj:shape")
        epsg_code = result_metadata.get("epsg", item.get("epsg"))
        if epsg_code:
            properties["proj:code"] = "EPSG:" + str(epsg_code)
        stac_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": item["id"],
            "geometry": item.get("geometry"),
            "bbox": item.get("bbox"),
            "properties": dict_no_none(properties),
            "links": (
                copy_auxiliary_links(
                    auxiliary_links=BadlyHashable(usage_metadata("").get("auxiliary_links", [])),
                    job_dir=job_dir,
                    for_export_workspace=True,
                )
                if attach_derived_from_document
                else []
            ),
            "assets": assets,
        }
        item_file = get_abs_path_of_asset(Path(f"{item['id']}.json"), job_dir)
        item_file.parent.mkdir(parents=True, exist_ok=True)
        with open(item_file, "wt") as fi:
            json.dump(stac_item, fi, allow_nan=False)

        return item_file

    item_files = [write_stac_item_file(item_key, item) for item_key, item in item_metadata.items()]

    derived_from_links = [
        link
        for link in usage_metadata(
            "", omit_derived_from_links=omit_derived_from_links or attach_derived_from_document
        ).get("links", [])
        if link["rel"] == "derived_from"
    ]

    def item_link(item_file: Path) -> dict:
        relative_path = item_file.relative_to(job_dir)
        return {
            "href": f"./{relative_path}",
            "rel": "item",
            "type": "application/geo+json",
        }

    stac_collection = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": job_id,
        "description": f"This is the STAC metadata for the openEO job {job_id!r}",  # TODO
        "license": "unknown",  # TODO
        "extent": {
            "spatial": {"bbox": [result_metadata.get("bbox", [-180, -90, 180, 90])]},
            "temporal": {"interval": [[result_metadata.get("start_datetime"), result_metadata.get("end_datetime")]]},
        },
        "links": [item_link(item_file) for item_file in item_files] + derived_from_links,
        "item_assets": item_assets,
    }

    collection_file = job_dir / "collection.json"  # TODO: file is reused for each result
    with open(collection_file, "wt") as fc:
        json.dump(stac_collection, fc)

    return item_files + [collection_file]
