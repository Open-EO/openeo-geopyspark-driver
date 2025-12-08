import json
import os
import shutil
from pathlib import Path
from typing import Union, Dict
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from flask import Response
from openeo_driver.datastructs import StacAsset
from openeo_driver.save_result import SaveResult


def get_files_from_stac_catalog(catalog_path: Union[str, Path]) -> list:
    """
    Goes through the stac catalog recursively to find all files.
    """
    if isinstance(catalog_path, str) and catalog_path.startswith("http"):
        response = requests.get(catalog_path)
        response.raise_for_status()
        catalog_json = response.json()
    else:
        catalog_path = str(catalog_path)
        assert os.path.exists(catalog_path)
        catalog_json = json.loads(Path(catalog_path).read_text())

    all_files = []
    links = []
    if "links" in catalog_json:
        links.extend(catalog_json["links"])
    if "assets" in catalog_json:
        links.extend(list(catalog_json["assets"].values()))
    for link in links:
        if "href" in link:
            href = link["href"]
            if href.startswith("file://"):
                href = href[7:]
            href = urljoin(catalog_path, href)
            all_files.append(href)

            if "rel" in link and (link["rel"] == "child" or link["rel"] == "item"):
                all_files.extend(get_files_from_stac_catalog(href))
    return all_files


def get_assets_from_stac_catalog(catalog_path: Union[str, Path]) -> Dict[str, StacAsset]:
    if isinstance(catalog_path, str) and catalog_path.startswith("http"):
        response = requests.get(catalog_path)
        response.raise_for_status()
        catalog_json = response.json()
    else:
        catalog_path = str(catalog_path)
        assert os.path.exists(catalog_path)
        catalog_json = json.loads(Path(catalog_path).read_text())

    all_assets = {}
    links = []
    if "links" in catalog_json:
        links.extend(catalog_json["links"])
    if "assets" in catalog_json:
        links.extend(list(catalog_json["assets"].values()))
        assets = catalog_json["assets"]
        all_assets.update(assets)
    for link in links:
        if "href" in link:
            href = link["href"]
            if href.startswith("file://"):
                href = href[7:]
            href = urljoin(catalog_path, href)

            if "rel" in link and (link["rel"] == "child" or link["rel"] == "item"):
                all_assets.update(get_assets_from_stac_catalog(href))
    return all_assets


def get_items_from_stac_catalog(catalog_path: Union[str, Path]) -> dict:
    if isinstance(catalog_path, str) and catalog_path.startswith("http"):
        response = requests.get(catalog_path)
        response.raise_for_status()
        catalog_json = response.json()
    else:
        catalog_path = str(catalog_path)
        assert os.path.exists(catalog_path), f"catalog_path does not exist: {catalog_path}"
        catalog_json = json.loads(Path(catalog_path).read_text())

    all_items = {}
    links = []
    if "links" in catalog_json:
        links.extend(catalog_json["links"])
    if "assets" in catalog_json:
        links.extend(list(catalog_json["assets"].values()))
        all_items.update({catalog_json["id"]: catalog_json})
    for link in links:
        if "href" in link:
            href = link["href"]
            if href.startswith("file://"):
                href = href[7:]
            href = urljoin(catalog_path, href)

            if "rel" in link and (link["rel"] == "child" or link["rel"] == "item"):
                all_items.update(get_items_from_stac_catalog(href))
    return all_items


class StacSaveResult(SaveResult):
    """
    SaveResult implementation that will copy over a STAC catalog while keeping asset metadata.
    Metadata in collection.json might be lost.
    """

    def __init__(self, stac_root: str):
        super().__init__(format=None, options=None)
        self.stac_root = stac_root
        self.stac_root_local = None

    def save_result(self, filename: str) -> str:
        raise NotImplementedError("save_result not implemented for cube type: {t}".format(t=type(self.cube)))

    def write_assets(self, directory: Union[str, Path]) -> Dict[str, StacAsset]:
        """
        Copy over the stac catalog to the expected directory
        :return: STAC assets dictionary: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#assets
        """
        stac_assets = get_files_from_stac_catalog(self.stac_root)
        if str(directory).endswith("out"):
            directory = str(directory)[:-4]

        def parent(url) -> str:
            parsed = urlparse(url)
            path = parsed.path
            parent_path = os.path.dirname(path)
            # Reconstruct the URL with the parent path
            return str(parsed._replace(path=parent_path).geturl())

        root = parent(self.stac_root)

        def copy_asset(asset_path: str) -> str:
            asset_path_parsed = urlparse(asset_path)
            relative_path = os.path.relpath(asset_path, root)
            dest_path = urljoin(str(directory) + "/", relative_path)
            Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
            if asset_path_parsed.scheme in ("http", "https"):
                response = requests.get(asset_path)
                response.raise_for_status()
                with open(dest_path, "wb") as f:
                    f.write(response.content)
            else:
                if asset_path.startswith("file://"):
                    asset_path = asset_path[7:]
                shutil.copy(asset_path, dest_path)
            return str(dest_path)

        self.stac_root_local = copy_asset(self.stac_root)

        for asset in stac_assets:
            copy_asset(asset)

        return get_items_from_stac_catalog(self.stac_root_local)

    def create_flask_response(self) -> Response:
        return self.flask_response_from_write_assets()
