import json
import logging
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
from openeo_driver.util.stac_utils import get_files_from_stac_catalog, get_items_from_stac_catalog


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
        stac_assets = get_files_from_stac_catalog(self.stac_root, include_metadata=True)
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
                logging.info(f"URL: copy_asset({asset_path})")
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

        return get_items_from_stac_catalog(self.stac_root_local, make_hrefs_absolute=True)

    def create_flask_response(self) -> Response:
        return self.flask_response_from_write_assets()
