from pathlib import PurePath, Path
from typing import Union
from urllib.error import HTTPError

import pystac_client
import requests
from openeo_driver.workspace import Workspace
from pystac import STACObject, Catalog, Collection


class StacApiWorkspace(Workspace):
    def __init__(self, root_url: str):
        self.root_url = root_url

    def import_file(self, common_path: Union[str, Path], file: Path, merge: str, remove_original: bool = False) -> str:
        raise NotImplementedError

    def import_object(
        self, common_path: Union[str, Path], s3_uri: str, merge: str, remove_original: bool = False
    ) -> str:
        raise NotImplementedError

    def merge(self, stac_resource: STACObject, target: PurePath, remove_original: bool = False) -> STACObject:
        stac_resource = stac_resource.full_copy()

        if isinstance(stac_resource, Collection):
            new_collection = stac_resource

            client = pystac_client.Client.open(self.root_url)

            if not self._supports_necessary_operations(client):
                # TODO: raise from within method?
                raise ValueError(f"STAC API {self.root_url} does not support transaction extensions")

            existing_collection = None
            try:
                existing_collection = Collection.from_file(f"{self.root_url}/{target}")
            except Exception as e:
                if not self._is_not_found_error(e):  # TODO: fix double negative?
                    raise

            if existing_collection:
                # TODO: update collection and add items
                raise NotImplementedError
            else:
                for item in new_collection.get_items():
                    item.remove_hierarchical_links()
                    requests.post(f"{self.root_url}/{target}/items", json=item.to_dict(include_self_link=False))

                new_collection.remove_hierarchical_links()
                # TODO: assume target is "$collection_id" rather than "collections/$collection_id"?
                requests.post(f"{self.root_url}/collections", json=stac_resource.to_dict(include_self_link=False))

                merged_collection = new_collection

            return merged_collection
        else:
            raise NotImplementedError(stac_resource)

    @staticmethod
    def _supports_necessary_operations(root_catalog: Catalog):
        conforms_to = root_catalog.extra_fields.get("conformsTo", [])
        # TODO: check additional extensions like for GET /collections as well?

        supports_collection_methods = any(
            conformance_class.endswith("/collections/extensions/transaction") for conformance_class in conforms_to
        )

        supports_item_methods = any(
            conformance_class.endswith("/ogcapi-features/extensions/transaction") for conformance_class in conforms_to
        )

        return supports_collection_methods and supports_item_methods

    def _is_not_found_error(self, e: BaseException) -> bool:
        return (isinstance(e, HTTPError) and e.code == 404) or (
            e.__cause__ is not None and self._is_not_found_error(e.__cause__)
        )
