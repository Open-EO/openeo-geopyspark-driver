from pathlib import PurePath, Path
from typing import Union

import pystac_client
from openeo_driver.workspace import Workspace
from pystac import STACObject, Catalog


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
        client = pystac_client.Client.open(self.root_url)

        if not self._supports_transaction_extensions(client):
            raise ValueError(f"STAC API {self.root_url} does not support transaction extensions")

        # TODO: create/update collection
        # TODO: add items
        raise NotImplementedError

    @staticmethod
    def _supports_transaction_extensions(root_catalog: Catalog):
        conforms_to = root_catalog.extra_fields.get("conformsTo", [])

        supports_collection_methods = any(
            conformance_class.endswith("/collections/extensions/transaction") for conformance_class in conforms_to
        )

        supports_item_methods = any(
            conformance_class.endswith("/ogcapi-features/extensions/transaction") for conformance_class in conforms_to
        )

        return supports_collection_methods and supports_item_methods
