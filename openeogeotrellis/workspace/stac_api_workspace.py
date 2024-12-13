from pathlib import PurePath, Path
from typing import Union, Callable
from urllib.error import HTTPError

import pystac_client
from openeo_driver.util.http import requests_with_retry
from openeo_driver.workspace import Workspace
from pystac import STACObject, Catalog, Collection, Item
from requests import Session


class StacApiWorkspace(Workspace):
    def __init__(
        self, root_url: str, additional_collection_properties=None, get_access_token: Callable[[], str] = None
    ):
        if additional_collection_properties is None:
            additional_collection_properties = {}

        self.root_url = root_url
        self._additional_collection_properties = additional_collection_properties
        self._get_access_token = get_access_token

    def import_file(self, common_path: Union[str, Path], file: Path, merge: str, remove_original: bool = False) -> str:
        raise NotImplementedError

    def import_object(
        self, common_path: Union[str, Path], s3_uri: str, merge: str, remove_original: bool = False
    ) -> str:
        raise NotImplementedError

    def merge(self, stac_resource: STACObject, target: PurePath, remove_original: bool = False) -> STACObject:
        stac_resource = stac_resource.full_copy()

        client = pystac_client.Client.open(self.root_url)
        if not self._supports_necessary_operations(client):
            # TODO: raise from within method?
            raise ValueError(f"STAC API {self.root_url} does not support transaction extensions")

        if isinstance(stac_resource, Collection):
            new_collection = stac_resource

            existing_collection = None
            try:
                existing_collection = Collection.from_file(f"{self.root_url}/{target}")
            except Exception as e:
                if self._is_not_found_error(e):
                    pass  # not exceptional: the target collection does not exist yet
                else:
                    raise

            if existing_collection:
                # TODO: update collection and add items
                raise NotImplementedError("merge into existing collection")
            else:
                with requests_with_retry() as session:
                    # TODO: only to quickly test against actual STAC API
                    session.headers = (
                        {"Authorization": f"Bearer {self._get_access_token()}"} if self._get_access_token else None
                    )

                    target_collection_id = self._upload_collection(new_collection, target, session)
                    for new_item in new_collection.get_items():
                        self._upload_item(new_item, target_collection_id, target, session)

            return new_collection
        else:
            raise NotImplementedError(f"merge from {stac_resource}")

    def _upload_collection(self, collection: Collection, target: PurePath, session: Session) -> str:
        target_collection_id = target.name

        bare_collection = collection.clone()
        bare_collection.id = target_collection_id
        bare_collection.remove_hierarchical_links()
        bare_collection.extra_fields.update(self._additional_collection_properties)

        resp = session.post(
            # TODO: assume target is "$collection_id" rather than "collections/$collection_id"?
            f"{self.root_url}/collections",
            json=bare_collection.to_dict(include_self_link=False),
        )
        resp.raise_for_status()

        return target_collection_id

    def _upload_item(self, item: Item, target_collection_id: str, target: PurePath, session: Session):
        item.remove_hierarchical_links()
        item.collection_id = target_collection_id

        resp = session.post(
            f"{self.root_url}/{target}/items",
            json=item.to_dict(include_self_link=False),
        )
        resp.raise_for_status()

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
