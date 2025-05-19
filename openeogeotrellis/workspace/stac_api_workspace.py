import json
import logging
from pathlib import PurePath, Path
from typing import Union, Callable, Optional

from openeo_driver.util.http import requests_with_retry
from openeo_driver.workspace import Workspace, _merge_collection_metadata
from pystac import STACObject, Collection, Item, Asset
import pystac_client
from pystac_client import ConformanceClasses, stac_api_io
import requests
import requests.adapters
from requests import JSONDecodeError
from urllib3 import Retry

from openeogeotrellis.integrations.stac import ResilientStacIO

_log = logging.getLogger(__name__)


class StacApiResponseError(Exception):
    pass


class StacApiWorkspace(Workspace):
    REQUESTS_TIMEOUT_SECONDS = 60

    def __init__(
        self,
        root_url: str,
        export_asset: Callable[
            [Asset, PurePath, PurePath, bool],  # asset, merge, relative_asset_path, remove_original
            str,  #  workspace URI
        ],
        asset_alternate_id: str,
        additional_collection_properties: dict = None,
        get_access_token: Callable[[bool], str] = None,  # fresh => access_token
    ):
        """
        :param root_url: the URL to the STAC API's root catalog
        :param additional_collection_properties: top-level Collection properties to include in the request
        :param get_access_token: supply an access token, if needed
        :param export_asset: copy/move an asset and return its workspace URI, to be used as an alternate URI
        :param asset_alternate_id
        """

        if additional_collection_properties is None:
            additional_collection_properties = {}

        self.root_url = root_url
        self._additional_collection_properties = additional_collection_properties
        self._get_access_token = get_access_token
        self._export_asset = export_asset
        self._asset_alternate_id = asset_alternate_id

    def import_file(self, common_path: Union[str, Path], file: Path, merge: str, remove_original: bool = False) -> str:
        raise NotImplementedError(f"export_workspace: import_file to {self.root_url} not implemented {common_path}")

    def import_object(
        self, common_path: Union[str, Path], s3_uri: str, merge: str, remove_original: bool = False
    ) -> str:
        raise NotImplementedError(f"export_workspace: import_file to {self.root_url} not implemented {common_path}")

    def merge(self, stac_resource: STACObject, target: PurePath, remove_original: bool = False) -> STACObject:
        self._assert_catalog_supports_necessary_api()

        stac_resource = stac_resource.full_copy()

        if isinstance(stac_resource, Collection):
            new_collection = stac_resource
            del stac_resource

            with requests_with_retry(
                total=3,
                backoff_factor=2,
                allowed_methods=Retry.DEFAULT_ALLOWED_METHODS.union({"POST"}),
            ) as session:
                collection_id = target.name
                existing_collection = None

                try:
                    stac_io = ResilientStacIO(session=session)
                    existing_collection = Collection.from_file(
                        f"{self.root_url}/collections/{collection_id}", stac_io=stac_io
                    )
                except Exception as e:
                    if self._is_not_found_error(e):  # not exceptional
                        pass
                    else:
                        raise

                _log.info(
                    f"merging into {'existing' if existing_collection else 'new'}"
                    f" {self.root_url}/collections/{collection_id}"
                )

                merged_collection = (
                    _merge_collection_metadata(existing_collection, new_collection) if existing_collection
                    else new_collection
                )

                self._upload_collection(
                    merged_collection,
                    collection_id,
                    modify_existing=bool(existing_collection),
                    session=session,
                )

                for new_item in new_collection.get_items():
                    for asset_key, asset in new_item.assets.items():
                        relative_asset_path = PurePath(asset_key)  # TODO: relies asset key == relative asset path; avoid?

                        # client takes care of copying asset and returns its workspace URI
                        workspace_uri = self._export_asset(
                            asset,
                            target,
                            relative_asset_path,
                            remove_original,
                        )
                        _log.info(f"exported asset {asset.get_absolute_href()} as {workspace_uri}")

                        asset.href = workspace_uri

                    self._upload_item(new_item, collection_id, session)

            def set_alternate_uri(_, asset) -> Asset:
                asset.extra_fields["alternate"] = {self._asset_alternate_id: asset.href}
                return asset

            return new_collection.map_assets(set_alternate_uri)
        else:
            raise NotImplementedError(f"merge from {stac_resource}")

    def _upload_collection(
        self, collection: Collection, collection_id: str, modify_existing: bool, session: requests.Session
    ):
        bare_collection = collection.clone()
        bare_collection.id = collection_id
        bare_collection.remove_hierarchical_links()
        bare_collection.remove_links("queryables")
        bare_collection.remove_links("aggregations")
        bare_collection.remove_links("aggregate")
        bare_collection.extra_fields.update(self._additional_collection_properties)

        request_json = bare_collection.to_dict(include_self_link=False)

        if modify_existing:
            update_collection_url = f"{self.root_url}/collections/{collection_id}"

            _log.debug(f"updating existing collection at {update_collection_url}: {self._dumps_minified(request_json)}")

            resp = self._authorized(
                lambda access_token: session.put(
                    update_collection_url,
                    json=request_json,
                    timeout=self.REQUESTS_TIMEOUT_SECONDS,
                    headers=self._headers(access_token),
                )
            )
        else:
            create_collection_url = f"{self.root_url}/collections"

            _log.debug(f"creating new collection at {create_collection_url}: {self._dumps_minified(request_json)}")

            resp = self._authorized(
                lambda access_token: session.post(
                    create_collection_url,
                    json=request_json,
                    timeout=self.REQUESTS_TIMEOUT_SECONDS,
                    headers=self._headers(access_token),
                )
            )

        self._raise_for_status(resp)

    def _upload_item(self, item: Item, collection_id: str, session: requests.Session):
        item.remove_hierarchical_links()
        item.collection_id = collection_id

        create_item_url = f"{self.root_url}/collections/{collection_id}/items"
        request_json = item.to_dict(include_self_link=False)

        _log.debug(f"creating new item at {create_item_url}: {self._dumps_minified(request_json)}")

        resp = self._authorized(
            lambda access_token: session.post(
                create_item_url,
                json=request_json,
                timeout=self.REQUESTS_TIMEOUT_SECONDS,
                headers=self._headers(access_token),
            )
        )

        self._raise_for_status(resp)

    @staticmethod
    def _headers(access_token: Optional[str]) -> dict:
        return {"Authorization": f"Bearer {access_token}"} if access_token else None

    def _authorized(self, request: Callable[[Optional[str]], requests.Response]) -> requests.Response:
        resp = request(self._get_access_token(False) if self._get_access_token else None)

        if resp.status_code == 401:  # Unauthorized TODO: avoid magic number
            resp = request(self._get_access_token(True) if self._get_access_token else None)

        return resp

    @staticmethod
    def _raise_for_status(resp: requests.Response):  # TODO: rename to e.g. _handle_error_response?
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            try:
                error_body = StacApiWorkspace._dumps_minified(e.response.json())
            except JSONDecodeError:
                error_body = e.response.text  # best effort

            if e.response.status_code == 409:  # Conflict TODO: avoid magic number
                _log.warning(
                    f"ignoring error response to POST that was retried because of a "
                    f"transient (network) error: {e} with response body: {error_body}",
                    exc_info=True,
                )
            else:
                raise StacApiResponseError(f"{e} with response body: {error_body}") from e

    def _assert_catalog_supports_necessary_api(self):
        # TODO: reduce code duplication with openeo_driver.util.http.requests_with_retry
        retry = requests.adapters.Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=frozenset([429, 500, 502, 503, 504]),
        )

        stac_io = pystac_client.stac_api_io.StacApiIO(timeout=self.REQUESTS_TIMEOUT_SECONDS, max_retries=retry)
        root_catalog_client = pystac_client.Client.open(self.root_url, stac_io=stac_io)

        if not root_catalog_client.conforms_to(ConformanceClasses.COLLECTIONS):
            raise ValueError(f"{self.root_url} does not support Collections")

        conforms_to = root_catalog_client.get_conforms_to()

        supports_collection_methods = any(
            conformance_class.endswith("/collections/extensions/transaction") for conformance_class in conforms_to
        )

        if not supports_collection_methods:
            raise ValueError(f"{self.root_url} does not support Transaction extension for Collections")

        supports_item_methods = any(
            conformance_class.endswith("/ogcapi-features/extensions/transaction") for conformance_class in conforms_to
        )

        if not supports_item_methods:
            raise ValueError(f"{self.root_url} does not support Transaction extension for Items")

    def _is_not_found_error(self, e: BaseException) -> bool:
        return (isinstance(e, requests.HTTPError) and e.response.status_code == 404) or (
            e.__cause__ is not None and self._is_not_found_error(e.__cause__)
        )

    @staticmethod
    def _dumps_minified(obj) -> str:
        return json.dumps(obj, separators=(",", ":"))
