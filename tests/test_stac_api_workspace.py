import datetime as dt
import os
from pathlib import PurePath, Path
from typing import Optional

from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item

from openeogeotrellis.workspace import StacApiWorkspace


def test_merge_new(requests_mock, urllib_mock, tmp_path):
    stac_api_workspace = StacApiWorkspace(root_url="https://stacapi.test")
    target = PurePath("collections/new_collection")
    item_id = "new_item"

    # STAC API root catalog with "conformsTo" for pystac_client
    requests_mock.get(
        stac_api_workspace.root_url,
        json={
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "stacapi.test",
            "description": "stacapi.test",
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/collections/extensions/transaction",
                "https://api.stacspec.org/v1.0.0/ogcapi-features/extensions/transaction",
            ],
            "links": [],
        },
    )

    create_collection_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections")
    create_item_mock = requests_mock.post(f"{stac_api_workspace.root_url}/{target}/items")

    collection1 = _collection(root_path=tmp_path, collection_id="collection1", item_id=item_id)
    stac_api_workspace.merge(stac_resource=collection1, target=target)

    assert create_collection_mock.called_once
    assert create_collection_mock.last_request.json() == dict(collection1.to_dict(), id=target.name, links=[])

    assert create_item_mock.called_once
    assert create_item_mock.last_request.json() == dict(collection1.get_item(item_id).to_dict(), links=[])


def test_against_actual_stac_api(tmp_path):
    root_url = os.environ["STAC_API_ROOT_URL"]
    client_id = os.environ["STAC_API_AUTH_CLIENT_ID"]
    issuer = os.environ["STAC_API_AUTH_ISSUER"]
    username = os.environ["STAC_API_AUTH_USERNAME"]
    password = os.environ["STAC_API_AUTH_PASSWORD"]

    collection1 = _collection(root_path=tmp_path, collection_id="collection1", item_id=None)

    def get_access_token():
        from openeo.rest.auth.oidc import OidcClientInfo, OidcProviderInfo, OidcResourceOwnerPasswordAuthenticator

        authenticator = OidcResourceOwnerPasswordAuthenticator(
            client_info=OidcClientInfo(client_id=client_id, provider=OidcProviderInfo(issuer=issuer)),
            username=username,
            password=password,
        )

        return authenticator.get_tokens().access_token

    additional_collection_properties = {
        "_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}
    }

    stac_api_workspace = StacApiWorkspace(
        root_url=root_url,
        additional_collection_properties=additional_collection_properties,
        get_access_token=get_access_token,
    )

    stac_api_workspace.merge(collection1, target=PurePath("collections/test_collection_for_stac_merge"))


def _collection(root_path: Path, collection_id: str, item_id: Optional[str] = None) -> Collection:
    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[None, None]])),
    )

    if item_id:
        collection.add_item(Item(id=item_id, geometry=None, bbox=None, datetime=dt.datetime.utcnow(), properties={}))

    collection.normalize_and_save(root_href=str(root_path))
    assert collection.validate_all() == (1 if item_id else 0)

    return collection
