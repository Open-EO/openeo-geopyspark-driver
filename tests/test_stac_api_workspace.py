import datetime as dt
import os
from pathlib import PurePath, Path

import pytest
from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item, Asset

from openeogeotrellis.workspace import StacApiWorkspace


def test_merge_new(requests_mock, urllib_mock, tmp_path):
    def export_asset(asset: Asset, remove_original) -> (str, str):
        assert not remove_original
        return "file", asset.href

    stac_api_workspace = StacApiWorkspace(root_url="https://stacapi.test", export_asset=export_asset)
    target = PurePath("collections/new_collection")
    asset_path = Path("/path") / "to" / "asset1.tif"

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

    collection1 = _collection(root_path=tmp_path, collection_id="collection1", asset_path=asset_path)
    imported_collection = stac_api_workspace.merge(stac_resource=collection1, target=target)

    assert isinstance(imported_collection, Collection)

    asset_workspace_uris = {
        asset_key: asset.extra_fields["alternate"]["file"]
        for item in imported_collection.get_items()
        for asset_key, asset in item.get_assets().items()
    }

    assert asset_workspace_uris == {
        "asset1.tif": str(asset_path),
    }

    assert create_collection_mock.called_once
    assert create_collection_mock.last_request.json() == dict(collection1.to_dict(), id=target.name, links=[])

    assert create_item_mock.called_once
    assert create_item_mock.last_request.json() == dict(
        collection1.get_item(id=asset_path.name).to_dict(), collection=target.name, links=[]
    )


@pytest.mark.skipif("BUILD_ID" in os.environ, reason="don't run on Jenkins")
def test_against_actual_stac_api(tmp_path):
    root_url = os.environ["STAC_API_ROOT_URL"]
    asset_path = Path("/tmp/asset1.tif")

    collection1 = _collection(root_path=tmp_path, collection_id="collection1", asset_path=asset_path)

    def get_access_token():
        from openeo.rest.auth.oidc import OidcClientInfo, OidcProviderInfo, OidcResourceOwnerPasswordAuthenticator

        client_id = os.environ["STAC_API_AUTH_CLIENT_ID"]
        issuer = os.environ["STAC_API_AUTH_ISSUER"]
        username = os.environ["STAC_API_AUTH_USERNAME"]
        password = os.environ["STAC_API_AUTH_PASSWORD"]

        authenticator = OidcResourceOwnerPasswordAuthenticator(
            client_info=OidcClientInfo(client_id=client_id, provider=OidcProviderInfo(issuer=issuer)),
            username=username,
            password=password,
        )

        return authenticator.get_tokens().access_token

    additional_collection_properties = {
        "_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}
    }

    def log(asset: Asset, remove_original: bool) -> (str, str):
        assert not remove_original
        # neither copies the asset nor changes its href
        print(asset)
        return "file", asset.href

    stac_api_workspace = StacApiWorkspace(
        root_url=root_url,
        export_asset=log,
        additional_collection_properties=additional_collection_properties,
        get_access_token=get_access_token,
    )

    stac_api_workspace.merge(collection1, target=PurePath("collections/test_collection_for_stac_merge"))


def _collection(root_path: Path, collection_id: str, asset_path: Path) -> Collection:
    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[None, None]])),
    )

    item_id = asset_key = asset_path.name

    item = Item(id=item_id, geometry=None, bbox=None, datetime=dt.datetime.utcnow(), properties={})
    asset = Asset(href=str(asset_path))

    item.add_asset(asset_key, asset)
    collection.add_item(item)

    collection.normalize_and_save(root_href=str(root_path))
    assert collection.validate_all() == (1 if item_id else 0)

    return collection
