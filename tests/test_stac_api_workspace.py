import datetime as dt
from pathlib import PurePath, Path
from typing import Dict

import pytest
import responses
from mock import ANY, MagicMock
from openeo_driver.testing import DictSubSet
from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item, Asset, Link, RelType

from openeogeotrellis.workspace import StacApiWorkspace
from openeogeotrellis.workspace import vito_stac_api_workspace
from openeogeotrellis.workspace.stac_api_workspace import StacApiResponseError


def test_merge_new(requests_mock, tmp_path):
    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=_export_asset,
        asset_alternate_id="file",
        get_access_token=lambda _: "s3cr3t",
    )
    target = PurePath("new_collection")
    asset_path = Path("/path") / "to" / "asset1.tif"

    _mock_stac_api_root_catalog(requests_mock, stac_api_workspace.root_url)
    requests_mock.get(f"{stac_api_workspace.root_url}/collections/{target}", status_code=404)

    create_collection_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections")
    create_item_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections/{target}/items")

    collection1 = _collection(root_path=tmp_path / "collection1", collection_id="collection1", asset_path=asset_path)
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
    assert create_collection_mock.last_request.headers["Authorization"] == "Bearer s3cr3t"
    assert create_collection_mock.last_request.json() == dict(
        collection1.to_dict(),
        id=str(target),
        links=[
            {
                "rel": "derived_from",
                "href": "https://src.test/asset1.tif",
            }
        ],
    )

    assert create_item_mock.called_once
    assert create_item_mock.last_request.headers["Authorization"] == "Bearer s3cr3t"
    assert create_item_mock.last_request.json() == dict(
        collection1.get_item(id=asset_path.name).to_dict(), collection=str(target), links=[]
    )


def test_merge_into_existing(requests_mock, tmp_path):
    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=_export_asset,
        asset_alternate_id="file",
        get_access_token=lambda _: "s3cr3t",
    )
    target = PurePath("existing_collection")
    asset_path = Path("/path") / "to" / "asset2.tif"

    _mock_stac_api_root_catalog(requests_mock, stac_api_workspace.root_url)
    update_collection_mock = requests_mock.put(f"{stac_api_workspace.root_url}/collections/{target}")
    create_item_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections/{target}/items")

    existing_collection = _collection(
        root_path=tmp_path / "collection1",
        collection_id="existing_collection",
        asset_path=Path("asset1.tif"),
        spatial_extent=SpatialExtent([[0, 50, 2, 52]]),
        temporal_extent=TemporalExtent([[
            dt.datetime.fromisoformat("2024-12-17T00:00:00+00:00"),
            dt.datetime.fromisoformat("2024-12-19T00:00:00+00:00")
        ]]),
    )
    requests_mock.get(
        f"{stac_api_workspace.root_url}/collections/{target}",
        json=existing_collection.to_dict(),
    )

    new_collection = _collection(
        root_path=tmp_path / "collection2",
        collection_id="collection2",
        asset_path=asset_path,
        spatial_extent=SpatialExtent([[1, 51, 3, 53]]),
        temporal_extent=TemporalExtent([[
            dt.datetime.fromisoformat("2024-12-18T00:00:00+00:00"),
            dt.datetime.fromisoformat("2024-12-20T00:00:00+00:00")
        ]]),
    )

    imported_collection = stac_api_workspace.merge(new_collection, target=target)
    assert isinstance(imported_collection, Collection)

    assert _asset_workspace_uris(imported_collection, alternate_key="file") == {
        "asset2.tif": str(asset_path),
    }

    assert update_collection_mock.called_once
    assert update_collection_mock.last_request.headers["Authorization"] == "Bearer s3cr3t"
    assert update_collection_mock.last_request.json() == dict(
        new_collection.to_dict(),
        id=str(target),
        description=str(target),
        links=[
            {
                "rel": "derived_from",
                "href": "https://src.test/asset1.tif",
            },
            {
                "rel": "derived_from",
                "href": "https://src.test/asset2.tif",
            },
        ],
        extent=Extent(
            SpatialExtent([[0, 50, 3, 53]]),
            TemporalExtent([[
                dt.datetime.fromisoformat("2024-12-17T00:00:00+00:00"),
                dt.datetime.fromisoformat("2024-12-20T00:00:00+00:00")
            ]])
        ).to_dict()
    )

    assert create_item_mock.called_once
    assert create_item_mock.last_request.headers["Authorization"] == "Bearer s3cr3t"
    assert create_item_mock.last_request.json() == dict(
        new_collection.get_item(id="asset2.tif").to_dict(),
        collection=str(target),
        links=[],
    )


@responses.activate(registry=responses.registries.OrderedRegistry)
def test_merge_resilience(tmp_path, caplog):
    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=_export_asset,
        asset_alternate_id="file",
    )
    target = PurePath("new_collection")
    asset_path = Path("/path") / "to" / "asset1.tif"

    get_root_catalog_error_resp = responses.get(stac_api_workspace.root_url, status=500)
    get_root_catalog_ok_resp = responses.get(
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

    get_collection_error_resp = responses.get(f"{stac_api_workspace.root_url}/collections/{target}", status=500)
    get_collection_not_found_resp = responses.get(f"{stac_api_workspace.root_url}/collections/{target}", status=404)

    create_collection_error_resp = responses.post(f"{stac_api_workspace.root_url}/collections", status=500)
    create_collection_conflict_resp = responses.post(
        f"{stac_api_workspace.root_url}/collections",
        status=409,
        json={
            "error": "collection already exists",
        },
    )

    create_item_error_resp = responses.post(f"{stac_api_workspace.root_url}/collections/{target}/items", status=500)
    create_item_conflict_resp = responses.post(
        f"{stac_api_workspace.root_url}/collections/{target}/items",
        status=409,
        json={
            "error": "item already exists",
        },
    )

    collection1 = _collection(root_path=tmp_path / "collection1", collection_id="collection1", asset_path=asset_path)
    stac_api_workspace.merge(stac_resource=collection1, target=target)

    assert get_root_catalog_error_resp.call_count == 1
    assert get_root_catalog_ok_resp.call_count == 1

    assert get_collection_error_resp.call_count == 1
    assert get_collection_not_found_resp.call_count == 1

    assert create_collection_error_resp.call_count == 1
    assert create_collection_conflict_resp.call_count == 1

    assert create_item_error_resp.call_count == 1
    assert create_item_conflict_resp.call_count == 1

    warn_logs = [r.message for r in caplog.records if r.levelname == "WARNING"]
    assert (
        "ignoring error response to POST that was retried because of a transient (network) error: "
        "409 Client Error: Conflict for url: https://stacapi.test/collections "
        'with response body: {"error":"collection already exists"}'
    ) in warn_logs
    assert (
        "ignoring error response to POST that was retried because of a transient (network) error: "
        "409 Client Error: Conflict for url: https://stacapi.test/collections/new_collection/items"
        ' with response body: {"error":"item already exists"}'
    ) in warn_logs


def test_error_details(tmp_path, requests_mock):
    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=_export_asset,
        asset_alternate_id="file",
    )

    target = PurePath("new_collection")
    asset_path = Path("/path") / "to" / "asset1.tif"

    _mock_stac_api_root_catalog(requests_mock, stac_api_workspace.root_url)
    requests_mock.get(f"{stac_api_workspace.root_url}/collections/{target}", status_code=404)

    requests_mock.post(
        f"{stac_api_workspace.root_url}/collections",
        status_code=400,
        reason="Bad Request",
        json={"detail": "Invalid collection authorizations"},
    )

    collection1 = _collection(root_path=tmp_path / "collection1", collection_id="collection1", asset_path=asset_path)

    with pytest.raises(
        StacApiResponseError,
        match="400 Client Error: Bad Request for url: https://stacapi.test/collections"
        ' with response body: {"detail":"Invalid collection authorizations"}',
    ):
        stac_api_workspace.merge(collection1, target)


def test_merge_target_supports_path(requests_mock, tmp_path):
    asset_path = Path("/path") / "to" / "asset.tif"
    collection = _collection(root_path=tmp_path / "collection", collection_id="collection", asset_path=asset_path)

    export_asset_mock = MagicMock(wraps=_export_asset)

    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=export_asset_mock,
        asset_alternate_id="file",
    )

    target = PurePath("path/to/collection_id")

    _mock_stac_api_root_catalog(requests_mock, stac_api_workspace.root_url)
    requests_mock.get(f"{stac_api_workspace.root_url}/collections/{target.name}", status_code=404)
    create_collection_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections")
    create_item_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections/{target.name}/items")

    stac_api_workspace.merge(collection, target)

    assert create_collection_mock.called_once
    assert create_item_mock.called_once
    export_asset_mock.assert_called_once_with(ANY, target, ANY, ANY)


def test_vito_stac_api_workspace_helper(tmp_path, requests_mock, mock_s3_bucket):
    asset_path = tmp_path / "asset.tif"
    with open(asset_path, "wb") as f:
        f.write(b"data")
    source_file_mtime_ns = asset_path.stat().st_mtime_ns

    collection = _collection(root_path=tmp_path / "collection", collection_id="collection", asset_path=asset_path)

    oidc_issuer = "https://auth.test/realms/test"

    stac_api_workspace = vito_stac_api_workspace(
        root_url="https://stacapi.test",
        oidc_issuer=oidc_issuer,
        oidc_client_id="abc123",
        oidc_client_secret="s3cr3t",
        asset_bucket=mock_s3_bucket.name,
        asset_prefix=lambda merge: f"assets/{merge}",
        additional_collection_properties={
            "_auth": {
                "read": ["anonymous"],
                "write": ["editor"],
            },
        },
    )

    target = PurePath("path/to/collection")

    # mock OIDC provider
    requests_mock.get(
        f"{oidc_issuer}/.well-known/openid-configuration",
        json={
            "token_endpoint": f"{oidc_issuer}/protocol/openid-connect/token",
        },
    )
    get_access_token_mock = requests_mock.post(
        f"{oidc_issuer}/protocol/openid-connect/token",
        json={"access_token": "4cc3ss_t0k3n"},
    )

    # mock STAC API
    _mock_stac_api_root_catalog(requests_mock, stac_api_workspace.root_url)
    requests_mock.get(f"{stac_api_workspace.root_url}/collections/{target.name}", status_code=404)
    create_collection_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections")
    create_item_mock = requests_mock.post(
        f"{stac_api_workspace.root_url}/collections/{target.name}/items",
        [
            {"status_code": 401},
            {"status_code": 201},
        ],
    )

    stac_api_workspace.merge(collection, target)

    assert get_access_token_mock.call_count == 2  # fetches new access_token upon 401 response
    assert create_collection_mock.called_once
    assert create_collection_mock.last_request.json() == DictSubSet(
        id=target.name,
        _auth={
            "read": ["anonymous"],
            "write": ["editor"],
        },
    )
    assert create_item_mock.call_count == 2  # single item is retried with new access_token

    object_keys = {obj.key for obj in mock_s3_bucket.objects.all()}
    assert object_keys == {"assets/path/to/collection/asset.tif"}

    asset_key = next(iter(object_keys))
    asset_object_metadata = mock_s3_bucket.Object(key=asset_key).metadata

    assert asset_object_metadata["md5"] == "8d777f385d3dfec8815d20f7496026dc"
    assert int(asset_object_metadata["mtime"]) == pytest.approx(source_file_mtime_ns, abs=1_000_000_000)  # 1s leeway


def _mock_stac_api_root_catalog(requests_mock, root_url: str):
    # STAC API root catalog with "conformsTo" for pystac_client
    requests_mock.get(
        root_url,
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



def _export_asset(asset: Asset, merge: PurePath, relative_asset_path: PurePath, remove_original: bool) -> str:
    assert isinstance(merge, PurePath)
    assert isinstance(relative_asset_path, PurePath)
    assert isinstance(remove_original, bool)
    # actual copying behaviour is the responsibility of the workspace creator
    return asset.get_absolute_href()


def _asset_workspace_uris(collection: Collection, alternate_key: str) -> Dict[str, str]:
    return {
        asset_key: asset.extra_fields["alternate"][alternate_key]
        for item in collection.get_items()
        for asset_key, asset in item.get_assets().items()
    }


def _collection(
    root_path: Path,
    collection_id: str,
    asset_path: Path,
    spatial_extent: SpatialExtent = SpatialExtent([[-180, -90, 180, 90]]),
    temporal_extent: TemporalExtent = TemporalExtent([[None, None]]),
) -> Collection:
    root_path.mkdir()

    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(spatial_extent, temporal_extent),
    )

    item_id = asset_key = asset_path.name

    item = Item(id=item_id, geometry=None, bbox=None, datetime=dt.datetime.now(dt.timezone.utc), properties={})
    asset = Asset(href=str(asset_path))

    item.add_asset(asset_key, asset)
    collection.add_item(item)

    collection.add_link(Link(rel=RelType.DERIVED_FROM, target=f"https://src.test/{asset_path.name}"))

    collection.normalize_and_save(root_href=str(root_path))
    assert collection.validate_all() == (1 if item_id else 0)

    return collection
