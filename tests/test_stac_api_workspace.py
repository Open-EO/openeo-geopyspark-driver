import datetime as dt
import json
from pathlib import PurePath, Path
from typing import Dict

from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item, Asset

from openeogeotrellis.workspace import StacApiWorkspace


def test_merge_new(requests_mock, urllib_mock, tmp_path):
    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=_export_asset,
        asset_alternate_id="file",
    )
    target = PurePath("new_collection")
    asset_path = Path("/path") / "to" / "asset1.tif"

    _mock_stac_api_root_catalog(requests_mock, stac_api_workspace.root_url)
    # no need to mock URL for existing Collection as urllib_mock will return a 404 by default

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
    assert create_collection_mock.last_request.json() == dict(collection1.to_dict(), id=str(target), links=[])

    assert create_item_mock.called_once
    assert create_item_mock.last_request.json() == dict(
        collection1.get_item(id=asset_path.name).to_dict(), collection=str(target), links=[]
    )


def test_merge_into_existing(requests_mock, urllib_mock, tmp_path):
    stac_api_workspace = StacApiWorkspace(
        root_url="https://stacapi.test",
        export_asset=_export_asset,
        asset_alternate_id="file",
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
    urllib_mock.get(
        f"{stac_api_workspace.root_url}/collections/{target}",
        data=json.dumps(existing_collection.to_dict()),
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
    assert update_collection_mock.last_request.json() == dict(
        new_collection.to_dict(),
        id=str(target),
        description=str(target),
        links=[],
        extent=Extent(
            SpatialExtent([[0, 50, 3, 53]]),
            TemporalExtent([[
                dt.datetime.fromisoformat("2024-12-17T00:00:00+00:00"),
                dt.datetime.fromisoformat("2024-12-20T00:00:00+00:00")
            ]])
        ).to_dict()
    )

    assert create_item_mock.called_once
    assert create_item_mock.last_request.json() == dict(
        new_collection.get_item(id="asset2.tif").to_dict(),
        collection=str(target),
        links=[],
    )


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



def _export_asset(asset: Asset, collection_id: str, relative_asset_path: PurePath, remove_original: bool) -> str:
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

    collection.normalize_and_save(root_href=str(root_path))
    assert collection.validate_all() == (1 if item_id else 0)

    return collection
