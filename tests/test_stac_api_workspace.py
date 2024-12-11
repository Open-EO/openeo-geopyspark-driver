import datetime as dt
from pathlib import PurePath, Path

from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item

from openeogeotrellis.workspace import StacApiWorkspace


def test_merge_new(requests_mock, urllib_mock, tmp_path):
    stac_api_workspace = StacApiWorkspace(root_url="https://stacapi.test")
    collection_id = "new_collection"
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
    create_item_mock = requests_mock.post(f"{stac_api_workspace.root_url}/collections/{collection_id}/items")

    new_collection = _collection(root_path=tmp_path, collection_id=collection_id, item_id=item_id)
    stac_api_workspace.merge(stac_resource=new_collection, target=PurePath(f"collections/{collection_id}"))

    assert create_collection_mock.called_once
    assert create_collection_mock.last_request.json() == dict(new_collection.to_dict(), links=[])

    assert create_item_mock.called_once
    assert create_item_mock.last_request.json() == dict(new_collection.get_item(item_id).to_dict(), links=[])


def _collection(root_path: Path, collection_id: str, item_id: str) -> Collection:
    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[None, None]])),
    )

    collection.add_item(Item(id=item_id, geometry=None, bbox=None, datetime=dt.datetime.utcnow(), properties={}))
    collection.normalize_and_save(root_href=str(root_path))

    return collection
