from pathlib import PurePath

from pystac import Collection, Extent, SpatialExtent, TemporalExtent

from openeogeotrellis.workspace import StacApiWorkspace


def test_basic(requests_mock, urllib_mock):
    stac_api_workspace = StacApiWorkspace(root_url="https://stacapi.test")

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

    new_collection = Collection(
        id="new_collection",
        description="new collection",
        extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[None, None]])),
    )

    stac_api_workspace.merge(stac_resource=new_collection, target=PurePath("collections/new_collection"))

    assert create_collection_mock.last_request.json()["id"] == "new_collection"
