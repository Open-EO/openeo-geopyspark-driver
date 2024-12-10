from pathlib import PurePath

from pystac import Collection, Extent, SpatialExtent, TemporalExtent

from openeogeotrellis.workspace import StacApiWorkspace


def test_basic(requests_mock):
    stac_api_workspace = StacApiWorkspace(root_url="https://stacapi.test")

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

    collection1 = Collection(
        id="collection1",
        description="collection1",
        extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[None, None]])),
    )

    stac_api_workspace.merge(stac_resource=collection1, target=PurePath("collections/collection1"))
