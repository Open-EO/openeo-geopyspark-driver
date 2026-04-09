from openeogeotrellis import query_stac
import pytest
from contextlib import nullcontext
from .test_load_stac import _mock_stac_api

def test_item_collection_response():
    pass

def test_empty_response(requests_mock, expectation=nullcontext()):
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/collection"

    _mock_stac_api(
        requests_mock,
        stac_api_root_url,
        stac_collection_url,
        feature_collection={
            "type": "FeatureCollection",
            "features": [],
        },
    )

    with expectation:
        # TODO make an actual query
        query_response = query_stac.item_collection_from_stac_query(
            url=stac_collection_url,
            spatial_extent={"west": 0.0, "south": 50.0, "east": 1.0, "north": 51.0},
            temporal_extent=("2022-01-01", "2022-01-02"),
        )

    print("Done")
