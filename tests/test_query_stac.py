from openeogeotrellis import query_stac
import pytest
from contextlib import nullcontext
from .test_load_stac import _mock_stac_api

def test_non_empty_response(requests_mock, test_data):
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/collection"

    stac_item = test_data.load_json(
        filename="stac/recursive-stac-example/sub-folder/openEO_2023-06-04Z.tif.json",
    )

    _mock_stac_api(
        requests_mock,
        stac_api_root_url,
        stac_collection_url,
        feature_collection={
            "type": "FeatureCollection",
            "features": [stac_item],
        },
    )

    query_response = query_stac.item_collection_from_stac_query(
        url=stac_collection_url,
        spatial_extent={"west": 4.0, "south": 50.0, "east": 6.0, "north": 52.0},
        temporal_extent=("2023-06-04", "2022-06-05"),
    )

    assert len(query_response) == 1
    assert "openEO_2023-06-04Z.tif" in  query_response.to_dict().get("features")[0]["assets"]


def test_empty_collection(requests_mock):
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

    query_response = query_stac.item_collection_from_stac_query(
        url=stac_collection_url,
        spatial_extent={"west": 0.0, "south": 50.0, "east": 1.0, "north": 51.0},
        temporal_extent=("2022-01-01", "2022-01-02"),
    )

    assert len(query_response) == 0
    assert query_response.to_dict().get("features") == []
