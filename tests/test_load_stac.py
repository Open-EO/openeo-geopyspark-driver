import datetime
import json
import re
from contextlib import nullcontext
from typing import List, Tuple
from unittest import mock, skip
import importlib.metadata

import dirty_equals
import geopandas
import openeo.metadata
import pystac
import pytest
import responses
import shapely.geometry
from openeo.testing.stac import StacDummyBuilder
from openeo.utils.version import ComparableVersion
from openeo_driver.backend import BatchJobMetadata, BatchJobs, LoadParameters
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.errors import OpenEOApiException
from openeo_driver.testing import approxify
from openeo_driver.util.date_math import now_utc
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv

from openeogeotrellis.load_stac import (
    NoDataAvailableException,
    SpatialFilteringGeometries,
    SpatioTemporalExtent,
    _StacMetadataParser,
    TemporalExtent,
    _prepare_context,
    spatiotemporal_extent_from_load_params,
    load_stac,
)
from openeogeotrellis.stac.own_job import extract_own_job_info
from openeogeotrellis.stac.extents import _SpatialExtent
from openeogeotrellis.stac.projection import ProjectionMetadata, _proj_code_to_epsg, get_proj_metadata
from openeogeotrellis.stac.stac_object_fetching import STAC_API_RETRY_TOTAL
from openeogeotrellis.testing import DummyStacApiServer, OpenSearchClientDumper


@pytest.mark.parametrize(
    "url, user_id, job_info_id",
    [
        ("https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results", "alice", "j-20240201abc123"),
        ("https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results", "bob", None),
        (
            "https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results/N2Q1MjMzODEzNzRiNjJlNmYyYWFkMWYyZjlmYjZlZGRmNjI0ZDM4MmE4ZjcxZGI2Z/095be1c7a37baf63b2044?expires=1707382334",
            "alice",
            None,
        ),
        (
            "https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results/N2Q1MjMzODEzNzRiNjJlNmYyYWFkMWYyZjlmYjZlZGRmNjI0ZDM4MmE4ZjcxZGI2Z/095be1c7a37baf63b2044?expires=1707382334",
            "bob",
            None,
        ),
        ("https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a", "alice", None),
    ],
)
def test_extract_own_job_info(url, user_id, job_info_id):
    batch_jobs = mock.Mock(spec=BatchJobs)

    def alices_single_job(job_id, user_id):
        return (
            BatchJobMetadata(id=job_id, status="finished", created=now_utc())
            if job_id == "j-20240201abc123" and user_id == "alice"
            else None
        )

    batch_jobs.get_job_info.side_effect = alices_single_job

    job_info = extract_own_job_info(url, user_id, batch_jobs=batch_jobs)

    if job_info_id is None:
        assert job_info is None
    else:
        assert job_info.id == job_info_id


def test_property_filter_from_parameter(requests_mock):
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/collection"

    def feature_collection(request, _) -> dict:
        assert request.qs["filter-lang"] == ["cql2-text"]
        assert request.qs["filter"] == [
            """"product_tile" = '31UFS'""".lower()  # https://github.com/jamielennox/requests-mock/issues/264
        ]

        return {
            "type": "FeatureCollection",
            "features": [],
        }

    search_mock = _mock_stac_api(requests_mock, stac_api_root_url, stac_collection_url, feature_collection)

    properties = {
        "product_tile": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {
                        "x": {"from_parameter": "value"},
                        "y": {"from_parameter": "tile_id"},
                    },
                    "result": True,
                }
            }
        }
    }

    load_params = LoadParameters(properties=properties)
    env = EvalEnv().push_parameters({"tile_id": "31UFS"})

    with pytest.raises(NoDataAvailableException):
        load_stac(
            url=stac_collection_url,
            load_params=load_params,
            env=env,
            layer_properties={},
            batch_jobs=None,
        )

    assert search_mock.called


@pytest.mark.parametrize(
    ["item_path"],
    [
        ("stac/issue609-api-temporal-bound-exclusive-eo-bands/item01.json",),
        ("stac/issue609-api-temporal-bound-exclusive-common-bands/item01.json",),
    ],
)
def test_stac_api_dimensions(requests_mock, test_data, item_path):
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/collection"

    stac_item = test_data.load_json(
        filename=item_path,
        preprocess={
            "asset01.tiff": f"file://{test_data.get_path('binary/load_stac/collection01/asset01.tif').absolute()}"
        },
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
    data_cube = load_stac(
        url=stac_collection_url,
        load_params=LoadParameters(),
        env=EvalEnv({"pyramid_levels": "highest"}),
        layer_properties={},
        batch_jobs=None,
    )

    assert {"x", "y", "t", "bands"} <= set(data_cube.metadata.dimension_names())


@pytest.mark.parametrize(
    ["band_names", "resolution", "expected_links"],
    [
        (
            ["AOT_10m"],
            10.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_AOT_10m.jp2"),
                    "title": "AOT_10m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": 0.0,
                    "bandNames": ["AOT_10m"],
                }
            ],
        ),
        (
            ["B01_60m"],
            60.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_B01_60m.jp2"),
                    "title": "B01_60m",
                    # has "raster:scale": 0.0001 and "raster:offset": -0.1
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": -1000.0,
                    "bandNames": ["B01_60m"],
                }
            ],
        ),
        (
            ["B01"],
            20.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_B01_20m.jp2"),
                    "title": "B01_20m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": -1000.0,
                    "bandNames": ["B01"],
                }
            ],
        ),
        (
            ["WVP_20m"],
            20.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_WVP_20m.jp2"),
                    "title": "WVP_20m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": 0.0,
                    "bandNames": ["WVP_20m"],
                }
            ],
        ),
        (
            ["WVP_60m"],
            60.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_WVP_60m.jp2"),
                    "title": "WVP_60m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": 0.0,
                    "bandNames": ["WVP_60m"],
                }
            ],
        ),
        (
            ["AOT_10m", "WVP_20m"],
            10.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_AOT_10m.jp2"),
                    "title": "AOT_10m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": 0.0,
                    "bandNames": ["AOT_10m"],
                },
                {
                    "href": dirty_equals.IsStr(regex=".*_WVP_20m.jp2"),
                    "title": "WVP_20m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": 0.0,
                    "bandNames": ["WVP_20m"],
                },
            ],
        ),
        (
            ["B01_20m", "SCL_20m"],
            20.0,
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_B01_20m.jp2"),
                    "title": "B01_20m",
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": -1000.0,
                    "bandNames": ["B01_20m"],
                },
                {
                    "href": dirty_equals.IsStr(regex=".*_SCL_20m.jp2"),
                    "title": "SCL_20m",
                    # has neither "raster:scale" nor "raster:offset"
                    "pixelValueScale": 1.0,
                    "pixelValueOffset": 0.0,
                    "bandNames": ["SCL_20m"],
                },
            ],
        ),
    ],
)
def test_resolution_and_offset_handling(
    requests_mock,
    test_data,
    band_names,
    resolution,
    expected_links,
):
    """
    resolution and offset behind a feature flag; alphabetical head tags are tested elsewhere
    Originally referred to as "LCFM Improvements"
    """
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/sentinel-2-l2a"

    features = test_data.load_json("stac/issue1043-api-proj-code/FeatureCollection.json")

    _mock_stac_api(
        requests_mock,
        stac_api_root_url,
        stac_collection_url,
        feature_collection=features,
    )

    context = _prepare_context(
        url=stac_collection_url,
        load_params=LoadParameters(bands=band_names),
        env=EvalEnv(),
    )

    assert (context.target_grid.cell_width, context.target_grid.cell_height) == (resolution, resolution)
    assert context.extent_crs == "EPSG:32636"

    dumper = OpenSearchClientDumper()
    assert [
        f["links"]
        for f in dumper.dump_opensearch_client_features(context.opensearch_client, add_pixel_value_scaling=True)
    ] == [expected_links]



@pytest.mark.parametrize(
    ["band_names", "expected_links"],
    [
        (
            ["WVP_20m"],
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_WVP_20m.jp2"),
                    "title": "WVP_20m",
                    "nodata": 0,
                    "datatype": 'uint16',
                    "bandNames": ["WVP_20m"],
                }
            ],
        ),
        (
            ["SCL_20m", "WVP_20m"],
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_SCL_20m.jp2"),
                    "title": "SCL_20m",
                    "nodata": 0,
                    "datatype": 'uint8',
                    "bandNames": ["SCL_20m"],
                },
                {
                    "href": dirty_equals.IsStr(regex=".*_WVP_20m.jp2"),
                    "title": "WVP_20m",
                    "nodata": 0,
                    "datatype": 'uint16',
                    "bandNames": ["WVP_20m"],
                },
            ],
        ),
        (
            ["B01_20m", "SCL_20m"],
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_B01_20m.jp2"),
                    "title": "B01_20m",
                    "nodata": 0,
                    "datatype": 'uint16',
                    "bandNames": ["B01_20m"],
                },
                {
                    "href": dirty_equals.IsStr(regex=".*_SCL_20m.jp2"),
                    "title": "SCL_20m",
                    "nodata": 0,
                    "datatype": 'uint8',
                    "bandNames": ["SCL_20m"],
                },
            ],
        ),
        (
            ["B01_20m", "SCL_20m", "CLD_20m"],
            [
                {
                    "href": dirty_equals.IsStr(regex=".*_B01_20m.jp2"),
                    "title": "B01_20m",
                    "nodata": 0,
                    "datatype": 'uint16',
                    "bandNames": ["B01_20m"],
                },
                {
                    "href": dirty_equals.IsStr(regex=".*_CLDPRB_20m.jp2"),
                    "title": "CLD_20m",
                    "datatype": 'uint8',
                    "bandNames": ["CLD_20m"],
                },
                {
                    "href": dirty_equals.IsStr(regex=".*_SCL_20m.jp2"),
                    "title": "SCL_20m",
                    "nodata": 0,
                    "datatype": 'uint8',
                    "bandNames": ["SCL_20m"],
                },
            ],
        ),
    ],
)
def test_data_type_and_nodata_handling(
    requests_mock,
    test_data,
    band_names,
    expected_links,
):
    """
    resolution and offset behind a feature flag; alphabetical head tags are tested elsewhere
    Originally referred to as "LCFM Improvements"
    """
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/sentinel-2-l2a"

    features = test_data.load_json("stac/issue1547-inconsistent-nodata-handling/FeatureCollection.json")

    _mock_stac_api(
        requests_mock,
        stac_api_root_url,
        stac_collection_url,
        feature_collection=features,
    )

    context = _prepare_context(
        url=stac_collection_url,
        load_params=LoadParameters(bands=band_names),
        env=EvalEnv(),
    )

    dumper = OpenSearchClientDumper()
    du = dumper.dump_opensearch_client_features(context.opensearch_client, add_data_type=True)

    assert context.extent_crs == "EPSG:32655"

    assert [
        f["links"]
        for f in du
    ] == [expected_links]

def _mock_stac_api(requests_mock, stac_api_root_url, stac_collection_url, feature_collection):
    requests_mock.get(
        stac_collection_url,
        json={
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "collection",
            "description": "collection",
            "license": "unknown",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                {
                    "rel": "root",
                    "href": stac_api_root_url,
                }
            ],
        },
    )

    catalog_response = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "stac.test",
        "description": "stac.test",
        "links": [],
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0-rc.1/item-search",
            "https://api.stacspec.org/v1.0.0-rc.3/item-search#filter",
        ],
    }

    requests_mock.get(stac_api_root_url, json=catalog_response)

    search_mock = requests_mock.get(f"{stac_api_root_url}/search", json=feature_collection)
    return search_mock


@skip("https://earthengine.openeo.org/ now gives error 503. This test would be better in scala with local data.")
def test_world_oom(requests_mock, test_data):
    stac_item_url = "https://oeo.test/b4fb00aee0a308"
    requests_mock.get(stac_item_url, json=test_data.load_json("stac/issue1055-world-oom/result_item.json"))
    load_stac(
        url=stac_item_url,
        load_params=LoadParameters(),
        env=EvalEnv({"pyramid_levels": "highest"}),
        layer_properties={},
        batch_jobs=None,
    )
    # TODO: assertions?


@pytest.mark.parametrize(
    ["featureflags", "env", "expectation"],
    [
        (
            {},
            EvalEnv({"pyramid_levels": "highest"}),
            pytest.raises(NoDataAvailableException),
        ),
        ({"allow_empty_cube": True}, EvalEnv({"pyramid_levels": "highest"}), nullcontext()),
        ({}, EvalEnv({"pyramid_levels": "highest", "allow_empty_cubes": True}), nullcontext()),
        ({}, EvalEnv({"allow_empty_cubes": True}), nullcontext()),  # pyramid_seq
    ],
)
def test_empty_cube_from_stac_api(requests_mock, featureflags, env, expectation):
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
        data_cube = load_stac(
            url=stac_collection_url,
            load_params=LoadParameters(
                spatial_extent={"west": 0.0, "south": 50.0, "east": 1.0, "north": 51.0},
                bands=["B04", "B03", "B02"],  # required if empty cubes allowed
                featureflags=featureflags,
            ),
            env=env,
            layer_properties={},
            batch_jobs=None,
        )

        assert data_cube.metadata.band_names == ["B04", "B03", "B02"]
        for level in data_cube.pyramid.levels.values():
            assert level.count() == 0


@pytest.mark.parametrize(
    ["featureflags", "env", "expectation"],
    [
        (
            {},
            EvalEnv({"pyramid_levels": "highest"}),
            pytest.raises(NoDataAvailableException),
        ),
        ({"allow_empty_cube": True}, EvalEnv({"pyramid_levels": "highest"}), nullcontext()),
        ({}, EvalEnv({"pyramid_levels": "highest", "allow_empty_cubes": True}), nullcontext()),
        ({}, EvalEnv({"allow_empty_cubes": True}), nullcontext()),  # pyramid_seq
    ],
)
@pytest.mark.parametrize(
    "item_path",
    [
        "stac/item01-eo-bands.json",
        "stac/item01-common-bands.json",
    ],
)
def test_empty_cube_from_non_intersecting_item(requests_mock, test_data, featureflags, env, expectation, item_path):
    stac_item_url = "https://stac.test/item.json"

    requests_mock.get(stac_item_url, json=test_data.load_json(item_path))

    with expectation:
        data_cube = load_stac(
            url=stac_item_url,
            load_params=LoadParameters(
                spatial_extent={"west": 0.0, "south": 50.0, "east": 1.0, "north": 51.0},
                featureflags=featureflags,
            ),
            env=env,
            layer_properties={},
            batch_jobs=None,
        )

        assert data_cube.metadata.band_names == ["A1"]
        for level in data_cube.pyramid.levels.values():
            assert level.count() == 0


@responses.activate
def test_stac_api_POST_item_search_resilience():
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/collection"
    stac_search_url = f"{stac_api_root_url}/search"

    responses.get(
        stac_collection_url,
        json={
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "collection",
            "description": "collection",
            "license": "unknown",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                {
                    "rel": "root",
                    "href": stac_api_root_url,
                }
            ],
        },
    )

    responses.get(
        stac_api_root_url,
        json={
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "stac.test",
            "description": "stac.test",
            "links": [
                {
                    "rel": "search",
                    "type": "application/geo+json",
                    "title": "STAC search",
                    "href": stac_search_url,
                    "method": "POST",
                },
            ],
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0-rc.1/item-search",
                "https://api.stacspec.org/v1.0.0-rc.3/item-search#filter",
            ],
        },
    )

    search_transient_error_resps = [
        responses.post(stac_search_url, status=500, body="some transient error"),
    ]

    # pass a property filter to do a POST item search like the API advertises above
    properties = {
        "product_tile": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {
                        "x": {"from_parameter": "value"},
                        "y": "31UFS",
                    },
                    "result": True,
                }
            }
        }
    }

    with pytest.raises(OpenEOApiException, match=r".*some transient error.*"):
        load_stac(
            stac_collection_url,
            load_params=LoadParameters(properties=properties),
            env=EvalEnv({"pyramid_levels": "highest"}),
        )

    assert [resp.call_count for resp in search_transient_error_resps] == [STAC_API_RETRY_TOTAL + 1]


class TestStacMetadataParser:
    # TODO: move/integrate these tests into openeo.metadata._StacMetadataParser tests
    def test_band_from_eo_bands_metadata(self):
        assert _StacMetadataParser()._band_from_eo_bands_metadata(
            {"name": "B04"},
        ) == openeo.metadata.Band(name="B04")
        assert _StacMetadataParser()._band_from_eo_bands_metadata(
            {"name": "B04", "common_name": "red", "center_wavelength": 0.665}
        ) == openeo.metadata.Band(name="B04", common_name="red", wavelength_um=0.665)

    def test_band_from_common_bands_metadata(self):
        assert _StacMetadataParser()._band_from_common_bands_metadata(
            {"name": "B04"},
        ) == openeo.metadata.Band(name="B04")
        assert _StacMetadataParser()._band_from_common_bands_metadata(
            {"name": "B04", "eo:common_name": "red", "eo:center_wavelength": 0.665}
        ) == openeo.metadata.Band(name="B04", common_name="red", wavelength_um=0.665)

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {
                    "type": "Catalog",
                    "id": "catalog123",
                    "description": "Catalog 123",
                    "stac_version": "1.0.0",
                    "stac_extensions": ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    "summaries": {
                        "eo:bands": [
                            {"name": "B04", "common_name": "red", "center_wavelength": 0.665},
                            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                        ],
                    },
                    "links": [],
                },
                ["B04", "B03"],
            ),
            (
                {
                    "type": "Catalog",
                    "id": "catalog123",
                    "description": "Catalog 123",
                    "stac_version": "1.1.0",
                    "summaries": {
                        "bands": [
                            {"name": "B04"},
                            {"name": "B03"},
                        ],
                    },
                    "links": [],
                },
                ["B04", "B03"],
            ),
        ],
    )
    def test_bands_from_stac_catatlog(self, data, expected):
        catalog = pystac.Catalog.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_catalog(catalog=catalog).band_names() == expected

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                StacDummyBuilder.collection(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    summaries={
                        "eo:bands": [
                            {"name": "B04", "common_name": "red", "center_wavelength": 0.665},
                            {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
                        ],
                    },
                ),
                ["B04", "B03"],
            ),
            (
                StacDummyBuilder.collection(
                    stac_version="1.1.0",
                    summaries={
                        "bands": [
                            {"name": "B04"},
                            {"name": "B03"},
                        ],
                    },
                ),
                ["B04", "B03"],
            ),
        ],
    )
    def test_bands_from_stac_collection(self, data, expected):
        collection = pystac.Collection.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_collection(collection=collection).band_names() == expected

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                StacDummyBuilder.item(
                    stac_version="1.0.0",
                    stac_extensions=["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
                    properties={
                        "datetime": "2023-10-01T00:00:00Z",
                        "eo:bands": [{"name": "B04"}, {"name": "B03"}],
                    },
                ),
                ["B04", "B03"],
            ),
            (
                StacDummyBuilder.item(
                    stac_version="1.1.0",
                    properties={
                        "datetime": "2023-10-01T00:00:00Z",
                        "bands": [{"name": "B04"}, {"name": "B03"}],
                    },
                ),
                ["B04", "B03"],
            ),
        ],
    )
    def test_bands_from_stac_item(self, data, expected):
        item = pystac.Item.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_item(item=item).band_names() == expected

    @pytest.mark.parametrize(
        ["data", "expected"],
        [
            (
                {
                    "href": "https://stac.test/asset.tif",
                    "eo:bands": [
                        {"name": "B04"},
                        {"name": "B03"},
                    ],
                },
                ["B04", "B03"],
            ),
            (
                {
                    "href": "https://stac.test/asset.tif",
                    "bands": [
                        {"name": "B04"},
                        {"name": "B03"},
                    ],
                },
                ["B04", "B03"],
            ),
        ],
    )
    def test_bands_from_stac_asset(self, data, expected):
        asset = pystac.Asset.from_dict(data)
        assert _StacMetadataParser().bands_from_stac_asset(asset=asset).band_names() == expected


def test_proj_code_to_epsg():
    assert _proj_code_to_epsg("EPSG:32631") == 32631
    assert _proj_code_to_epsg("EPSG:4326!") is None
    assert _proj_code_to_epsg("EPSG:onetwothree") is None
    assert _proj_code_to_epsg("IAU_2015:30100") is None
    assert _proj_code_to_epsg(None) is None
    assert _proj_code_to_epsg(1234) is None


class TestProjectionMetadata:
    def test_code_from_epsg(self):
        metadata = ProjectionMetadata(epsg=32631)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631

    def test_epsg_from_code(self):
        metadata = ProjectionMetadata(code="EPSG:32631")
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631

    def test_bbox_from_shape_and_transform(self):
        # https://github.com/soxofaan/projection/blob/22ada42310b58c00d74f68250fd65c8ba6f178b3/examples/assets.json
        metadata = ProjectionMetadata(
            code="EPSG:32659",
            shape=[5558, 9559],
            transform=[0.5, 0, 712710, 0, -0.5, 151406, 0, 0, 1],
        )
        assert metadata.bbox == (712710.0, 148627.0, 717489.5, 151406.0)

    def test_resolution_empty(self):
        pm = ProjectionMetadata()

        with pytest.raises(ValueError, match="Unable to calculate cell size"):
            pm.resolution()

        assert pm.resolution(fail_on_miss=False) is None

    def test_resolution_from_bbox_and_shape(self):
        assert ProjectionMetadata(
            bbox=(100, 200, 300, 500),
            shape=(30, 50),
        ).resolution() == (4, 10)

        assert ProjectionMetadata(
            bbox=(1000, 2000, 5000, 8000),
            shape=(100, 200),
        ).resolution() == (20.0, 60.0)

    def test_resolution_from_transform(self):
        assert ProjectionMetadata(
            transform=[0.5, 0, 712710, 0, -0.5, 151406, 0, 0, 1],
        ).resolution() == (0.5, 0.5)

    def test_resolution_fail_from_item(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item(id="item-no-proj-metadata"))
        metadata = ProjectionMetadata.from_item(item)
        with pytest.raises(ValueError, match="Unable to calculate cell size.*item 'item-no-proj-metadata'"):
            _ = metadata.resolution(fail_on_miss=True)

    def test_resolution_fail_from_asset(self):
        asset = pystac.Asset(href="https://stac.test/asset.tif")
        metadata = ProjectionMetadata.from_asset(asset)
        with pytest.raises(
            ValueError, match="Unable to calculate cell size.*asset with href='https://stac.test/asset.tif'"
        ):
            _ = metadata.resolution(fail_on_miss=True)

        # add item link
        item = pystac.Item.from_dict(StacDummyBuilder.item(id="item-no-proj-metadata"))
        asset.set_owner(item)
        metadata = ProjectionMetadata.from_asset(asset)
        with pytest.raises(
            ValueError,
            match="Unable to calculate cell size.*asset with href='https://stac.test/asset.tif'.*item 'item-no-proj-metadata'",
        ):
            _ = metadata.resolution(fail_on_miss=True)

    def test_from_item_minimal(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        metadata = ProjectionMetadata.from_item(item)
        assert metadata.code is None
        assert metadata.epsg is None
        assert metadata.bbox is None
        assert metadata.shape is None

    def test_from_item_full(self):
        item = pystac.Item.from_dict(
            StacDummyBuilder.item(
                properties={
                    "proj:epsg": 32631,
                    "proj:bbox": [1200, 3400, 5600, 7800],
                    "proj:shape": [100, 200],
                }
            )
        )
        metadata = ProjectionMetadata.from_item(item)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631
        assert metadata.bbox == (1200, 3400, 5600, 7800)
        assert metadata.shape == (100, 200)

    def test_from_asset_basic(self):
        asset = pystac.Asset(
            href="https://stac.test/asset.tif",
            extra_fields={
                "proj:epsg": 32631,
                "proj:bbox": [1200, 3400, 5600, 7800],
                "proj:shape": [100, 200],
            },
        )
        metadata = ProjectionMetadata.from_asset(asset)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631
        assert metadata.bbox == (1200, 3400, 5600, 7800)
        assert metadata.shape == (100, 200)

    def test_from_asset_with_item(self):
        asset = pystac.Asset(
            href="https://stac.test/asset.tif",
            extra_fields={
                "proj:bbox": [1111, 2222, 3333, 4444],
                "proj:shape": [10, 20],
            },
        )
        item = pystac.Item.from_dict(
            StacDummyBuilder.item(
                properties={
                    "proj:epsg": 32631,
                    "proj:shape": [100, 200],
                }
            )
        )
        metadata = ProjectionMetadata.from_asset(asset, item=item)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631
        assert metadata.bbox == (1111, 2222, 3333, 4444)
        assert metadata.shape == (10, 20)

    def test_from_asset_with_owner_item(self):
        asset = pystac.Asset(
            href="https://stac.test/asset.tif",
            extra_fields={
                "proj:bbox": [1111, 2222, 3333, 4444],
                "proj:shape": [10, 20],
            },
        )
        asset.set_owner(
            pystac.Item.from_dict(
                StacDummyBuilder.item(
                    properties={
                        "proj:epsg": 32631,
                        "proj:shape": [100, 200],
                    }
                )
            )
        )
        metadata = ProjectionMetadata.from_asset(asset)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631
        assert metadata.bbox == (1111, 2222, 3333, 4444)
        assert metadata.shape == (10, 20)

    @pytest.mark.parametrize(
        ["extent", "shape", "expected"],
        [
            (
                BoundingBox(12, 26, 37, 52, crs="EPSG:4326"),
                [10, 10],
                BoundingBox(12, 26, 30, 40, crs="EPSG:4326"),
            ),
            (
                BoundingBox(12.345, 26.345, 27.345, 35.345, crs="EPSG:4326"),
                [100, 100],
                BoundingBox(12.2, 26.2, 27.4, 35.4, crs="EPSG:4326").approx(abs=1e-6),
            ),
            (
                BoundingBox(12.3434, 26.3434, 27.3434, 35.3434, crs="EPSG:4326"),
                [1000, 500],
                BoundingBox(12.32, 26.34, 27.36, 35.36, crs="EPSG:4326"),
            ),
        ],
    )
    def test_coverage_for_simple(self, extent, shape, expected):
        metadata = ProjectionMetadata(epsg=4326, bbox=(10, 20, 30, 40), shape=shape)
        coverage = metadata.coverage_for(extent)
        assert coverage == expected

    def test_coverage_for_snap_option(self):
        metadata = ProjectionMetadata(epsg=4326, bbox=(10, 20, 30, 40), shape=[100, 100])
        extent = BoundingBox(12.345, 26.345, 27.345, 35.345, crs="EPSG:4326")

        # Do snapping (default)
        expected = BoundingBox(12.2, 26.2, 27.4, 35.4, crs="EPSG:4326").approx(abs=1e-6)
        assert metadata.coverage_for(extent) == expected
        assert metadata.coverage_for(extent, snap=True) == expected

        # No snapping
        expected = BoundingBox(12.345, 26.345, 27.345, 35.345, crs="EPSG:4326")
        assert metadata.coverage_for(extent, snap=False) == expected

    @pytest.mark.parametrize(
        ["extent", "shape", "expected"],
        [
            (
                # Fully inside UTM tile (10m resolution)
                BoundingBox(5.1, 50.8, 5.2, 50.9, crs="EPSG:4326"),
                [10980, 10980],
                BoundingBox(647660, 5629680, 655030, 5641010, crs="EPSG:32631"),
            ),
            (
                # At corer of UTM tile, partially outside (to be clipped in footprint)
                BoundingBox(6.0, 51.4, 6.2, 51.6, crs="EPSG:4326"),
                [10980, 10980],
                BoundingBox(707760, 5698570, 709800, 5700000, crs="EPSG:32631"),
            ),
            (
                # outside UTM tile
                BoundingBox(20, 51, 20.1, 51.1, crs="EPSG:4326"),
                [10980, 10980],
                None,
            ),
            (
                # Low res asset (200m) and clipping
                BoundingBox(6.0, 51.4, 6.2, 51.6, crs="EPSG:4326"),
                [549, 549],
                BoundingBox(707600, 5698400, 709800, 5700000, crs="EPSG:32631"),
            ),
        ],
    )
    def test_coverage_for_lonlat_in_utm(self, extent, shape, expected):
        # SENTINEL2_L2A-alike projection metadata
        metadata = ProjectionMetadata(epsg=32631, bbox=[600000, 5590200, 709800, 5700000], shape=shape)
        coverage = metadata.coverage_for(extent)
        assert coverage == expected

    def test_hashing_for_set(self):
        metadatas = {
            ProjectionMetadata(epsg=4326, shape=[10, 20], bbox=[1, 2, 3, 4]),
            ProjectionMetadata(code="EPSG:4326", shape=(10, 20), bbox=(1, 2, 3, 4)),
        }
        assert metadatas == {
            ProjectionMetadata(code="EPSG:4326", shape=(10, 20), bbox=(1, 2, 3, 4)),
        }

    def test_hashing_for_dict(self):
        data = {}
        data[ProjectionMetadata(epsg=4326, shape=[10, 20], bbox=[1, 2, 3, 4])] = "red"
        data[ProjectionMetadata(code="EPSG:4326", shape=(10, 20), bbox=(1, 2, 3, 4))] = "green"
        data[ProjectionMetadata(epsg=4326, shape=[100, 100], bbox=[1, 2, 3, 4])] = "blue"
        assert data == {
            ProjectionMetadata(code="EPSG:4326", shape=(10, 20), bbox=(1, 2, 3, 4)): "green",
            ProjectionMetadata(code="EPSG:4326", shape=(100, 100), bbox=(1, 2, 3, 4)): "blue",
        }



def test_get_proj_metadata_minimal():
    asset = pystac.Asset(href="https://example.com/asset.tif")
    item = pystac.Item.from_dict(StacDummyBuilder.item())
    assert get_proj_metadata(asset, item=item) == (None, None, None)


@pytest.mark.parametrize(
    ["item_properties", "asset_extra_fields", "expected"],
    [
        ({}, {}, (None, None, None)),
        (
            # at item level
            {"proj:epsg": 32631, "proj:shape": [12, 34], "proj:bbox": [12, 34, 56, 78]},
            {},
            (32631, (12, 34, 56, 78), (12, 34)),
        ),
        (
            # at asset level
            {},
            {"proj:epsg": 32631, "proj:shape": [12, 34], "proj:bbox": [12, 34, 56, 78]},
            (32631, (12, 34, 56, 78), (12, 34)),
        ),
        (
            # At bands level
            # (https://github.com/Open-EO/openeo-geopyspark-driver/issues/1391, https://github.com/stac-extensions/projection/issues/25)
            {},
            {
                "bands": [
                    {"name": "B04", "proj:epsg": 32631, "proj:shape": [12, 34], "proj:bbox": [12, 34, 56, 78]},
                    {"name": "B02", "proj:epsg": 32631, "proj:shape": [12, 34], "proj:bbox": [12, 34, 56, 78]},
                ]
            },
            (32631, (12, 34, 56, 78), (12, 34)),
        ),
        (
            # Mixed
            {"proj:epsg": 32631},
            {
                "proj:shape": [12, 34],
                "bands": [
                    {"name": "B04", "proj:bbox": [12, 34, 56, 78]},
                ],
            },
            (32631, (12, 34, 56, 78), (12, 34)),
        ),
        (
            # Mixed and precedence
            {"proj:epsg": 32601, "proj:shape": [10, 10]},
            {"proj:code": "EPSG:32602", "proj:shape": [32, 32]},
            (32602, None, (32, 32)),
        ),
    ],
)
def test_get_proj_metadata_from_asset(item_properties, asset_extra_fields, expected):
    """ """
    asset = pystac.Asset(href="https://example.com/asset.tif", extra_fields=asset_extra_fields)
    item = pystac.Item.from_dict(StacDummyBuilder.item(properties=item_properties))
    assert get_proj_metadata(asset, item=item) == expected


class TestFixGdalOrderedTransform:
    """Tests for ProjectionMetadata._fix_gdal_ordered_transform"""

    @pytest.mark.parametrize(
        "transform",
        [
            [0.001, 0.0, 3.0, 0.0, -0.001, 51.0],
            [0.001, 0.0, 0.0, 0.0, -0.001, 51.0],
            [0.001, 0.0, 0.0, 0.0, -0.001, 0.0],
            [0.001, 0.0, 3.0, 0.0, -0.001, 0.0],
        ],
    )
    def test_already_valid_rasterio_order(self, transform):
        """Valid rasterio/affine order should not be changed."""
        assert ProjectionMetadata._fix_gdal_ordered_transform(transform) == transform

    @pytest.mark.parametrize(
        "transform",
        [
            [0.001, 0.0, 3.0, 0.0, -0.001, 51.0],
            [0.001, 0.0, 0.0, 0.0, -0.001, 51.0],
            [0.001, 0.0, 0.0, 0.0, -0.001, 0.0],
            [0.001, 0.0, 3.0, 0.0, -0.001, 0.0],
            [0.0, 0.001, 3.0, -0.001, 0.0, 51.0],
            [0.0, 0.001, 0.0, -0.001, 0.0, 51.0],
            [0.0, 0.001, 0.0, -0.001, 0.0, 0.0],
            [0.0, 0.001, 3.0, -0.001, 0.0, 0.0],
        ],
    )
    def test_valid_rasterio_order_including_yx_transpose(self, transform):
        """Valid rasterio/affine order should not be changed."""
        assert ProjectionMetadata._fix_gdal_ordered_transform(transform, also_check_yx_transposed=True) == transform

    @pytest.mark.parametrize(
        ["given", "expected"],
        [
            ([3, 0.001, 0.0, 51, 0.0, -0.001], [0.001, 0.0, 3, 0.0, -0.001, 51]),
            ([0, 0.001, 0.0, 51, 0.0, -0.001], [0.001, 0.0, 0, 0.0, -0.001, 51]),
            ([0, 0.001, 0.0, 0, 0.0, -0.001], [0.001, 0.0, 0, 0.0, -0.001, 0]),
            ([3, 0.001, 0.0, 0, 0.0, -0.001], [0.001, 0.0, 3, 0.0, -0.001, 0]),
        ],
    )
    def test_fix_gdal_order(self, given, expected):
        """GDAL GetGeoTransform order should be reshuffled to rasterio/affine order."""
        assert ProjectionMetadata._fix_gdal_ordered_transform(given) == expected

    @pytest.mark.parametrize(
        ["given", "expected"],
        [
            ([3, 0.001, 0.0, 51, 0.0, -0.001], [0.001, 0.0, 3, 0.0, -0.001, 51]),
            # TODO Possible to detect for this case as well? Is consistent with rasterio order
            # ([0, 0.001, 0.0, 51, 0.0, -0.001], [0.001, 0.0, 0, 0.0, -0.001, 51]),
            ([0, 0.001, 0.0, 0, 0.0, -0.001], [0.001, 0.0, 0, 0.0, -0.001, 0]),
            ([3, 0.001, 0.0, 0, 0.0, -0.001], [0.001, 0.0, 3, 0.0, -0.001, 0]),
            ([3, 0.0, 0.001, 51, -0.001, 0.0], [0.0, 0.001, 3, -0.001, 0.0, 51]),
            ([0, 0.0, 0.001, 51, -0.001, 0.0], [0.0, 0.001, 0, -0.001, 0.0, 51]),
            ([0, 0.0, 0.001, 0, -0.001, 0.0], [0.0, 0.001, 0, -0.001, 0.0, 0]),
            # TODO Possible to detect for this case as well? Is consistent with rasterio order
            # ([3, 0.0, 0.001, 0, -0.001, 0.0], [0.0, 0.001, 3, -0.001, 0.0, 0]),
        ],
    )
    def test_fix_gdal_order_including_yx_transpose(self, given, expected):
        """GDAL GetGeoTransform order should be reshuffled to rasterio/affine order."""
        assert ProjectionMetadata._fix_gdal_ordered_transform(given, also_check_yx_transposed=True) == expected

    def test_fix_gdal_order_9_elements(self):
        """GDAL order with 9 elements (full 3x3 matrix) should preserve trailing elements."""
        gdal_order = [3.0, 0.00025, 0.0, 51.0, 0.0, -0.00025, 0.0, 0.0, 1.0]
        expected = [0.00025, 0.0, 3.0, 0.0, -0.00025, 51.0, 0.0, 0.0, 1.0]
        assert ProjectionMetadata._fix_gdal_ordered_transform(gdal_order) == expected

    def test_short_transform_unchanged(self):
        """Transform with fewer than 6 elements should not be changed."""
        transform = [1.0, 2.0]
        assert ProjectionMetadata._fix_gdal_ordered_transform(transform) == transform

    def test_from_asset_with_fix_flag(self):
        """from_asset with fix_proj_transform=True should fix GDAL-ordered transforms."""
        asset = pystac.Asset(
            href="https://stac.test/asset.tif",
            extra_fields={
                "proj:epsg": 4326,
                "proj:shape": [4000, 4000],
                "proj:transform": [3.0, 0.00025, 0.0, 51.0, 0.0, -0.00025],
            },
        )
        metadata = ProjectionMetadata.from_asset(asset, fix_proj_transform=True)
        assert metadata.bbox == pytest.approx((3.0, 50.0, 4.0, 51.0))

    def test_from_asset_without_fix_flag(self):
        """from_asset without fix_proj_transform should not fix GDAL-ordered transforms."""
        asset = pystac.Asset(
            href="https://stac.test/asset.tif",
            extra_fields={
                "proj:epsg": 4326,
                "proj:shape": [4000, 4000],
                "proj:transform": [3.0, 0.00025, 0.0, 51.0, 0.0, -0.00025],
            },
        )
        metadata = ProjectionMetadata.from_asset(asset, fix_proj_transform=False)
        # Without fix, bbox will be computed from the wrong transform
        assert metadata.bbox != pytest.approx((3.0, 50.0, 4.0, 51.0))

    @pytest.mark.parametrize(
        ["gdal_transform", "expected_rasterio"],
        [
            (
                # sentinel-1-global-mosaics
                [600000, 20, 0, 5700000, 0, -20],
                [20, 0, 600000, 0, -20, 5700000],
            ),
            (
                # dem30
                [4.999791666666667, 0.0004166666666667, 0, 52.00013888888889, 0, -0.0002777777777778],
                [0.0004166666666667, 0, 4.999791666666667, 0, -0.0002777777777778, 52.00013888888889],
            ),
            (
                # dem90
                [4.999375, 0.00125, 0, 52.000416666666666, 0, -0.0008333333333333],
                [0.00125, 0, 4.999375, 0, -0.0008333333333333, 52.000416666666666],
            ),
        ],
    )
    def test_fix_real_world_gdal_transforms(self, gdal_transform, expected_rasterio):
        """Real-world GDAL-ordered transforms should be correctly converted to rasterio/affine order."""
        assert ProjectionMetadata._fix_gdal_ordered_transform(gdal_transform) == expected_rasterio


class TestTemporalExtent:
    def test_as_tuple_empty(self):
        extent = TemporalExtent(None, None)
        assert extent.as_tuple() == (None, None)

    def test_as_tuple(self):
        extent = TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.as_tuple() == (
            datetime.datetime(2025, 3, 4, 11, 11, 11, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 5, 6, 22, 22, 22, tzinfo=datetime.timezone.utc),
        )

    def test_isoformat(self):
        assert TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22").isoformat() == (
            "2025-03-04T11:11:11+00:00",
            "2025-05-06T22:22:22+00:00",
        )
        assert TemporalExtent(None, "2025-05-06").isoformat() == (None, "2025-05-06T00:00:00+00:00")
        assert TemporalExtent("2025-05-06", None).isoformat() == ("2025-05-06T00:00:00+00:00", None)
        assert TemporalExtent(None, None).isoformat() == (None, None)

    def test_intersects_empty(self):
        extent = TemporalExtent(None, None)
        assert extent.intersects("1789-07-14") == True
        assert extent.intersects(nominal="1789-07-14") == True
        assert extent.intersects(start_datetime="1914-07-28", end_datetime="1918-11-11") == True
        assert extent.intersects(nominal="2025-07-24") == True
        assert extent.intersects(nominal=None) == True

    def test_intersects_nominal_basic(self):
        extent = TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(nominal="2022-10-11") == False
        assert extent.intersects(nominal="2025-03-03T12:13:14") == False
        assert extent.intersects(nominal="2025-03-05T05:05:05") == True
        assert extent.intersects(nominal="2025-07-07T07:07:07") == False

        assert extent.intersects(nominal=datetime.date(2025, 4, 10)) == True
        assert extent.intersects(nominal=datetime.datetime(2025, 4, 10, 12)) == True

        assert extent.intersects(nominal=None) == True

    def test_intersects_nominal_edges(self):
        extent = TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(nominal="2025-03-04T11:11:10") == False
        assert extent.intersects(nominal="2025-03-04T11:11:11") == True
        assert extent.intersects(nominal="2025-03-05T05:05:05") == True
        assert extent.intersects(nominal="2025-05-06T22:22:21") == True
        assert extent.intersects(nominal="2025-05-06T22:22:22") == False

    def test_intersects_nominal_timezones(self):
        extent = TemporalExtent("2025-03-04T11:11:11Z", "2025-05-06T22:22:22-03")
        assert extent.intersects(nominal="2025-03-04T11:11:10") == False
        assert extent.intersects(nominal="2025-03-04T11:11:10-02") == True
        assert extent.intersects(nominal="2025-03-04T11:11:10-04:00") == True
        assert extent.intersects(nominal="2025-03-04T13:11:11") == True
        assert extent.intersects(nominal="2025-03-04T13:11:11+02") == True
        assert extent.intersects(nominal="2025-03-04T13:11:10+02") == False

        assert extent.intersects(nominal="2025-05-06T22:22:22") == True
        assert extent.intersects(nominal="2025-05-06T22:22:22-02") == True
        assert extent.intersects(nominal="2025-05-06T22:22:22-03") == False
        assert extent.intersects(nominal="2025-05-07T01:22:21Z") == True
        assert extent.intersects(nominal="2025-05-07T01:22:22Z") == False

    def test_intersects_nominal_half_open(self):
        extent = TemporalExtent(None, "2025-05-06")
        assert extent.intersects(nominal="1789-07-14") == True
        assert extent.intersects(nominal="2025-05-05") == True
        assert extent.intersects(nominal="2025-05-06") == False
        assert extent.intersects(nominal="2025-11-11") == False
        assert extent.intersects(nominal=None) == True

        extent = TemporalExtent("2025-05-06", None)
        assert extent.intersects(nominal="2025-05-05") == False
        assert extent.intersects(nominal="2025-05-06") == True
        assert extent.intersects(nominal="2099-11-11") == True
        assert extent.intersects(nominal=None) == True

    def test_intersects_start_end_basic(self):
        extent = TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(start_datetime="2022-02-02", end_datetime="2022-02-03") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-04-04") == True
        assert extent.intersects(start_datetime="2025-03-10", end_datetime="2025-04-04") == True
        assert extent.intersects(start_datetime="2025-03-10", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-06-10", end_datetime="2025-08-08") == False

        # Half-open intervals
        assert extent.intersects(start_datetime=None, end_datetime=None) == True
        assert extent.intersects(start_datetime="2022-02-02", end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-10-02", end_datetime=None) == False
        assert extent.intersects(start_datetime=None, end_datetime="2025-01-01") == False
        assert extent.intersects(start_datetime=None, end_datetime="2025-04-04") == True

    def test_intersects_start_end_edges(self):
        extent = TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T11:11:10") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T11:11:11") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T11:11:12") == True

        assert extent.intersects(start_datetime="2025-05-06T22:22:21", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-05-06T22:22:22", end_datetime="2025-08-08") == False
        assert extent.intersects(start_datetime="2025-05-06T22:22:23", end_datetime="2025-08-08") == False

    def test_intersects_start_end_timezones(self):
        extent = TemporalExtent("2025-03-04T11:11:11Z", "2025-05-06T22:22:22-03")
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T12:12:12") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T12:12:12Z") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T12:12:12+06") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T10:10:10") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T10:10:10-03") == True

    def test_intersects_start_end_half_open(self):
        extent = TemporalExtent(None, "2025-05-06")
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-05-05") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-06-06", end_datetime="2025-08-08") == False
        assert extent.intersects(start_datetime=None, end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-06-06", end_datetime=None) == False
        assert extent.intersects(start_datetime=None, end_datetime="2025-05-05") == True

        extent = TemporalExtent("2025-05-06", None)
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-05-05") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-06-06", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime=None, end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime=None) == True
        assert extent.intersects(start_datetime=None, end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime=None, end_datetime="2025-02-02") == False

    def test_intersects_nominal_vs_start_end(self):
        """https://github.com/Open-EO/openeo-geopyspark-driver/issues/1293"""
        extent = TemporalExtent("2024-02-01", "2024-02-10")
        assert extent.intersects(nominal="2024-01-01", start_datetime="2024-01-01", end_datetime="2024-12-31") == True

    def test_intersects_interval(self):
        extent = TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects_interval(["2022-02-02", "2022-02-03"]) == False
        assert extent.intersects_interval(["2025-02-02", "2025-04-04"]) == True
        assert extent.intersects_interval(["2025-03-10", "2025-04-04"]) == True
        assert extent.intersects_interval(["2025-03-10", "2025-08-08"]) == True
        assert extent.intersects_interval(["2025-06-10", "2025-08-08"]) == False
        assert extent.intersects_interval([None, None]) == True
        assert extent.intersects_interval(["2022-02-02", None]) == True
        assert extent.intersects_interval(["2025-10-02", None]) == False
        assert extent.intersects_interval([None, "2025-01-01"]) == False
        assert extent.intersects_interval([None, "2025-04-04"]) == True

    def test_as_cache_key(self):
        extent1 = TemporalExtent("2024-02-01", "2024-02-10")
        extent2 = TemporalExtent("2024-02-01", "2024-02-10")
        extent3 = TemporalExtent(None, "2024-02-10")
        extent4 = TemporalExtent(None, "2024-02-10")
        extent5 = TemporalExtent("2024-02-10", None)

        cache = {extent1: 1, extent3: 3}
        assert cache[extent2] == 1
        assert cache[extent4] == 3
        assert extent5 not in cache

    @pytest.mark.parametrize(
        ["given", "expected"],
        [
            (
                # Legacy (invalid) usage pattern with same day
                ("2026-03-04", "2026-03-04"),
                (
                    datetime.datetime(2026, 3, 4, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2026, 3, 4, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc),
                ),
            ),
            (
                ("2026-03-04", "2026-03-07"),
                (
                    datetime.datetime(2026, 3, 4, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2026, 3, 6, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc),
                ),
            ),
            (
                (None, "2026-03-07"),
                (None, datetime.datetime(2026, 3, 6, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc)),
            ),
            (
                ("2026-03-04", None),
                (datetime.datetime(2026, 3, 4, tzinfo=datetime.timezone.utc), None),
            ),
            (
                # Given to second precision (and timezone)
                ("2026-03-04T12:34:56+00:00", "2026-03-07T11:22:33Z"),
                (
                    datetime.datetime(2026, 3, 4, 12, 34, 56, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2026, 3, 7, 11, 22, 32, 999000, tzinfo=datetime.timezone.utc),
                ),
            ),
        ],
    )
    def test_from_load_param_extent(self, given, expected):
        extent = TemporalExtent.from_load_param_extent(given)
        assert extent.as_tuple() == expected


class TestSpatialExtent:
    def test_as_bbox_empty(self):
        extent = _SpatialExtent(bbox=None)
        assert extent.as_bbox() is None
        assert extent.as_bbox(crs="EPSG:32631") is None

    def test_as_bbox(self):
        extent = _SpatialExtent(bbox=BoundingBox(west=3, south=51, east=4, north=52, crs=4326))
        assert extent.as_bbox() == BoundingBox(west=3, south=51, east=4, north=52, crs=4326)
        assert extent.as_bbox(crs="EPSG:32631") == BoundingBox(
            west=500000, south=5649824, east=570168, north=5761510, crs="EPSG:32631"
        ).approx(abs=1)

    def test_intersects_empty(self):
        extent = _SpatialExtent(bbox=None)
        assert extent.intersects(None) is True
        assert extent.intersects((1, 2, 3, 4)) == True

    def test_intersects_basic(self):
        extent = _SpatialExtent(bbox=BoundingBox(west=3, south=51, east=4, north=52, crs=4326))
        assert extent.intersects((1, 2, 3, 4)) == False
        assert extent.intersects((2, 50, 3.1, 51.1)) == True
        assert extent.intersects((3.3, 51.1, 3.5, 51.5)) == True
        assert extent.intersects((3.9, 51.9, 4.4, 52.2)) == True
        assert extent.intersects((5, 51.1, 6, 52.2)) == False

    def test_intersects_antimeridian(self):
        # Extent across antimeridian:
        extent = _SpatialExtent(bbox=BoundingBox(west=179, south=51, east=-179, north=52, crs=4326))

        assert extent.intersects((1, 50, 3, 52)) == False
        assert extent.intersects((1, 51.1, 3, 51.5)) == False

        # Non-crossing bboxes west from antimeridian
        assert extent.intersects((178, 51.1, 178.9, 51.5)) == False
        assert extent.intersects((179.1, 51.1, 179.5, 51.5)) == True
        assert extent.intersects((178, 50, 179.5, 51.5)) == True
        assert extent.intersects((178, 51.5, 179.5, 53)) == True

        # Non-crossing bboxes east from antimeridian
        assert extent.intersects((-178.9, 51.1, -178, 51.5)) == False
        assert extent.intersects((-179.5, 51.1, -179.1, 51.5)) == True
        assert extent.intersects((-179.5, 50, 178, 51.5)) == True
        assert extent.intersects((-179.5, 51.5, 178, 53)) == True

        # Bboxes crossing the antimeridian
        assert extent.intersects((178, 50, -178, 50.5)) == False
        assert extent.intersects((179.5, 50, -179.5, 50.5)) == False
        assert extent.intersects((178, 50, -178, 51.5)) == True
        assert extent.intersects((179.1, 50, -179.1, 51.5)) == True
        assert extent.intersects((178, 51.1, -178, 51.5)) == True
        assert extent.intersects((179.1, 51.1, -179.1, 51.5)) == True

    def test_intersects_bounding_box(self):
        extent = _SpatialExtent(bbox=BoundingBox(west=3, south=51, east=4, north=52, crs=4326))
        assert extent.intersects(BoundingBox(1, 2, 3, 4, crs=4326)) == False
        assert extent.intersects(BoundingBox(2, 50, 3.1, 51.1, crs=4326)) == True
        assert extent.intersects(BoundingBox(3.3, 51.1, 3.5, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(3.9, 51.9, 4.4, 52.2, crs=4326)) == True
        assert extent.intersects(BoundingBox(5, 51.1, 6, 52.2, crs=4326)) == False

        # Test with different CRS (should be reprojected internally)
        assert extent.intersects(BoundingBox(429_000, 5_537_000, 507_000, 5_660_000, crs=32631)) == True
        assert extent.intersects(BoundingBox(429_000, 5_537_000, 507_000, 5_660_000, crs=32627)) == False

    def test_intersects_bounding_box_antimeridian(self):
        # Extent across antimeridian:
        extent = _SpatialExtent(bbox=BoundingBox(west=179, south=51, east=-179, north=52, crs=4326))

        assert extent.intersects(BoundingBox(1, 50, 3, 52, crs=4326)) == False
        assert extent.intersects(BoundingBox(1, 51.1, 3, 51.5, crs=4326)) == False

        # Non-crossing bboxes west from antimeridian
        assert extent.intersects(BoundingBox(178, 51.1, 178.9, 51.5, crs=4326)) == False
        assert extent.intersects(BoundingBox(179.1, 51.1, 179.5, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(178, 50, 179.5, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(178, 51.5, 179.5, 53, crs=4326)) == True

        # Non-crossing bboxes east from antimeridian
        assert extent.intersects(BoundingBox(-178.9, 51.1, -178, 51.5, crs=4326)) == False
        assert extent.intersects(BoundingBox(-179.5, 51.1, -179.1, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(-179.5, 50, 178, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(-179.5, 51.5, 178, 53, crs=4326)) == True

        # Bboxes crossing the antimeridian
        assert extent.intersects(BoundingBox(178, 50, -178, 50.5, crs=4326)) == False
        assert extent.intersects(BoundingBox(179.5, 50, -179.5, 50.5, crs=4326)) == False
        assert extent.intersects(BoundingBox(178, 50, -178, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(179.1, 50, -179.1, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(178, 51.1, -178, 51.5, crs=4326)) == True
        assert extent.intersects(BoundingBox(179.1, 51.1, -179.1, 51.5, crs=4326)) == True

        # Different CRS
        assert extent.intersects(BoundingBox(250_000, 5_655_000, 350_000, 5_765_000, crs=32601)) == True
        assert extent.intersects(BoundingBox(250_000, 5_655_000, 350_000, 5_765_000, crs=32602)) == False
        assert extent.intersects(BoundingBox(650_000, 5_655_000, 750_000, 5_765_000, crs=32631)) == False
        assert extent.intersects(BoundingBox(650_000, 5_655_000, 750_000, 5_765_000, crs=32659)) == False
        assert extent.intersects(BoundingBox(650_000, 5_655_000, 750_000, 5_765_000, crs=32660)) == True

    def _diamond(self, x: float, y: float, r: float) -> shapely.geometry.Polygon:
        """Diamond shape around (x, y) with radius r"""
        return shapely.geometry.Polygon([(x, y - r), (x + r, y), (x, y + r), (x - r, y), (x, y - r)])

    def _diamonds(self, xyr: List[Tuple[float, float, float]]) -> shapely.geometry.MultiPolygon:
        """Multiple diamonds"""
        return shapely.geometry.MultiPolygon([self._diamond(x=x, y=y, r=r) for (x, y, r) in xyr])

    def test_intersect_shapely(self):
        extent = _SpatialExtent(bbox=BoundingBox(west=3, south=51, east=4, north=52, crs=4326))

        # Basic polygon handling
        assert extent.intersects(self._diamond(3.5, 51.5, r=0.4)) == True
        assert extent.intersects(self._diamond(2.5, 51.5, r=0.4)) == False
        assert extent.intersects(self._diamond(2.5, 51.5, r=0.6)) == True
        assert extent.intersects(self._diamond(0, 50, r=1)) == False
        assert extent.intersects(self._diamond(0, 50, r=4)) == True

        # Basic Multipolygon handling
        assert extent.intersects(self._diamonds([(2.5, 51.5, 0.4), (3.5, 50.5, 0.4)])) == False
        assert extent.intersects(self._diamonds([(2.5, 51.5, 0.6), (3.5, 50.5, 0.6)])) == True

    def test_intersects_shapely_antimeridian(self):
        # Extent across antimeridian:
        extent = _SpatialExtent(bbox=BoundingBox(west=179, south=51, east=-179, north=52, crs=4326))

        assert extent.intersects(self._diamond(2, 50, r=1)) == False

        # Non-crossing shapes west from antimeridian
        assert extent.intersects(self._diamond(178.5, 51.1, r=0.4)) == False
        assert extent.intersects(self._diamond(178.5, 51.1, r=0.6)) == True
        assert extent.intersects(self._diamond(179.5, 51.1, r=0.2)) == True
        assert extent.intersects(self._diamond(179.5, 50.1, r=0.2)) == False

        # Non-crossing bboxes east from antimeridian
        assert extent.intersects(self._diamond(-178.5, 51.1, r=0.4)) == False
        assert extent.intersects(self._diamond(-178.5, 51.1, r=0.6)) == True
        assert extent.intersects(self._diamond(-179.5, 51.1, r=0.2)) == True
        assert extent.intersects(self._diamond(-179.5, 50.1, r=0.2)) == False

        # Antimeridian-split multipolygons
        assert extent.intersects(self._diamonds([(178.5, 51.1, 0.4), (-178.5, 51.1, 0.4)])) == False
        assert extent.intersects(self._diamonds([(178.5, 51.1, 0.6), (-178.5, 51.1, 0.2)])) == True
        assert extent.intersects(self._diamonds([(178.5, 51.1, 0.2), (-178.5, 51.1, 0.6)])) == True
        assert extent.intersects(self._diamonds([(179.5, 51.1, 0.2), (-179.5, 51.1, 0.2)])) == True
        assert extent.intersects(self._diamonds([(179.5, 50.1, 0.2), (-179.5, 50.1, 0.2)])) == False

    def test_as_cache_key(self):
        extent1 = _SpatialExtent(bbox=None)
        extent2 = _SpatialExtent(bbox=None)
        extent3 = _SpatialExtent(bbox=BoundingBox(west=1, south=2, east=3, north=4, crs=4326))
        extent4 = _SpatialExtent(bbox=BoundingBox(west=1, south=2, east=3, north=4, crs=4326))
        extent5 = _SpatialExtent(bbox=BoundingBox(west=10, south=20, east=30, north=40, crs=4326))

        cache = {extent1: 1, extent3: 3}
        assert cache[extent2] == 1
        assert cache[extent4] == 3
        assert extent5 not in cache


class TestSpatialFilteringGeometries:
    def test_empty(self):
        sfg = SpatialFilteringGeometries(geometries=None)
        assert sfg.get_simplified_geojson() is None

    def test_simple_box_geoseries(self):
        geometries = geopandas.GeoSeries([shapely.geometry.box(1, 2, 3, 4)])
        sfg = SpatialFilteringGeometries(geometries=geometries)
        simplified = sfg.get_simplified_geojson()
        simplified = json.loads(simplified)
        assert simplified == {
            "type": "Polygon",
            "coordinates": [dirty_equals.IsList([1, 2], [1, 4], [3, 4], [3, 2], length=5, check_order=False)],
        }

    def test_simple_box_vector_cube(self):
        gdf = geopandas.GeoDataFrame(geometry=[shapely.geometry.box(1, 2, 3, 4)])
        geometries = DriverVectorCube(geometries=gdf)
        sfg = SpatialFilteringGeometries(geometries=geometries)
        simplified = sfg.get_simplified_geojson()
        simplified = json.loads(simplified)
        assert simplified == {
            "type": "Polygon",
            "coordinates": [dirty_equals.IsList([1, 2], [1, 4], [3, 4], [3, 2], length=5, check_order=False)],
        }

    @pytest.mark.parametrize(
        "gdf",
        [
            geopandas.GeoDataFrame(geometry=[shapely.geometry.Point(3.333333, 50.505050)]),
            geopandas.GeoDataFrame(
                geometry=[
                    shapely.geometry.box(1, 2, 3, 4),
                    shapely.geometry.Point(3.333333, 50.505050),
                ]
            ),
        ],
    )
    def test_unsupported_geometry_types(self, gdf):
        sfg = SpatialFilteringGeometries(geometries=gdf)
        assert sfg.get_simplified_geojson() is None


class TestSpatioTemporalExtent:
    @pytest.mark.parametrize(
        ["bbox", "properties", "expected"],
        [
            (
                [20, 34, 26, 40],
                {
                    "datetime": "2024-01-01T00:00:00Z",
                    "start_datetime": "2024-01-01T00:00:00Z",
                    "end_datetime": "2024-12-31T23:59:59Z",
                },
                True,
            ),
            (
                [20, 34, 26, 40],
                {"datetime": "2024-01-01T00:00:00Z"},
                False,
            ),
            (
                [20, 34, 26, 40],
                {"datetime": "2024-02-02T00:00:00Z"},
                True,
            ),
            (
                [60, 34, 66, 40],
                {
                    "datetime": "2024-01-01T00:00:00Z",
                    "start_datetime": "2024-01-01T00:00:00Z",
                    "end_datetime": "2024-12-31T23:59:59Z",
                },
                False,
            ),
        ],
    )
    def test_item_intersects(self, bbox, properties, expected):
        extent = SpatioTemporalExtent(
            bbox=BoundingBox(west=21, south=35, east=25, north=38, crs=4326),
            from_date="2024-02-01",
            to_date="2024-02-10",
        )
        item = pystac.Item.from_dict(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": "2024_GRC_V00",
                "bbox": bbox,
                "properties": properties,
            }
        )
        assert extent.item_intersects(item) == expected

    @pytest.mark.parametrize(
        ["bboxes", "intervals", "expected"],
        [
            # Single bbox/interval cases
            ([[20, 34, 26, 40]], [["2024-01-01", "2024-12-31"]], True),
            ([[10, 34, 16, 40]], [["2024-01-01", "2024-12-31"]], False),
            ([[20, 34, 26, 40]], [["2025-01-01", "2025-12-31"]], False),
            # Multiple bboxes: first "overall" one should be ignored
            ([[20, 30, 30, 40], [23, 34, 26, 39]], [["2024-01-01", "2024-12-31"]], True),
            ([[20, 30, 30, 40], [21, 31, 22, 32]], [["2024-01-01", "2024-12-31"]], False),
            ([[20, 30, 30, 40], [21, 31, 22, 32], [23, 34, 26, 39]], [["2024-01-01", "2024-12-31"]], True),
            ([[20, 30, 30, 40], [21, 31, 22, 32], [25, 31, 26, 32]], [["2024-01-01", "2024-12-31"]], False),
            # Multiple intervals: first "overall" one should be ignored
            ([[20, 34, 26, 40]], [["2024-01-01", "2024-12-31"], ["2024-02-01", "2024-02-20"]], True),
            ([[20, 34, 26, 40]], [["2024-01-01", "2024-12-31"], ["2024-10-01", "2024-10-20"]], False),
        ],
    )
    def test_collection_intersects_simple(self, bboxes, intervals, expected):
        extent = SpatioTemporalExtent(
            bbox=BoundingBox(west=21, south=35, east=25, north=38, crs=4326),
            from_date="2024-02-01",
            to_date="2024-02-10",
        )
        collection = pystac.Collection(
            id="c123",
            description="C123",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=bboxes),
                temporal=pystac.TemporalExtent.from_dict({"interval": intervals}),
            ),
        )
        assert extent.collection_intersects(collection) == expected

    def test_as_cache_key(self):
        bbox1 = BoundingBox(west=1, south=2, east=3, north=4, crs=4326)
        extent1 = SpatioTemporalExtent(bbox=bbox1, from_date="2024-02-01")
        extent2 = SpatioTemporalExtent(bbox=bbox1, from_date="2024-02-01")
        extent3 = SpatioTemporalExtent(bbox=None, from_date="2024-02-01", to_date="2024-02-10")
        extent4 = SpatioTemporalExtent(bbox=None, from_date="2024-02-01", to_date="2024-02-10")
        extent5 = SpatioTemporalExtent(bbox=bbox1, from_date="2024-02-01", to_date="2024-02-10")

        cache = {extent1: 1, extent3: 3}
        assert cache[extent2] == 1
        assert cache[extent4] == 3
        assert extent5 not in cache


@pytest.mark.parametrize(
    ["load_params", "expected"],
    [
        (
            LoadParameters(),
            (None, (None, None)),
        ),
        (
            LoadParameters(
                spatial_extent={"west": 10, "south": 20, "east": 30, "north": 40},
                temporal_extent=("2025-09-01", "2025-10-11"),
            ),
            (
                BoundingBox(west=10, south=20, east=30, north=40, crs=4326),
                (
                    datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2025, 10, 10, 23, 59, 59, microsecond=999000, tzinfo=datetime.timezone.utc),
                ),
            ),
        ),
        (
            LoadParameters(temporal_extent=("2025-09-01", "2025-09-01")),
            (
                None,
                (
                    datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc),
                    datetime.datetime(2025, 9, 1, 23, 59, 59, microsecond=999999, tzinfo=datetime.timezone.utc),
                ),
            ),
        ),
        (
            LoadParameters(temporal_extent=(None, "2025-09-05")),
            (
                None,
                (
                    None,
                    datetime.datetime(2025, 9, 4, 23, 59, 59, microsecond=999000, tzinfo=datetime.timezone.utc),
                ),
            ),
        ),
        (
            LoadParameters(temporal_extent=("2025-09-01", None)),
            (
                None,
                (
                    datetime.datetime(2025, 9, 1, tzinfo=datetime.timezone.utc),
                    None,
                ),
            ),
        ),
    ],
)
def test_spatiotemporal_extent_from_load_params(load_params, expected, time_machine):
    time_machine.move_to("2024-01-02T03:04:05Z")
    extent = spatiotemporal_extent_from_load_params(load_params.spatial_extent, load_params.temporal_extent)
    assert (extent.spatial_extent.as_bbox(), extent.temporal_extent.as_tuple()) == expected


class TestPrepareContext:
    def _define_collection_s2_with_granule_metadata(
        self, dummy_server: DummyStacApiServer, collection_id: str = "s2-with-granule_metadata"
    ):
        dummy_server.define_collection(
            collection_id,
            extent={
                "spatial": {"bbox": [[3, 50, 5, 51]]},
                "temporal": {"interval": [["2024-02-01", "2024-12-01"]]},
            },
            summaries={
                "bands": [
                    {"name": "B02", "common_name": "blue"},
                    {"name": "B03", "common_name": "green"},
                ]
            },
        )
        dummy_server.define_item(
            collection_id=collection_id,
            item_id=f"item-123",
            datetime=f"2024-10-20T12:00:00Z",
            bbox=[3, 50, 5, 51],
            assets={
                "B02_10m": {
                    "href": "https://stac.test/B02_10m.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "gsd": 10,
                    "proj:code": 4326,
                    "proj:bbox": [5, 51, 6, 52],
                    "proj:shape": [1000, 1000],
                },
                "B02_20m": {
                    "href": "https://stac.test/B02_20m.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                    "gsd": 20,
                    "proj:code": 4326,
                    "proj:bbox": [5, 51, 6, 52],
                    "proj:shape": [500, 500],
                },
                "B03_10m": {
                    "href": "https://stac.test/B03_10m.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "B03"}],
                    "gsd": 10,
                    "proj:code": 4326,
                    "proj:bbox": [5, 51, 6, 52],
                    "proj:shape": [1000, 1000],
                },
                "granule_metadata": {
                    "href": "https://stac.test/MTD_TL.xml",
                    "type": "application/xml",
                    "roles": ["metadata"],
                    "title": "MTD_TL.xml",
                },
            },
        )

    def test_prepare_context_basic(self, dummy_stac_api_server):
        with dummy_stac_api_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/collection-123",
                load_params=LoadParameters(),
                env=EvalEnv(),
            )

        dumper = OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client) == [
            {
                "id": "item-1",
                "links": [
                    {
                        "title": "asset-1",
                        "href": dirty_equals.IsStr(regex=".*/asset-1.tiff"),
                        "bandNames": ["B02"],
                    }
                ],
            },
            {
                "id": "item-2",
                "links": [
                    {
                        "title": "asset-2",
                        "href": dirty_equals.IsStr(regex=".*/asset-2.tiff"),
                        "bandNames": ["B02"],
                    }
                ],
            },
            {
                "id": "item-3",
                "links": [
                    {
                        "title": "asset-2",
                        "href": dirty_equals.IsStr(regex=".*/asset-3.tiff"),
                        "bandNames": ["B02"],
                    }
                ],
            },
        ]
        assert list(context.pyramid_factory.openSearchLinkTitles()) == ["B02"]
        assert context.metadata.band_names == ["B02"]
        assert context.metadata.temporal_extent == ("2024-05-01T00:00:00+00:00", "2024-07-03T00:00:00+00:00")
        assert context.metadata.spatial_extent is None

    @pytest.mark.parametrize(
        [
            "load_params_bands",
            "normalized_band_selection",
            "expected_links",
            "expected_metadata_band_names",
            "expected_link_titles",
        ],
        [
            (
                None,
                None,
                [
                    {"title": "B02_10m", "href": "https://stac.test/B02_10m.tif", "bandNames": ["B02"]},
                    {"title": "B03_10m", "href": "https://stac.test/B03_10m.tif", "bandNames": ["B03"]},
                    {
                        "title": "granule_metadata",
                        "href": "https://stac.test/MTD_TL.xml",
                        "bandNames": [
                            "granule_metadata##0",
                            "granule_metadata##1",
                            "granule_metadata##2",
                            "granule_metadata##3",
                        ],
                    },
                ],
                ["B02", "B03", "sunAzimuthAngles", "sunZenithAngles", "viewAzimuthMean", "viewZenithMean"],
                [
                    "B02",
                    "B03",
                    "granule_metadata##0",
                    "granule_metadata##1",
                    "granule_metadata##2",
                    "granule_metadata##3",
                ],
            ),
            (
                ["B02"],
                None,
                [{"title": "B02_10m", "href": "https://stac.test/B02_10m.tif", "bandNames": ["B02"]}],
                ["B02"],
                ["B02"],
            ),
            (
                ["B02_20m"],
                None,
                [{"title": "B02_20m", "href": "https://stac.test/B02_20m.tif", "bandNames": ["B02_20m"]}],
                ["B02_20m"],
                ["B02_20m"],
            ),
            (
                ["B02", "sunAzimuthAngles", "viewZenithMean"],
                None,
                [
                    {"title": "B02_10m", "href": "https://stac.test/B02_10m.tif", "bandNames": ["B02"]},
                    {
                        "title": "granule_metadata",
                        "href": "https://stac.test/MTD_TL.xml",
                        "bandNames": [
                            "granule_metadata##0",
                            "granule_metadata##1",
                            "granule_metadata##2",
                            "granule_metadata##3",
                        ],
                    },
                ],
                ["B02", "sunAzimuthAngles", "viewZenithMean"],
                ["B02", "granule_metadata##0", "granule_metadata##3"],
            ),
            (
                ["B02", "SAA", "VZA"],
                ["B02", "sunAzimuthAngles", "viewZenithMean"],
                [
                    {"title": "B02_10m", "href": "https://stac.test/B02_10m.tif", "bandNames": ["B02"]},
                    {
                        "title": "granule_metadata",
                        "href": "https://stac.test/MTD_TL.xml",
                        "bandNames": [
                            "granule_metadata##0",
                            "granule_metadata##1",
                            "granule_metadata##2",
                            "granule_metadata##3",
                        ],
                    },
                ],
                ["B02", "SAA", "VZA"],
                ["B02", "granule_metadata##0", "granule_metadata##3"],
            ),
        ],
    )
    def test_prepare_context_sentinel2_with_azimuth_and_zenith_bands(
        self,
        load_params_bands,
        normalized_band_selection,
        expected_links,
        expected_metadata_band_names,
        expected_link_titles,
    ):
        dummy_server = DummyStacApiServer()
        collection_id = "s2-with-granule_metadata"
        self._define_collection_s2_with_granule_metadata(dummy_server, collection_id=collection_id)
        layercatalog_feature_flags = {
            "granule_metadata_band_map": {
                "sunAzimuthAngles": "granule_metadata##0",
                "sunZenithAngles": "granule_metadata##1",
                "viewAzimuthMean": "granule_metadata##2",
                "viewZenithMean": "granule_metadata##3",
            },
        }
        with dummy_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/{collection_id}",
                load_params=LoadParameters(
                    bands=load_params_bands,
                ),
                normalized_band_selection=normalized_band_selection,
                env=EvalEnv(),
                feature_flags=layercatalog_feature_flags,
            )

        dumper = OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client) == [
            {
                "id": "item-123",
                "links": expected_links,
            }
        ]

        assert context.metadata.band_names == expected_metadata_band_names
        assert list(context.pyramid_factory.openSearchLinkTitles()) == expected_link_titles

    @pytest.mark.parametrize(
        ["asset_id", "user_bands", "normalized_band_selection", "expected_metadata_bands", "expected_link_titles"],
        [
            ("asset-123", None, None, ["SO2CBR"], ["SO2CBR"]),
            ("SO2CBR", ["SO2CBR"], None, ["SO2CBR"], ["SO2CBR"]),
            ("SO2CBR", ["SO2CBR"], ["SO2CBR"], ["SO2CBR"], ["SO2CBR"]),
            ("asset-123", ["SomeAlias"], ["SO2CBR"], ["SomeAlias"], ["SO2CBR"]),
        ],
    )
    def test_prepare_context_so2cbr(
        self, asset_id, user_bands, normalized_band_selection, expected_metadata_bands, expected_link_titles
    ):
        """
        Problem uncovered from S5P SO2CBR use case:
        low res assets + usage of band aliases
        cell size detection should still work,
        and not fallback to stupid 10m resolution assumption
        """
        dummy_server = DummyStacApiServer()
        collection_id = "s5p_l3_so2cbr_ty"
        dummy_server.define_collection(collection_id)
        dummy_server.define_item(
            collection_id=collection_id,
            item_id=f"item-123",
            datetime=f"2023-01-01T00:00:00Z",
            bbox=[-180.0, -87.5, 180.0, 87.5],
            assets={
                asset_id: {
                    "href": "https://stac.test/SO2CBR.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "SO2CBR"}],
                    "proj:code": "EPSG:4326",
                    "proj:bbox": [-180.0, -87.5, 180.0, 87.5],
                    "proj:shape": [1400, 2880],
                },
            },
        )

        with dummy_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/{collection_id}",
                load_params=LoadParameters(bands=user_bands),
                normalized_band_selection=normalized_band_selection,
                env=EvalEnv(),
            )

        cell_size = context.pyramid_factory.maxSpatialResolution()
        assert (cell_size.width(), cell_size.height()) == (0.125, 0.125)
        assert context.metadata.band_names == expected_metadata_bands
        assert list(context.pyramid_factory.openSearchLinkTitles()) == expected_link_titles

    @pytest.mark.parametrize(
        ["requested_temporal_extent", "expected_items", "expected_temporal_extent"],
        [
            (
                (None, None),
                ["item-1", "item-2", "item-3"],
                ("2024-05-01T00:00:00+00:00", "2024-07-03T00:00:00+00:00"),
            ),
            (
                ("2024-03-27", "2024-06-06"),
                ["item-1", "item-2"],
                ("2024-03-27T00:00:00+00:00", "2024-06-05T23:59:59.999000+00:00"),
            ),
            (
                (None, "2024-06-06"),
                ["item-1", "item-2"],
                ("2024-05-01T00:00:00+00:00", "2024-06-05T23:59:59.999000+00:00"),
            ),
            (
                ("2024-05-20", None),
                ["item-2", "item-3"],
                ("2024-05-20T00:00:00+00:00", "2024-07-03T00:00:00+00:00"),
            ),
        ],
    )
    def test_prepare_context_requested_temporal_extent(
        self, dummy_stac_api_server, requested_temporal_extent, expected_items, expected_temporal_extent
    ):
        with dummy_stac_api_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/collection-123",
                load_params=LoadParameters(temporal_extent=requested_temporal_extent),
                env=EvalEnv(),
            )

        dumper = OpenSearchClientDumper()
        assert [i["id"] for i in dumper.dump_opensearch_client_features(context.opensearch_client)] == expected_items
        assert context.metadata.temporal_extent == expected_temporal_extent

    @pytest.mark.parametrize(
        ["spatial_extent", "item2_overrides", "expected_features", "expected_logging"],
        [
            (
                # Requested spatial extent is far far away from antimeridian: skip corrupt item.
                # Item-2: invalid bbox, but valid geometry and proj:bbox
                {"west": 2, "south": 50.0, "east": 5, "north": 55.0},
                {"bbox": [-179, 50, 179, 51]},
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                ],
                dirty_equals.IsStr(regex=r".*Skipping.*item-2.*implausible bbox.*epsg.*32601.*", regex_flags=re.DOTALL),
            ),
            (
                # Requested spatial extent is far far away from antimeridian: skip corrupt item.
                # Item-2: invalid bbox, no proj:bbox, but valid geometry
                {"west": 2, "south": 50.0, "east": 5, "north": 55.0},
                {
                    "bbox": [-179, 50, 179, 51],
                    "properties": {"proj:code": "EPSG:32601"},
                },
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                ],
                dirty_equals.IsStr(regex=r".*Skipping.*item-2.*implausible bbox.*epsg.*32601.*", regex_flags=re.DOTALL),
            ),
            (
                # Requested spatial extent includes antimeridian region: include item with corrupt item.bbox.
                # Item-2: invalid bbox, but valid geometry and proj:bbox
                {"west": -179.9, "south": 50.0, "east": 5, "north": 55.0},
                {"bbox": [-179, 50, 179, 51]},
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                    {"id": "item-2", "bbox": approxify((179.00, 49.94, 180.99, 51.05), abs=0.01)},
                ],
                dirty_equals.IsStr(regex=r".*Detected implausible bbox.*item-2.*epsg.*32601.*", regex_flags=re.DOTALL),
            ),
            (
                # Requested spatial extent includes antimeridian region: include item with corrupt item.bbox.
                # Item-2: invalid bbox, no proj:bbox, but valid geometry
                {"west": -179.9, "south": 50.0, "east": 5, "north": 55.0},
                {
                    "bbox": [-179, 50, 179, 51],
                    "properties": {"proj:code": "EPSG:32601"},
                },
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                    {"id": "item-2", "bbox": None},
                ],
                dirty_equals.IsStr(regex=r".*Detected implausible bbox.*item-2.*epsg.*32601.*", regex_flags=re.DOTALL),
            ),
            (
                # Requested spatial extent includes antimeridian region
                # Item-2: invalid bbox, no proj:bbox, invalid geometry
                # Skip item, but don't break whole flow
                {"west": -179.9, "south": 50.0, "east": 5, "north": 55.0},
                {
                    "bbox": [-179, 50, 179, 51],
                    "geometry": {"type": "garbage"},
                    "properties": {"proj:code": "EPSG:32601"},
                },
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                ],
                dirty_equals.IsStr(regex=r".*Skipping.*item-2.*implausible bbox.*epsg.*32601.*", regex_flags=re.DOTALL),
            ),
            (
                # Happy case: no corruption (valid bbox, valid proj:bbox)
                # Requested spatial extent is far far away from antimeridian: only item-1 should be included
                {"west": 2, "south": 50.0, "east": 5, "north": 55.0},
                {"bbox": [179, 50, -179, 51]},
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                ],
                # Regex with negative look-ahead here: logs should NOT contain "implausible"
                dirty_equals.IsStr(regex=r"^(?!.*implausible).*", regex_flags=re.DOTALL),
            ),
            (
                # Happy case: no corruption (valid bbox, valid proj:bbox)
                # Requested spatial extent includes antimeridian region: item-2 should be included too
                {"west": -179.9, "south": 50.0, "east": 5, "north": 55.0},
                {"bbox": [179, 50, -179, 51]},
                [
                    {"id": "item-1", "bbox": approxify((3.00, 49.99, 4.03, 51.00), abs=0.01)},
                    {"id": "item-2", "bbox": approxify((179.00, 49.94, 180.99, 51.05), abs=0.01)},
                ],
                # Regex with negative look-ahead here: logs should NOT contain "implausible"
                dirty_equals.IsStr(regex=r"^(?!.*implausible).*", regex_flags=re.DOTALL),
            ),
        ],
    )
    def test_prepare_context_handle_corrupt_item_bbox_antimeridian(
        self, dummy_stac_api_server, item2_overrides, spatial_extent, expected_features, expected_logging, caplog
    ):
        """https://github.com/Open-EO/openeo-geopyspark-driver/issues/1592"""
        collection_id = "S1592"
        dummy_stac_api_server.define_collection(collection_id)
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-1",
            datetime="2024-05-01T00:00:00Z",
            bbox=[3, 50, 4, 51],
            properties={
                "proj:code": "EPSG:32631",
                "proj:shape": [100, 100],
                "proj:bbox": [500_000, 5_538_000, 572_000, 5_650_000],
            },
            assets={
                "asset-1": {
                    "href": "https://stac.test/asset-1.tiff",
                    "type": "image/tiff",
                    "roles": ["data"],
                    "bands": [{"name": "asset-1"}],
                }
            },
        )
        assert "bbox" in item2_overrides, "Always specify bbox for clarity of test intent"
        item2_fields = {
            "bbox": [179, 50, -179, 51],
            "properties": {
                "proj:code": "EPSG:32601",
                "proj:shape": [100, 100],
                "proj:bbox": [213_000, 5_540_000, 359_000, 5_657_000],
            },
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    (((-180.0, 49.97), (-178.96, 49.99), (-179.01, 51.04), (-180.0, 51.02), (-180.0, 49.97)),),
                    (((180.0, 51.02), (178.90, 50.99), (178.99, 49.94), (180.0, 49.97), (180.0, 51.025)),),
                ],
            },
            **item2_overrides,
        }
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-2",
            datetime="2024-05-02T00:00:00Z",
            **item2_fields,
            assets={
                "asset-2": {
                    "href": "https://stac.test/asset-2.tiff",
                    "type": "image/tiff",
                    "roles": ["data"],
                    "bands": [{"name": "asset-2"}],
                }
            },
        )

        with dummy_stac_api_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/{collection_id}",
                load_params=LoadParameters(spatial_extent=spatial_extent),
                env=EvalEnv(),
            )

        dumper = OpenSearchClientDumper()
        assert (
            dumper.dump_opensearch_client_features(context.opensearch_client, add_links=False, add_bbox=True)
            == expected_features
        )
        assert caplog.text == expected_logging

    @pytest.mark.parametrize(
        ["proj_bbox", "expected"],
        [
            (
                # Crossing the antimeridian: east bound gets an additional +360 offset
                # to avoid confusion from west > east
                [300_000, 7_590_240, 409_800, 7_700_040],
                approxify((178.137, 68.354, 180.703, 69.394), abs=0.01),
            ),
            (
                # Not crossing the antimeridian
                [500_000, 7_590_240, 609_800, 7_700_040],
                approxify((-177.000, 68.403, -174.205, 69.410), abs=0.01),
            ),
        ],
    )
    def test_prepare_context_anitmeridian_bbox(self, dummy_stac_api_server, proj_bbox, expected):
        """https://github.com/Open-EO/openeo-geopyspark-driver/issues/1594"""
        collection_id = "S1594"
        dummy_stac_api_server.define_collection(collection_id)
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-1",
            datetime="2026-03-09T23:56:09.024000Z",
            bbox=[177.1, 67.2, -178.3, 69.4],
            properties={
                "proj:code": "EPSG:32601",
                "proj:shape": [10980, 10980],
                "proj:bbox": proj_bbox,
            },
            assets={
                "B02_10m": {
                    "href": "https://stac.test/B02_10m.tiff",
                    "type": "image/tiff",
                    "roles": ["data"],
                    "bands": [{"name": "B02"}],
                }
            },
        )

        with dummy_stac_api_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/{collection_id}",
                load_params=LoadParameters(),
                env=EvalEnv(),
            )

        dumper = OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client, add_links=False, add_bbox=True) == [
            {
                "id": "item-1",
                "bbox": expected,
            }
        ]

    @pytest.mark.parametrize(
        ["aggregate_spatial_geometries", "feature_flags", "env_dict", "expected_items"],
        [
            (
                shapely.geometry.box(2.5, 49.5, 3.5, 51.5),
                {},
                {},
                ["item-1", "item-2"],
            ),
            (
                shapely.geometry.Polygon([(6.2, 49.8), (6.5, 51.5), (7, 49), (2.5, 49.5), (6.2, 49.8)]),
                {},
                {},
                ["item-1", "item-3"],
            ),
            (
                shapely.geometry.Polygon([(6.2, 49.8), (6.5, 51.5), (7, 49), (2.5, 49.5), (6.2, 49.8)]),
                {"stac_api_filter_by_geometry": False},
                {},
                ["item-1", "item-2", "item-3"],
            ),
            (
                shapely.geometry.Polygon([(6.2, 49.8), (6.5, 51.5), (7, 49), (2.5, 49.5), (6.2, 49.8)]),
                {},
                {"stac_api_filter_by_geometry": False},
                ["item-1", "item-2", "item-3"],
            ),
            (
                DriverVectorCube(
                    geometries=geopandas.GeoDataFrame(
                        geometry=[
                            shapely.geometry.box(1.5, 48.5, 2.5, 49.5),
                            shapely.geometry.box(6, 50, 7, 52),
                        ]
                    )
                ),
                {},
                {},
                ["item-1", "item-3"],
            ),
        ],
    )
    def test_prepare_context_spatial_filtering_geometries(
        self, dummy_stac_api_server, aggregate_spatial_geometries, expected_items, feature_flags, env_dict
    ):
        with dummy_stac_api_server.serve() as dummy_stac_api:
            context = _prepare_context(
                url=f"{dummy_stac_api}/collections/collection-123",
                load_params=LoadParameters(
                    aggregate_spatial_geometries=aggregate_spatial_geometries,
                ),
                feature_flags=feature_flags,
                env=EvalEnv(env_dict),
            )

        dumper = OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client, add_links=False) == [
            {"id": item_id} for item_id in expected_items
        ]

    @pytest.mark.parametrize(
        ["feature_flags", "item_properties", "asset_properties", "expected_scale_and_offset"],
        [
            (
                {"apply_raster_scale_and_offset": True},
                {},
                {},
                (1.0, 0.0),
            ),
            (
                {"apply_raster_scale_and_offset": True},
                {},
                {"raster:scale": 0.01, "raster:offset": 123},
                (0.01, 123.0),
            ),
            (
                {"apply_raster_scale_and_offset": True},
                {"raster:scale": 0.01, "raster:offset": 123},
                {},
                (0.01, 123.0),
            ),
            (
                {"apply_raster_scale_and_offset": True},
                {"raster:scale": 0.01, "raster:offset": 123},
                {"raster:scale": 0.02, "raster:offset": 456},
                (0.02, 456.0),
            ),
            (
                {"apply_raster_scale_and_offset": True},
                {"raster:offset": 123},
                {"raster:scale": 0.02},
                (0.02, 123.0),
            ),
            (
                {"apply_raster_scale_and_offset": False},
                {"raster:scale": 0.01, "raster:offset": 123},
                {"raster:scale": 0.02, "raster:offset": 456},
                (1, 0),
            ),
        ],
    )
    def test_prepare_context_raster_scale_and_offset(
        self,
        dummy_stac_api_server,
        dummy_stac_api,
        feature_flags,
        item_properties,
        asset_properties,
        expected_scale_and_offset,
    ):
        collection_id = "collection-with-scale-and-offset"
        dummy_stac_api_server.define_collection(id=collection_id)
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-1",
            properties=item_properties,
            assets={
                "asset-1": StacDummyBuilder.asset(
                    href="https://example.com/asset-1.tiff",
                    roles=["data"],
                    proj_code="EPSG:4326",
                    proj_bbox=[2, 49, 3, 50],
                    proj_shape=[32, 1 * 32],
                    bands=[{"name": "B02"}],
                    **asset_properties,
                )
            },
        )
        context = _prepare_context(
            url=f"{dummy_stac_api}/collections/{collection_id}",
            load_params=LoadParameters(),
            env=EvalEnv(),
            feature_flags=feature_flags,
        )

        expected_scale, expected_offset = expected_scale_and_offset
        dumper = OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client, add_pixel_value_scaling=True) == [
            {
                "id": "item-1",
                "links": [
                    {
                        "href": "https://example.com/asset-1.tiff",
                        "title": "asset-1",
                        "pixelValueScale": expected_scale,
                        "pixelValueOffset": expected_offset,
                        "bandNames": ["B02"],
                    }
                ],
            }
        ]


    @pytest.mark.parametrize(
        ["feature_flags", "asset_href", "expected"],
        [
            (
                # Default behavior with full http href:
                {},
                "http://example.com/asset.tiff",
                "http://example.com/asset.tiff",
            ),
            pytest.param(
                # Default behavior on scheme-less absolute pass: resolve as root relative href
                {},
                "/absolute/path/without/scheme.tiff",
                "{dummy_stac_api}/absolute/path/without/scheme.tiff",
                marks=pytest.mark.skipif(
                    condition=ComparableVersion(importlib.metadata.version("pystac")) < "1.14.2",
                    reason="Proper href normalization requires at least pystac 1.14.2 https://github.com/stac-utils/pystac/issues/1597",
                ),
            ),
            (
                # Override: force using raw href as-is
                {"use_raw_asset_href": True},
                "/absolute/path/without/scheme.tiff",
                "/absolute/path/without/scheme.tiff",
            ),
        ],
    )
    def test_prepare_context_use_raw_asset_href(
        self,
        dummy_stac_api_server,
        dummy_stac_api,
        feature_flags: dict,
        asset_href: str,
        expected,
    ):
        collection_id = "collection-with-non-standard-asset-hrefs"
        dummy_stac_api_server.define_collection(id=collection_id)
        dummy_stac_api_server.define_item(
            collection_id=collection_id,
            item_id="item-1",
            assets={
                "asset-1": StacDummyBuilder.asset(
                    href=asset_href,
                    roles=["data"],
                    proj_code="EPSG:4326",
                    proj_bbox=[2, 49, 3, 50],
                    proj_shape=[32, 32],
                    bands=[{"name": "B02"}],
                )
            },
        )
        context = _prepare_context(
            url=f"{dummy_stac_api}/collections/{collection_id}",
            load_params=LoadParameters(),
            env=EvalEnv(),
            feature_flags=feature_flags,
        )

        dumper = OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client) == [
            {
                "id": "item-1",
                "links": [
                    {
                        "href": expected.format(dummy_stac_api=dummy_stac_api),
                        "title": "asset-1",
                        "bandNames": ["B02"],
                    }
                ],
            }
        ]


