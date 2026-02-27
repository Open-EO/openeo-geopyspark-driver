import logging

import datetime
from contextlib import nullcontext
from typing import Iterator, List
from unittest import mock

import dirty_equals
import openeo.metadata
import pystac
import pystac_client
import pytest
import responses
from openeo.testing.stac import StacDummyBuilder
from openeo_driver.backend import BatchJobMetadata, BatchJobs, LoadParameters
from openeo_driver.errors import OpenEOApiException
from openeo_driver.users import User
from openeo_driver.util.date_math import now_utc
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv
from py4j.java_gateway import JavaObject

from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.job_registry import InMemoryJobRegistry
from openeogeotrellis.load_stac import (
    ItemCollection,
    ItemDeduplicator,
    PropertyFilter,
    _get_proj_metadata,
    _is_band_asset,
    _is_supported_raster_mime_type,
    _proj_code_to_epsg,
    _ProjectionMetadata,
    _SpatialExtent,
    _spatiotemporal_extent_from_load_params,
    _SpatioTemporalExtent,
    _StacMetadataParser,
    _supports_item_search,
    _TemporalExtent,
    construct_item_collection,
    extract_own_job_info,
    load_stac,
    _prepare_context,
    AdaptingPropertyFilter,
    _get_apply_sentinel2_reflectance_offset,
    _is_sentinel2_reflectance_asset,
    _ResolutionTracker,
    STAC_API_PER_PAGE_LIMIT_DEFAULT,
)
from openeogeotrellis.testing import DummyStacApiServer, gps_config_overrides
from openeogeotrellis.util.geometry import bbox_to_geojson


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
            """"properties.product_tile" = '31UFS'""".lower()  # https://github.com/jamielennox/requests-mock/issues/264
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

    with pytest.raises(OpenEOApiException, match="There is no data available for the given extents."):
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


@pytest.fixture
def jvm_mock():
    with mock.patch("openeogeotrellis.load_stac.get_jvm") as get_jvm:
        jvm_mock = get_jvm.return_value

        raster_layer = mock.MagicMock()
        jvm_mock.geopyspark.geotrellis.TemporalTiledRasterLayer.return_value = raster_layer
        raster_layer.layerMetadata.return_value = """{
            "crs": "EPSG:4326",
            "cellType": "uint8",
            "bounds": {"minKey": {"col":0, "row":0}, "maxKey": {"col": 1, "row": 1}},
            "extent": {"xmin": 0,"ymin": 0, "xmax": 1,"ymax": 1},
            "layoutDefinition": {
                "extent": {"xmin": 0, "ymin": 0,"xmax": 1,"ymax": 1},
                "tileLayout": {"layoutCols": 1, "layoutRows": 1, "tileCols": 256, "tileRows": 256}
            }
        }"""

        yield jvm_mock


@pytest.mark.parametrize(
    ["band_names", "resolution", "expected_add_links"],
    [
        (
            ["AOT_10m"],
            10.0,
            [(dirty_equals.IsStr(regex=".*_AOT_10m.jp2"), "AOT_10m", 0.0, ["AOT_10m"])],
        ),
        (
            ["B01_60m"],
            60.0,
            [
                (
                    dirty_equals.IsStr(regex=".*_B01_60m.jp2"),
                    "B01_60m",
                    # has "raster:scale": 0.0001 and "raster:offset": -0.1
                    -1000.0,
                    ["B01_60m"],
                )
            ],
        ),
        (
            ["B01"],
            20.0,
            [(dirty_equals.IsStr(regex=".*_B01_20m.jp2"), "B01_20m", -1000.0, ["B01"])],
        ),
        (
            ["WVP_20m"],
            20.0,
            [(dirty_equals.IsStr(regex=".*_WVP_20m.jp2"), "WVP_20m", 0.0, ["WVP_20m"])],
        ),
        (
            ["WVP_60m"],
            60.0,
            [(dirty_equals.IsStr(regex=".*_WVP_60m.jp2"), "WVP_60m", 0.0, ["WVP_60m"])],
        ),
        (
            ["AOT_10m", "WVP_20m"],
            10.0,
            [
                (dirty_equals.IsStr(regex=".*_AOT_10m.jp2"), "AOT_10m", 0.0, ["AOT_10m"]),
                (dirty_equals.IsStr(regex=".*_WVP_20m.jp2"), "WVP_20m", 0.0, ["WVP_20m"]),
            ],
        ),
        (
            ["B01_20m", "SCL_20m"],
            20.0,
            [
                (dirty_equals.IsStr(regex=".*_B01_20m.jp2"), "B01_20m", -1000.0, ["B01_20m"]),
                (
                    dirty_equals.IsStr(regex=".*_SCL_20m.jp2"),
                    "SCL_20m",
                    # has neither "raster:scale" nor "raster:offset"
                    0.0,
                    ["SCL_20m"],
                ),
            ],
        ),
    ],
)
def test_resolution_and_offset_handling(
    requests_mock,
    test_data,
    jvm_mock,
    band_names,
    resolution,
    expected_add_links,
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

    factory_mock = jvm_mock.org.openeo.geotrellis.file.PyramidFactory
    cellsize_mock = jvm_mock.geotrellis.raster.CellSize

    feature_builder = mock.MagicMock()
    jvm_mock.org.openeo.opensearch.OpenSearchResponses.featureBuilder.return_value = feature_builder
    feature_builder.withId.return_value = feature_builder
    feature_builder.withNominalDate.return_value = feature_builder
    feature_builder.addLink.return_value = feature_builder

    data_cube = load_stac(
        url=stac_collection_url,
        load_params=LoadParameters(bands=band_names),
        env=EvalEnv(dict(pyramid_levels="highest")),
        layer_properties={},
        batch_jobs=None,
    )

    # TODO: how to check the actual argument to PyramidFactory()?
    factory_mock.assert_called_once()
    cellsize_mock.assert_called_once_with(resolution, resolution)
    assert data_cube.metadata.spatial_extent["crs"] == "EPSG:32636"

    assert [c.args for c in feature_builder.addLink.call_args_list] == expected_add_links


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
            pytest.raises(OpenEOApiException, match="There is no data available for the given extents"),
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
            pytest.raises(OpenEOApiException, match="There is no data available for the given extents"),
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

    assert [resp.call_count for resp in search_transient_error_resps] == [5]


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


def test_is_supported_raster_mime_type():
    assert _is_supported_raster_mime_type("image/tiff; application=geotiff")
    assert _is_supported_raster_mime_type("image/tiff; application=geotiff; profile=cloud-optimized")
    assert _is_supported_raster_mime_type("image/jp2")
    assert _is_supported_raster_mime_type("application/x-hdf5")
    assert _is_supported_raster_mime_type("application/x-hdf")
    assert not _is_supported_raster_mime_type("text/html")


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        ({"href": "https://stac.test/asset.tif"}, False),
        ({"href": "https://stac.test/asset.tif", "roles": ["data"]}, True),
        ({"href": "https://stac.test/asset.tif", "roles": ["data"], "type": "image/tiff; application=geotiff"}, True),
        ({"href": "https://stac.test/asset.tif", "type": "image/tiff; application=geotiff"}, False),
        ({"href": "https://stac.test/asset.html", "roles": ["data"], "type": "text/html"}, False),
        ({"href": "https://stac.test/asset.png", "roles": ["thumbnail"]}, False),
        ({"href": "https://stac.test/asset.png", "bands": [{"name": "B02"}]}, True),
        ({"href": "https://stac.test/asset.png", "eo:bands": [{"name": "B02"}]}, True),
        ({"href": "https://stac.test/asset.png", "roles": [], "bands": [{"name": "B02"}]}, True),
    ],
)
def test_is_band_asset(data, expected):
    asset = pystac.Asset.from_dict(data)
    assert _is_band_asset(asset) == expected


@pytest.mark.parametrize(
    ["catalog", "expected"],
    [
        (None, False),
        (
            pystac.Catalog(
                id="catalog123",
                description="Test Catalog",
                extra_fields={"conformsTo": ["https://api.stacspec.org/v1.0.0/item-search"]},
            ),
            True,
        ),
    ],
)
def test_supports_item_search(tmp_path, catalog, expected):
    links = []
    if catalog:
        catalog_path = tmp_path / "catalog.json"
        pystac.write_file(catalog, dest_href=catalog_path)
        links.append({"rel": "root", "href": str(catalog_path)})

    collection = pystac.Collection.from_dict(StacDummyBuilder.collection(links=links))
    assert _supports_item_search(collection) == expected


def test_proj_code_to_epsg():
    assert _proj_code_to_epsg("EPSG:32631") == 32631
    assert _proj_code_to_epsg("EPSG:4326!") is None
    assert _proj_code_to_epsg("EPSG:onetwothree") is None
    assert _proj_code_to_epsg("IAU_2015:30100") is None
    assert _proj_code_to_epsg(None) is None
    assert _proj_code_to_epsg(1234) is None


class TestProjectionMetadata:
    def test_code_from_epsg(self):
        metadata = _ProjectionMetadata(epsg=32631)
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631

    def test_epsg_from_code(self):
        metadata = _ProjectionMetadata(code="EPSG:32631")
        assert metadata.code == "EPSG:32631"
        assert metadata.epsg == 32631

    def test_bbox_from_shape_and_transform(self):
        # https://github.com/soxofaan/projection/blob/22ada42310b58c00d74f68250fd65c8ba6f178b3/examples/assets.json
        metadata = _ProjectionMetadata(
            code="EPSG:32659",
            shape=[5558, 9559],
            transform=[0.5, 0, 712710, 0, -0.5, 151406, 0, 0, 1],
        )
        assert metadata.bbox == (712710.0, 148627.0, 717489.5, 151406.0)

    def test_resolution_empty(self):
        pm = _ProjectionMetadata()

        with pytest.raises(ValueError, match="Unable to calculate cell size"):
            pm.resolution()

        assert pm.resolution(fail_on_miss=False) is None

    def test_resolution_from_bbox_and_shape(self):
        assert _ProjectionMetadata(
            bbox=(100, 200, 300, 500),
            shape=(30, 50),
        ).resolution() == (4, 10)

        assert _ProjectionMetadata(
            bbox=(1000, 2000, 5000, 8000),
            shape=(100, 200),
        ).resolution() == (20.0, 60.0)

    def test_resolution_from_transform(self):
        assert _ProjectionMetadata(
            transform=[0.5, 0, 712710, 0, -0.5, 151406, 0, 0, 1],
        ).resolution() == (0.5, 0.5)

    def test_from_item_minimal(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        metadata = _ProjectionMetadata.from_item(item)
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
        metadata = _ProjectionMetadata.from_item(item)
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
        metadata = _ProjectionMetadata.from_asset(asset)
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
        metadata = _ProjectionMetadata.from_asset(asset, item=item)
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
        metadata = _ProjectionMetadata.from_asset(asset)
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
        metadata = _ProjectionMetadata(epsg=4326, bbox=(10, 20, 30, 40), shape=shape)
        coverage = metadata.coverage_for(extent)
        assert coverage == expected

    def test_coverage_for_snap_option(self):
        metadata = _ProjectionMetadata(epsg=4326, bbox=(10, 20, 30, 40), shape=[100, 100])
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
        metadata = _ProjectionMetadata(epsg=32631, bbox=[600000, 5590200, 709800, 5700000], shape=shape)
        coverage = metadata.coverage_for(extent)
        assert coverage == expected




def test_get_proj_metadata_minimal():
    asset = pystac.Asset(href="https://example.com/asset.tif")
    item = pystac.Item.from_dict(StacDummyBuilder.item())
    assert _get_proj_metadata(asset, item=item) == (None, None, None)


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
    assert _get_proj_metadata(asset, item=item) == expected


class TestFixGdalOrderedTransform:
    """Tests for _ProjectionMetadata._fix_gdal_ordered_transform"""

    def test_already_valid_rasterio_order(self):
        """Valid rasterio/affine order should not be changed."""
        transform = [0.00025, 0.0, 3.0, 0.0, -0.00025, 51.0]
        assert _ProjectionMetadata._fix_gdal_ordered_transform(transform) == transform

    def test_fix_gdal_order(self):
        """GDAL GetGeoTransform order should be reshuffled to rasterio/affine order."""
        gdal_order = [3.0, 0.00025, 0.0, 51.0, 0.0, -0.00025]
        expected = [0.00025, 0.0, 3.0, 0.0, -0.00025, 51.0]
        assert _ProjectionMetadata._fix_gdal_ordered_transform(gdal_order) == expected

    def test_fix_gdal_order_9_elements(self):
        """GDAL order with 9 elements (full 3x3 matrix) should preserve trailing elements."""
        gdal_order = [3.0, 0.00025, 0.0, 51.0, 0.0, -0.00025, 0.0, 0.0, 1.0]
        expected = [0.00025, 0.0, 3.0, 0.0, -0.00025, 51.0, 0.0, 0.0, 1.0]
        assert _ProjectionMetadata._fix_gdal_ordered_transform(gdal_order) == expected

    def test_short_transform_unchanged(self):
        """Transform with fewer than 6 elements should not be changed."""
        transform = [1.0, 2.0]
        assert _ProjectionMetadata._fix_gdal_ordered_transform(transform) == transform

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
        metadata = _ProjectionMetadata.from_asset(asset, fix_proj_transform=True)
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
        metadata = _ProjectionMetadata.from_asset(asset, fix_proj_transform=False)
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
        assert _ProjectionMetadata._fix_gdal_ordered_transform(gdal_transform) == expected_rasterio


class TestTemporalExtent:
    def test_as_tuple_empty(self):
        extent = _TemporalExtent(None, None)
        assert extent.as_tuple() == (None, None)

    def test_as_tuple(self):
        extent = _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.as_tuple() == (
            datetime.datetime(2025, 3, 4, 11, 11, 11, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 5, 6, 22, 22, 22, tzinfo=datetime.timezone.utc),
        )

    def test_isoformat(self):
        assert _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22").isoformat() == (
            "2025-03-04T11:11:11+00:00",
            "2025-05-06T22:22:22+00:00",
        )
        assert _TemporalExtent(None, "2025-05-06").isoformat() == (None, "2025-05-06T00:00:00+00:00")
        assert _TemporalExtent("2025-05-06", None).isoformat() == ("2025-05-06T00:00:00+00:00", None)
        assert _TemporalExtent(None, None).isoformat() == (None, None)

    def test_intersects_empty(self):
        extent = _TemporalExtent(None, None)
        assert extent.intersects("1789-07-14") == True
        assert extent.intersects(nominal="1789-07-14") == True
        assert extent.intersects(start_datetime="1914-07-28", end_datetime="1918-11-11") == True
        assert extent.intersects(nominal="2025-07-24") == True
        assert extent.intersects(nominal=None) == True

    def test_intersects_nominal_basic(self):
        extent = _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(nominal="2022-10-11") == False
        assert extent.intersects(nominal="2025-03-03T12:13:14") == False
        assert extent.intersects(nominal="2025-03-05T05:05:05") == True
        assert extent.intersects(nominal="2025-07-07T07:07:07") == False

        assert extent.intersects(nominal=datetime.date(2025, 4, 10)) == True
        assert extent.intersects(nominal=datetime.datetime(2025, 4, 10, 12)) == True

        assert extent.intersects(nominal=None) == True

    def test_intersects_nominal_edges(self):
        extent = _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(nominal="2025-03-04T11:11:10") == False
        assert extent.intersects(nominal="2025-03-04T11:11:11") == True
        assert extent.intersects(nominal="2025-03-05T05:05:05") == True
        assert extent.intersects(nominal="2025-05-06T22:22:21") == True
        assert extent.intersects(nominal="2025-05-06T22:22:22") == False

    def test_intersects_nominal_timezones(self):
        extent = _TemporalExtent("2025-03-04T11:11:11Z", "2025-05-06T22:22:22-03")
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
        extent = _TemporalExtent(None, "2025-05-06")
        assert extent.intersects(nominal="1789-07-14") == True
        assert extent.intersects(nominal="2025-05-05") == True
        assert extent.intersects(nominal="2025-05-06") == False
        assert extent.intersects(nominal="2025-11-11") == False
        assert extent.intersects(nominal=None) == True

        extent = _TemporalExtent("2025-05-06", None)
        assert extent.intersects(nominal="2025-05-05") == False
        assert extent.intersects(nominal="2025-05-06") == True
        assert extent.intersects(nominal="2099-11-11") == True
        assert extent.intersects(nominal=None) == True

    def test_intersects_start_end_basic(self):
        extent = _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
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
        extent = _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T11:11:10") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T11:11:11") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T11:11:12") == True

        assert extent.intersects(start_datetime="2025-05-06T22:22:21", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-05-06T22:22:22", end_datetime="2025-08-08") == False
        assert extent.intersects(start_datetime="2025-05-06T22:22:23", end_datetime="2025-08-08") == False

    def test_intersects_start_end_timezones(self):
        extent = _TemporalExtent("2025-03-04T11:11:11Z", "2025-05-06T22:22:22-03")
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T12:12:12") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T12:12:12Z") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T12:12:12+06") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T10:10:10") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-03-04T10:10:10-03") == True

    def test_intersects_start_end_half_open(self):
        extent = _TemporalExtent(None, "2025-05-06")
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-05-05") == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-06-06", end_datetime="2025-08-08") == False
        assert extent.intersects(start_datetime=None, end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-06-06", end_datetime=None) == False
        assert extent.intersects(start_datetime=None, end_datetime="2025-05-05") == True

        extent = _TemporalExtent("2025-05-06", None)
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-05-05") == False
        assert extent.intersects(start_datetime="2025-02-02", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime="2025-06-06", end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime=None, end_datetime=None) == True
        assert extent.intersects(start_datetime="2025-02-02", end_datetime=None) == True
        assert extent.intersects(start_datetime=None, end_datetime="2025-08-08") == True
        assert extent.intersects(start_datetime=None, end_datetime="2025-02-02") == False

    def test_intersects_nominal_vs_start_end(self):
        """https://github.com/Open-EO/openeo-geopyspark-driver/issues/1293"""
        extent = _TemporalExtent("2024-02-01", "2024-02-10")
        assert extent.intersects(nominal="2024-01-01", start_datetime="2024-01-01", end_datetime="2024-12-31") == True

    def test_intersects_interval(self):
        extent = _TemporalExtent("2025-03-04T11:11:11", "2025-05-06T22:22:22")
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


class TestSpatialExtent:
    def test_empty(self):
        extent = _SpatialExtent(bbox=None)
        assert extent.intersects(None) is True
        assert extent.intersects((1, 2, 3, 4)) == True

    def test_basic(self):
        extent = _SpatialExtent(bbox=BoundingBox(west=3, south=51, east=4, north=52, crs=4326))
        assert extent.intersects((1, 2, 3, 4)) == False
        assert extent.intersects((2, 50, 3.1, 51.1)) == True
        assert extent.intersects((3.3, 51.1, 3.5, 51.5)) == True
        assert extent.intersects((3.9, 51.9, 4.4, 52.2)) == True
        assert extent.intersects((5, 51.1, 6, 52.2)) == False


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
        extent = _SpatioTemporalExtent(
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
        extent = _SpatioTemporalExtent(
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
    extent = _spatiotemporal_extent_from_load_params(load_params.spatial_extent, load_params.temporal_extent)
    assert (extent.spatial_extent.as_bbox(), extent.temporal_extent.as_tuple()) == expected


class TestPropertyFilter:
    def test_build_matcher_empty(self):
        """Empty property filter: always matches"""
        property_filter = PropertyFilter(properties={})
        matcher = property_filter.build_matcher()
        assert matcher({}) == True
        assert matcher({"foo": "bar"}) == True

    def test_build_matcher_basic(self):
        """Basic use case: single (equality) condition"""
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "bar",
                        },
                        "result": True,
                    }
                }
            }
        }
        property_filter = PropertyFilter(properties)
        matcher = property_filter.build_matcher()
        assert matcher({}) == False
        assert matcher({"foo": "bar"}) == True
        assert matcher({"foo": "nope"}) == False
        assert matcher({"fooooo": "bar"}) == False

    def test_build_matcher_multiple_conditions(self):
        """Multiple conditions: all must match"""
        properties = {
            "color": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "red",
                        },
                        "result": True,
                    }
                }
            },
            "size": {
                "process_graph": {
                    "lte1": {
                        "process_id": "lte",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": 42,
                        },
                        "result": True,
                    }
                }
            },
        }
        property_filter = PropertyFilter(properties=properties)
        matcher = property_filter.build_matcher()
        assert matcher({}) == False
        assert matcher({"color": "red", "size": 10}) == True
        assert matcher({"color": "rrred", "size": 10}) == False
        assert matcher({"color": "red", "size": 41}) == True
        assert matcher({"color": "red", "size": 42}) == True
        assert matcher({"color": "red", "size": 43}) == False
        assert matcher({"color": "red", "size": 100}) == False

    @pytest.mark.parametrize(
        ["pg_node", "matching", "non_matching"],
        [
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "y-bar"}},
                ["y-bar"],
                ["nope", None],
            ),
            (
                {"process_id": "eq", "arguments": {"x": "x-bar", "y": {"from_parameter": "value"}}},
                ["x-bar"],
                ["nope", None],
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                [42],
                [0, 42.01, 44, None],
            ),
            (
                {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                [0, 42],
                [42.01, 100, None],
            ),
            (
                {"process_id": "lte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                [42, 100],
                [0, 41, None],
            ),
            (
                {"process_id": "gte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                [42, 100],
                [0, 41, None],
            ),
            (
                {"process_id": "gte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                [42, 0],
                [100, None],
            ),
            (
                {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
                [42, 4242],
                [0, 41, None, -101],
            ),
        ],
    )
    def test_build_matcher_operators(self, pg_node, matching, non_matching):
        """Single conditions in multiple variants (operators, argument order)"""
        properties = {"foo": {"process_graph": {"_": {**pg_node, "result": True}}}}
        property_filter = PropertyFilter(properties=properties)
        matcher = property_filter.build_matcher()
        for value in matching:
            assert matcher({"foo": value}) == True
        for value in non_matching:
            assert matcher({"foo": value}) == False

    def test_build_matcher_with_env(self):
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": {"from_parameter": "name"},
                        },
                        "result": True,
                    }
                }
            }
        }
        env = EvalEnv().push_parameters({"name": "alice"})
        property_filter = PropertyFilter(properties=properties, env=env)
        matcher = property_filter.build_matcher()
        assert matcher({"foo": "alice"}) == True
        assert matcher({"foo": "bob"}) == False

    @pytest.mark.parametrize(
        ["properties", "expected"],
        [
            ({}, ""),
            (
                {
                    "foo": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {"x": {"from_parameter": "value"}, "y": "bar"},
                                "result": True,
                            }
                        }
                    }
                },
                "\"properties.foo\" = 'bar'",
            ),
            (
                {
                    "color": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": "red",
                                },
                                "result": True,
                            }
                        }
                    },
                    "size": {
                        "process_graph": {
                            "lte1": {
                                "process_id": "lte",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": 42,
                                },
                                "result": True,
                            }
                        }
                    },
                },
                dirty_equals.IsOneOf(
                    '"properties.color" = \'red\' and "properties.size" <= 42',
                    '"properties.size" <= 42 and "properties.color" = \'red\'',
                ),
            ),
        ],
    )
    def test_to_cql2_text(self, properties, expected):
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_text() == expected

    @pytest.mark.parametrize(
        ["pg_node", "expected"],
        [
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "y-bar"}},
                "\"properties.foo\" = 'y-bar'",
            ),
            (
                {"process_id": "eq", "arguments": {"x": "x-bar", "y": {"from_parameter": "value"}}},
                "\"properties.foo\" = 'x-bar'",
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                '"properties.foo" = 42',
            ),
            (
                {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                '"properties.foo" <= 42',
            ),
            (
                {"process_id": "lte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                '"properties.foo" >= 42',
            ),
            (
                {"process_id": "gte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                '"properties.foo" >= 42',
            ),
            (
                {"process_id": "gte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                '"properties.foo" <= 42',
            ),
            (
                {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
                '"properties.foo" in (42, 4242)',
            ),
            (
                {
                    "process_id": "array_contains",
                    "arguments": {"data": ["blue", "green"], "y": {"from_parameter": "value"}},
                },
                "\"properties.foo\" in ('blue', 'green')",
            ),
        ],
    )
    def test_to_cql2_text_operators(self, pg_node, expected):
        properties = {"foo": {"process_graph": {"_": {**pg_node, "result": True}}}}
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_text() == expected

    def test_to_cql2_text_with_env(self):
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": {"from_parameter": "name"},
                        },
                        "result": True,
                    }
                }
            }
        }
        env = EvalEnv().push_parameters({"name": "alice"})
        property_filter = PropertyFilter(properties=properties, env=env)
        expected = "\"properties.foo\" = 'alice'"
        assert property_filter.to_cql2_text() == expected

    @pytest.mark.parametrize(
        ["properties", "expected"],
        [
            ({}, None),
            (
                {
                    "foo": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {"x": {"from_parameter": "value"}, "y": "bar"},
                                "result": True,
                            }
                        }
                    }
                },
                {"op": "=", "args": [{"property": "properties.foo"}, "bar"]},
            ),
            (
                {
                    "color": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": "red",
                                },
                                "result": True,
                            }
                        }
                    },
                    "size": {
                        "process_graph": {
                            "lte1": {
                                "process_id": "lte",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": 42,
                                },
                                "result": True,
                            }
                        }
                    },
                },
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "properties.color"}, "red"]},
                        {"op": "<=", "args": [{"property": "properties.size"}, 42]},
                    ],
                },
            ),
        ],
    )
    def test_to_cql2_json(self, properties, expected):
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_json() == expected

    @pytest.mark.parametrize(
        ["pg_node", "expected"],
        [
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "y-bar"}},
                {"op": "=", "args": [{"property": "properties.foo"}, "y-bar"]},
            ),
            (
                {"process_id": "eq", "arguments": {"x": "x-bar", "y": {"from_parameter": "value"}}},
                {"op": "=", "args": [{"property": "properties.foo"}, "x-bar"]},
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                {"op": "=", "args": [{"property": "properties.foo"}, 42]},
            ),
            (
                {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                {"op": "<=", "args": [{"property": "properties.foo"}, 42]},
            ),
            (
                {"process_id": "lte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                {"op": ">=", "args": [{"property": "properties.foo"}, 42]},
            ),
            (
                {"process_id": "gte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                {"op": ">=", "args": [{"property": "properties.foo"}, 42]},
            ),
            (
                {"process_id": "gte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                {"op": "<=", "args": [{"property": "properties.foo"}, 42]},
            ),
            (
                {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
                {"op": "in", "args": [{"property": "properties.foo"}, [42, 4242]]},
            ),
            (
                {
                    "process_id": "array_contains",
                    "arguments": {"data": ["blue", "green"], "y": {"from_parameter": "value"}},
                },
                {"op": "in", "args": [{"property": "properties.foo"}, ["blue", "green"]]},
            ),
        ],
    )
    def test_to_cql2_json_operators(self, pg_node, expected):
        properties = {"foo": {"process_graph": {"_": {**pg_node, "result": True}}}}
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_json() == expected

    def test_to_cql2_json_with_env(self):
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": {"from_parameter": "name"},
                        },
                        "result": True,
                    }
                }
            }
        }
        env = EvalEnv().push_parameters({"name": "alice"})
        property_filter = PropertyFilter(properties=properties, env=env)
        expected = {"op": "=", "args": [{"property": "properties.foo"}, "alice"]}
        assert property_filter.to_cql2_json() == expected

    @pytest.mark.parametrize(
        ["use_filter_extension", "search_method", "expected"],
        [
            ("cql2-text", None, "\"properties.foo\" = 'bar'"),
            ("cql2-json", None, {"op": "=", "args": [{"property": "properties.foo"}, "bar"]}),
            (True, "POST", {"op": "=", "args": [{"property": "properties.foo"}, "bar"]}),
            (True, "GET", "\"properties.foo\" = 'bar'"),
        ],
    )
    def test_to_cql2_filter(self, use_filter_extension, search_method, expected, requests_mock):
        links = [{"rel": "self", "href": "https://stac.test/"}]
        if search_method:
            links.append({"rel": "search", "href": "https://stac.test/search", "method": search_method})

        requests_mock.get(
            "https://stac.test/",
            json={
                "stac_version": "1.0.0",
                "conformsTo": ["https://api.stacspec.org/v1.0.0/item-search"],
                "type": "Catalog",
                "id": "test-catalog",
                "description": "Test STAC catalog",
                "links": links,
            },
        )

        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "bar",
                        },
                        "result": True,
                    }
                }
            }
        }
        property_filter = PropertyFilter(properties=properties)
        client = pystac_client.Client.open("https://stac.test/")

        assert (
            property_filter.to_cql2_filter(
                use_filter_extension=use_filter_extension,
                client=client,
            )
            == expected
        )


class TestAdaptingPropertyFilter:
    @pytest.mark.parametrize(
        ["adaptations", "expected_text", "expected_json"],
        [
            (
                # Empty case (no adaptations)
                {},
                "\"properties.foo\" = 'FOO' and \"properties.bar\" = 'BAR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "properties.foo"}, "FOO"]},
                        {"op": "=", "args": [{"property": "properties.bar"}, "BAR"]},
                    ],
                },
            ),
            (
                # Drop "foo"
                {"foo": "drop"},
                "\"properties.bar\" = 'BAR'",
                {"op": "=", "args": [{"property": "properties.bar"}, "BAR"]},
            ),
            (
                # Rename "foo" to "fancyfoo"
                {"foo": {"rename": "fancyfoo"}},
                "\"properties.fancyfoo\" = 'FOO' and \"properties.bar\" = 'BAR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "properties.fancyfoo"}, "FOO"]},
                        {"op": "=", "args": [{"property": "properties.bar"}, "BAR"]},
                    ],
                },
            ),
            (
                # Map values
                {
                    "foo": {"value_mapping": {"SOMETHING": "else"}},
                    "bar": {"value_mapping": {"BAR": "BARRRR"}},
                },
                "\"properties.foo\" = 'FOO' and \"properties.bar\" = 'BARRRR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "properties.foo"}, "FOO"]},
                        {"op": "=", "args": [{"property": "properties.bar"}, "BARRRR"]},
                    ],
                },
            ),
            (
                # add-MGRS-prefix
                {"foo": {"value_mapping": "add-MGRS-prefix"}},
                "\"properties.foo\" = 'MGRS-FOO' and \"properties.bar\" = 'BAR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "properties.foo"}, "MGRS-FOO"]},
                        {"op": "=", "args": [{"property": "properties.bar"}, "BAR"]},
                    ],
                },
            ),
        ],
    )
    def test_overrides_to_cql2(self, adaptations, expected_text, expected_json):
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "FOO"},
                        "result": True,
                    }
                }
            },
            "bar": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "BAR"},
                        "result": True,
                    }
                }
            },
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)
        assert property_filter.to_cql2_text() == expected_text
        assert property_filter.to_cql2_json() == expected_json

    @pytest.mark.parametrize(
        ["adaptations", "no_match", "match"],
        [
            (
                {},
                [{}, {"foo": "bar"}],
                [{"foo": "FOO"}],
            ),
            (
                {"foo": "drop"},
                [],
                [{}, {"anything": "goes"}],
            ),
            (
                {"foo": {"rename": "hohoho", "value_mapping": {"FOO": "HAHAHA"}}},
                [{}, {"fancyfoo": "FOO"}, {"foo": "FOO"}],
                [{"hohoho": "HAHAHA"}],
            ),
            (
                {"foo": {"rename": "mgrs-foo", "value_mapping": "add-MGRS-prefix"}},
                [{}, {"foo": "FOO"}, {"mgrs-foo": "FOO"}],
                [{"mgrs-foo": "MGRS-FOO"}],
            ),
        ],
    )
    def test_overrides_to_matcher(self, adaptations, no_match, match):
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "FOO"},
                        "result": True,
                    }
                }
            }
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)

        matcher = property_filter.build_matcher()
        assert [matcher(input) for input in no_match] == [False] * len(no_match)
        assert [matcher(input) for input in match] == [True] * len(match)

    def test_logging(self, caplog):
        caplog.set_level(level=logging.INFO)
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "FOO"},
                        "result": True,
                    }
                }
            }
        }
        adaptations = {
            "foo": {"rename": "mgrs-foo", "value_mapping": "add-MGRS-prefix"},
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)
        property_filter.to_cql2_text()
        assert caplog.messages == [
            """AdaptingPropertyFilter: updates=["Rename 'foo' to 'mgrs-foo'", "Map 'mgrs-foo' value 'FOO' to 'MGRS-FOO'"]"""
        ]

    @pytest.mark.parametrize(
        ["adaptations", "expected_text", "expected_json"],
        [
            (
                {"foo": {"rename": "ffooo", "value_mapping": {"F22": "F2000"}}},
                """"properties.ffooo" in ('F1', 'F2000', 'F333')""",
                {"op": "in", "args": [{"property": "properties.ffooo"}, ["F1", "F2000", "F333"]]},
            ),
            (
                {"foo": {"rename": "mgrs-foo", "value_mapping": "add-MGRS-prefix"}},
                """"properties.mgrs-foo" in ('MGRS-F1', 'MGRS-F22', 'MGRS-F333')""",
                {"op": "in", "args": [{"property": "properties.mgrs-foo"}, ["MGRS-F1", "MGRS-F22", "MGRS-F333"]]},
            ),
        ],
    )
    def test_contains(self, adaptations, expected_text, expected_json):
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "contains": {
                        "process_id": "array_contains",
                        "arguments": {"data": ["F1", "F22", "F333"], "value": {"from_parameter": "value"}},
                        "result": True,
                    }
                }
            }
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)
        assert property_filter.to_cql2_text() == expected_text
        assert property_filter.to_cql2_json() == expected_json


class TestItemCollection:
    def test_from_stac_item_basic(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        spatiotemporal_extent = _SpatioTemporalExtent(bbox=None, from_date=None, to_date=None)
        item_collection = ItemCollection.from_stac_item(item, spatiotemporal_extent=spatiotemporal_extent)

        assert item_collection.items == [item]

    @pytest.mark.parametrize(
        ["bbox", "interval", "expected"],
        [
            ((20, 34, 26, 40), ["2025-09-01", "2025-10-01"], True),
            ((20, 34, 26, 40), ["2025-10-01", "2025-11-01"], False),
            ((30, 34, 36, 40), ["2025-09-01", "2025-10-01"], False),
        ],
    )
    def test_from_stac_item_with_filtering(self, bbox, interval, expected):
        item = pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-09-04", bbox=[20, 30, 25, 35]))

        from_date, to_date = interval
        spatiotemporal_extent = _SpatioTemporalExtent(
            bbox=BoundingBox.from_wsen_tuple(bbox, crs=4326), from_date=from_date, to_date=to_date
        )
        item_collection = ItemCollection.from_stac_item(item, spatiotemporal_extent=spatiotemporal_extent)
        expected = [item] if expected else []
        assert item_collection.items == expected

    @pytest.mark.parametrize(
        ["bbox", "interval", "expected"],
        [
            # Full spatio-temporal overlap
            ((10, 20, 30, 40), ["2025-09-01", "2025-10-01"], [1, 2]),
            ((21, 31, 26, 36), ["2025-09-01", "2025-10-01"], [1, 2]),
            # Spatial constraints
            ((20, 30, 23, 33), ["2025-09-01", "2025-10-01"], [1]),
            ((26, 36, 27, 37), ["2025-09-01", "2025-10-01"], [2]),
            # Temporal constraints
            ((20, 34, 26, 40), ["2025-09-01", "2025-09-07"], [1]),
            ((20, 34, 26, 40), ["2025-09-05", "2025-09-10"], [2]),
            # No overlap
            ((10, 20, 30, 40), ["2025-10-01", "2025-11-01"], []),
            ((70, 70, 80, 80), ["2025-09-01", "2025-10-01"], []),
        ],
    )
    @gps_config_overrides(use_zk_job_registry=False)
    def test_from_own_job(self, bbox, interval, expected):
        from_date, to_date = interval
        spatiotemporal_extent = _SpatioTemporalExtent(
            bbox=BoundingBox.from_wsen_tuple(bbox, crs=4326), from_date=from_date, to_date=to_date
        )

        user = User("john")
        job_registry = InMemoryJobRegistry()
        batch_jobs = GpsBatchJobs(catalog=None, jvm=None, elastic_job_registry=job_registry)
        job = batch_jobs.create_job(user=user, process={"foo": "bar"}, api_version="1.0.0", metadata={})
        job_registry.set_status(job_id=job.id, user_id=user.user_id, status="finished")
        job_registry.set_results_metadata(
            job_id=job.id,
            user_id=user.user_id,
            costs=0,
            usage={},
            results_metadata={
                "assets": {
                    "asset1": {
                        "bbox": [20, 30, 25, 35],
                        "geometry": bbox_to_geojson(20, 30, 25, 35),
                        "datetime": "2025-09-04T10:00:00Z",
                        "roles": ["data"],
                        "href": "https://data.test/asset1.tif",
                        "bands": [{"name": "red"}],
                    },
                    "asset2": {
                        "bbox": [24, 34, 28, 38],
                        "geometry": bbox_to_geojson(24, 34, 28, 38),
                        "datetime": "2025-09-08T10:00:00Z",
                        "roles": ["data"],
                        "href": "https://data.test/asset2.tif",
                        "bands": [{"name": "red"}],
                    },
                }
            },
        )

        item_collection = ItemCollection.from_own_job(
            job=job, spatiotemporal_extent=spatiotemporal_extent, batch_jobs=batch_jobs, user=user
        )

        expected_map = {
            1: dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "stac_version": dirty_equals.IsOneOf("1.0.0", "1.1.0"),
                    "id": "asset1",
                    "assets": {"asset1": {"eo:bands": [{"name": "red"}], "href": "https://data.test/asset1.tif"}},
                    "bbox": [20, 30, 25, 35],
                    "properties": {"datetime": "2025-09-04T10:00:00Z"},
                }
            ),
            2: dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "stac_version": dirty_equals.IsOneOf("1.0.0", "1.1.0"),
                    "id": "asset2",
                    "assets": {"asset2": {"eo:bands": [{"name": "red"}], "href": "https://data.test/asset2.tif"}},
                    "bbox": [24, 34, 28, 38],
                    "properties": {"datetime": "2025-09-08T10:00:00Z"},
                }
            ),
        }
        expected = [expected_map[e] for e in expected]
        assert [item.to_dict() for item in item_collection.items] == expected

    def test_from_stac_catalog_basic(self):
        collection = pystac.Collection(
            id="c123",
            description="C123",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[20, 30, 25, 35]]),
                temporal=pystac.TemporalExtent.from_dict({"interval": ["2025-07-01", "2025-08-31"]}),
            ),
        )
        item1 = pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-07-10", bbox=[21, 31, 25, 35]))
        item2 = pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-07-20", bbox=[22, 32, 25, 35]))
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item1))
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item2))

        spatiotemporal_extent = _SpatioTemporalExtent(bbox=None, from_date=None, to_date=None)
        item_collection = ItemCollection.from_stac_catalog(collection, spatiotemporal_extent=spatiotemporal_extent)
        assert item_collection.items == [item1, item2]

    @pytest.mark.parametrize(
        ["bbox", "interval", "expected"],
        [
            (None, None, [1, 2]),
            ([20, 30, 25, 35], ["2025-06-01", "2025-09-30"], [1, 2]),
            ([20, 30, 21, 31], ["2025-06-01", "2025-09-30"], [1]),
            ([22.3, 32.3, 22.6, 32.6], ["2025-06-01", "2025-09-30"], [2]),
            ([20, 30, 25, 35], ["2025-07-05", "2025-07-15"], [1]),
            ([20, 30, 25, 35], ["2025-07-15", "2025-07-25"], [2]),
        ],
    )
    def test_from_stac_catalog_spatiotemporal_filtering(self, bbox, interval, expected):
        collection = pystac.Collection(
            id="c123",
            description="C123",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent(bboxes=[[20, 30, 25, 35]]),
                temporal=pystac.TemporalExtent.from_dict({"interval": ["2025-07-01", "2025-08-31"]}),
            ),
        )
        item1 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item1", datetime="2025-07-10", bbox=[21, 31, 21.5, 31.5])
        )
        item2 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", datetime="2025-07-20", bbox=[22, 32, 22.5, 32.5])
        )
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item1))
        collection.add_link(pystac.Link(rel=pystac.RelType.ITEM, target=item2))

        from_date, to_date = interval or (None, None)
        if bbox:
            bbox = BoundingBox.from_wsen_tuple(bbox, crs=4326)
        spatiotemporal_extent = _SpatioTemporalExtent(bbox=bbox, from_date=from_date, to_date=to_date)
        item_collection = ItemCollection.from_stac_catalog(collection, spatiotemporal_extent=spatiotemporal_extent)

        expected = [{1: item1, 2: item2}[x] for x in expected]
        assert item_collection.items == expected

    @pytest.fixture
    def dummy_stac_api_server(self) -> DummyStacApiServer:
        dummy_server = DummyStacApiServer()

        dummy_server.define_collection(
            "custom-s2",
            extent={
                "spatial": {"bbox": [[3, 50, 5, 51]]},
                "temporal": {"interval": [["2024-02-01T00:00:00Z", "2024-12-01"]]},
            },
        )
        for m in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
            for x in [3, 4]:
                dummy_server.define_item(
                    collection_id="custom-s2",
                    item_id=f"item-{m}-{x}",
                    datetime=f"2024-{m:02d}-20T12:00:00Z",
                    bbox=[x + 0.1, 50.1, x + 0.9, 50.9],
                    properties={"flavor": {0: "apple", 1: "banana", 2: "coconut"}[(m + x) % 3]},
                )
        return dummy_server

    @pytest.fixture
    def dummy_stac_api(self, dummy_stac_api_server) -> Iterator[str]:
        with dummy_stac_api_server.serve() as root_url:
            yield root_url

    def test_from_stac_api_basic(self, dummy_stac_api):
        given_url = f"{dummy_stac_api}/collections/collection-123"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        spatiotemporal_extent = _SpatioTemporalExtent(bbox=None, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert [item.id for item in item_collection.items] == ["item-1", "item-2", "item-3"]

    @pytest.mark.parametrize(
        ["from_date", "to_date", "expected_items"],
        [
            ("2024-06-01", "2024-09-01", {"item-6-3", "item-6-4", "item-7-3", "item-7-4", "item-8-3", "item-8-4"}),
            (None, "2024-04-01", {"item-2-3", "item-2-4", "item-3-3", "item-3-4"}),
            ("2024-09-01", None, {"item-9-3", "item-9-4", "item-10-3", "item-10-4", "item-11-3", "item-11-4"}),
        ],
    )
    def test_from_stac_api_temporal_filter(self, dummy_stac_api, from_date, to_date, expected_items):
        given_url = f"{dummy_stac_api}/collections/custom-s2"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        spatiotemporal_extent = _SpatioTemporalExtent(bbox=None, from_date=from_date, to_date=to_date)
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert set(item.id for item in item_collection.items) == expected_items

    @pytest.mark.parametrize(
        ["bbox", "expected_items"],
        [
            (
                BoundingBox(3, 50, 4, 51, crs=4326),
                {f"item-{x}-3" for x in range(2, 12)},
            ),
            (
                BoundingBox(4, 50, 5, 51, crs=4326),
                {f"item-{x}-4" for x in range(2, 12)},
            ),
        ],
    )
    def test_from_stac_api_spatial_filter(self, dummy_stac_api, bbox, expected_items):
        given_url = f"{dummy_stac_api}/collections/custom-s2"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(properties={})
        spatiotemporal_extent = _SpatioTemporalExtent(bbox=bbox, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
        )
        assert set(item.id for item in item_collection.items) == expected_items

    @pytest.mark.parametrize(
        ["use_filter_extension", "expected_search"],
        [
            (
                "cql2-json",
                {
                    "method": "POST",
                    "path": "/search",
                    "url_params": {},
                    "json": {
                        "collections": ["custom-s2"],
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": STAC_API_PER_PAGE_LIMIT_DEFAULT,
                        "filter-lang": "cql2-json",
                        "filter": {"op": "=", "args": [{"property": "properties.flavor"}, "banana"]},
                    },
                },
            ),
            (
                "cql2-text",
                {
                    "method": "GET",
                    "path": "/search",
                    "url_params": {
                        "collections": "custom-s2",
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": str(STAC_API_PER_PAGE_LIMIT_DEFAULT),
                        "filter-lang": "cql2-text",
                        "filter": "\"properties.flavor\" = 'banana'",
                    },
                    "json": None,
                },
            ),
            (
                # Auto mode: Prefer POST with cql2-json if supported by server
                True,
                {
                    "method": "POST",
                    "path": "/search",
                    "url_params": {},
                    "json": {
                        "collections": ["custom-s2"],
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": STAC_API_PER_PAGE_LIMIT_DEFAULT,
                        "filter-lang": "cql2-json",
                        "filter": {"op": "=", "args": [{"property": "properties.flavor"}, "banana"]},
                    },
                },
            ),
            (
                # No usage of filter extension
                False,
                {
                    "method": "GET",
                    "path": "/search",
                    "url_params": {
                        "collections": "custom-s2",
                        "datetime": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
                        "limit": str(STAC_API_PER_PAGE_LIMIT_DEFAULT),
                    },
                    "json": None,
                },
            ),
        ],
    )
    def test_from_stac_api_property_filter(
        self, dummy_stac_api, dummy_stac_api_server, use_filter_extension, expected_search
    ):
        given_url = f"{dummy_stac_api}/collections/custom-s2"
        collection: pystac.Collection = pystac.read_file(given_url)
        property_filter = PropertyFilter(
            properties={
                "flavor": {
                    "process_graph": {
                        "eq": {
                            "process_id": "eq",
                            "arguments": {"x": {"from_parameter": "value"}, "y": "banana"},
                            "result": True,
                        }
                    }
                }
            }
        )
        spatiotemporal_extent = _SpatioTemporalExtent(bbox=None, from_date="2024-01-01", to_date="2025-01-01")
        item_collection = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=property_filter,
            spatiotemporal_extent=spatiotemporal_extent,
            use_filter_extension=use_filter_extension,
        )
        assert set(item.id for item in item_collection.items) == {
            "item-3-4",
            "item-4-3",
            "item-6-4",
            "item-7-3",
            "item-9-4",
            "item-10-3",
        }

        # Check search requests made to the STAC API server
        search_requests = [r for r in dummy_stac_api_server.request_history if r["path"] == "/search"]
        assert search_requests == [expected_search]

    def test_get_temporal_extent_empty(self):
        item_collection = ItemCollection(items=[])
        assert item_collection.get_temporal_extent() == (None, None)

    def test_get_temporal_extent_just_datetime(self):
        item_collection = ItemCollection(
            items=[
                pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-11-11T00:00:00Z")),
                pystac.Item.from_dict(StacDummyBuilder.item(datetime="2025-11-12T00:00:00Z")),
            ]
        )

        assert item_collection.get_temporal_extent() == (
            datetime.datetime(2025, 11, 11, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 11, 12, tzinfo=datetime.timezone.utc),
        )

    def test_get_temporal_extent_start_and_end(self):
        item_collection = ItemCollection(
            items=[
                pystac.Item.from_dict(
                    StacDummyBuilder.item(
                        datetime="2025-11-11T00:00:00Z",
                        properties={
                            "start_datetime": "2025-11-10T10:00:00Z",
                            "end_datetime": "2025-11-12T12:00:00Z",
                        },
                    )
                ),
                pystac.Item.from_dict(
                    StacDummyBuilder.item(
                        datetime="2025-11-15T00:00:00Z",
                        properties={
                            "start_datetime": "2025-11-14T14:00:00Z",
                            "end_datetime": "2025-11-16T16:00:00Z",
                        },
                    )
                ),
            ]
        )

        assert item_collection.get_temporal_extent() == (
            datetime.datetime(2025, 11, 10, hour=10, tzinfo=datetime.timezone.utc),
            datetime.datetime(2025, 11, 16, hour=16, tzinfo=datetime.timezone.utc),
        )

    def test_deduplicated(self, dummy_stac_api_server, dummy_stac_api):
        collection_id = "with-dups"
        dummy_stac_api_server.define_collection(collection_id)
        for i in range(6):
            dummy_stac_api_server.define_item(
                collection_id=collection_id,
                item_id=f"item-{i}",
                datetime=f"2025-11-{(i // 2) + 1:02d}",
            )

        given_url = f"{dummy_stac_api}/collections/{collection_id}"
        collection: pystac.Collection = pystac.read_file(given_url)
        orig = ItemCollection.from_stac_api(
            collection,
            original_url=given_url,
            property_filter=PropertyFilter(properties={}),
            spatiotemporal_extent=_SpatioTemporalExtent(bbox=None, from_date="2025-01-01", to_date=None),
        )
        assert set(item.id for item in orig.items) == {
            "item-0",
            "item-1",
            "item-2",
            "item-3",
            "item-4",
            "item-5",
        }

        deduplicator = ItemDeduplicator()
        deduped = orig.deduplicated(deduplicator=deduplicator)
        assert set(item.id for item in deduped.items) == {
            "item-1",
            "item-3",
            "item-5",
        }


def test_construct_item_collection_minimal(dummy_stac_api):
    url = f"{dummy_stac_api}/collections/collection-123"
    item_collection, metadata, bands, netcdf_with_time_dimension = construct_item_collection(url=url)
    assert set(item.id for item in item_collection.items) == {"item-1", "item-2", "item-3"}
    assert bands == []
    # TODO deeper tests that also involve various band metadata detection aspects


class TestItemDeduplicator:
    def test_trivial(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        depuplicator = ItemDeduplicator()
        assert depuplicator.deduplicate([item]) == [item]

    def test_basic(self):
        item10 = pystac.Item.from_dict(StacDummyBuilder.item(id="item-10", datetime="2025-11-10T00:00:00Z"))
        item10_1s = pystac.Item.from_dict(StacDummyBuilder.item(id="item-10+1s", datetime="2025-11-10T00:00:01Z"))
        item10_1h = pystac.Item.from_dict(StacDummyBuilder.item(id="item-10+1h", datetime="2025-11-10T01:00:00Z"))
        item11 = pystac.Item.from_dict(StacDummyBuilder.item(id="item-11", datetime="2025-11-11T00:00:00Z"))

        depuplicator = ItemDeduplicator()
        assert depuplicator.deduplicate([item10, item10_1s, item11]) == [item10_1s, item11]
        assert depuplicator.deduplicate([item10, item10_1h, item11]) == [item10, item10_1h, item11]

    def test_property_based(self):
        item600 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item1", properties={"product:type": "a", "flavor": "apple"})
        )
        item600b = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", properties={"product:type": "a", "flavor": "banana"})
        )
        item601 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item3", properties={"product:type": "b", "flavor": "apple"})
        )

        depuplicator = ItemDeduplicator()
        assert depuplicator.deduplicate([item600, item600b, item601]) == [item600b, item601]

        depuplicator = ItemDeduplicator(duplication_properties=["flavor"])
        assert depuplicator.deduplicate([item600, item600b, item601]) == [item601, item600b]

    @pytest.mark.parametrize(
        ["item2_updated", "best"],
        [
            ("2025-11-12T12:00:10Z", "item2"),
            ("2025-11-12T11:00:00Z", "item1"),
        ],
    )
    def test_updated(self, item2_updated, best):
        item1 = pystac.Item.from_dict(StacDummyBuilder.item(id="item1", properties={"updated": "2025-11-12T12:00:00Z"}))
        item2 = pystac.Item.from_dict(StacDummyBuilder.item(id="item2", properties={"updated": item2_updated}))
        depuplicator = ItemDeduplicator()
        result = depuplicator.deduplicate([item1, item2])
        assert [r.id for r in result] == [best]

    @pytest.mark.parametrize(
        ["bbox2", "expected"],
        [
            ([3, 50, 4, 51], ["item2", "item3"]),
            ([4, 50, 5, 51], ["item1", "item3"]),
            (None, ["item1", "item2", "item3"]),
            ([8, 40, 9, 41], ["item1", "item2", "item3"]),
            # Invalid bboxes, but should not break deduplication
            (123, ["item1", "item2", "item3"]),
            ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], ["item1", "item2", "item3"]),
            (["one", "two", "three"], ["item1", "item2", "item3"]),
        ],
    )
    def test_duplicate_by_bbox(self, bbox2, expected):
        item1 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item1", bbox=[3, 50, 4, 51], properties={"updated": "2025-11-01"})
        )
        item2 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", bbox=bbox2, properties={"updated": "2025-11-02"})
        )
        item3 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item3", bbox=[4, 50, 5, 51], properties={"updated": "2025-11-03"})
        )

        depuplicator = ItemDeduplicator()
        result = depuplicator.deduplicate([item1, item2, item3])
        assert [r.id for r in result] == expected

    @pytest.mark.parametrize(
        ["geometry2", "expected"],
        [
            (
                {"type": "Polygon", "coordinates": [[[3, 50], [4, 50], [4, 51], [3, 51], [3, 50]]]},
                ["item2", "item3"],
            ),
            (
                {"type": "Polygon", "coordinates": [[[4, 50], [5, 50], [5, 51], [4, 51], [4, 50]]]},
                ["item1", "item3"],
            ),
            (
                {"type": "Polygon", "coordinates": [[[4, 50], [5, 50], [5, 51], [4, 51], [4, 50.0001]]]},
                ["item1", "item3"],
            ),
            (None, ["item1", "item2", "item3"]),
            (
                {"type": "Polygon", "coordinates": [[[8, 40], [9, 40], [9, 41], [8, 41], [8, 40]]]},
                ["item1", "item2", "item3"],
            ),
            # Invalid geometry, but should not break deduplication
            ({"type": "MobiusRing"}, ["item1", "item2", "item3"]),
            ([666, 777], ["item1", "item2", "item3"]),
        ],
    )
    def test_duplicate_by_geometry(self, geometry2, expected):
        item1 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="item1",
                geometry={"type": "Polygon", "coordinates": [[[3, 50], [4, 50], [4, 51], [3, 51], [3, 50]]]},
                properties={"updated": "2025-11-01"},
            )
        )
        item2 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", geometry=geometry2, properties={"updated": "2025-11-02"})
        )
        item3 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="item3",
                geometry={"type": "Polygon", "coordinates": [[[4, 50], [5, 50], [5, 51], [4, 51], [4, 50]]]},
                properties={"updated": "2025-11-03"},
            )
        )

        depuplicator = ItemDeduplicator()
        result = depuplicator.deduplicate([item1, item2, item3])
        assert [r.id for r in result] == expected

    @pytest.mark.parametrize(
        ["datetime2", "expected"],
        [
            ("2025-11-10T00:00:00Z", ["item2", "item3"]),
            ("2025-11-10T12:00:00Z", ["item1", "item2", "item3"]),
            ("2025-11-11T00:00:00Z", ["item1", "item3"]),
            ("2025-11-12T00:00:00Z", ["item1", "item3", "item2"]),
            ("2025-11-10T00:00:00+00", ["item2", "item3"]),
            ("2025-11-10T00:00:00+07", ["item2", "item1", "item3"]),
            ("2025-11-10", ["item2", "item3"]),
            ("2025-11-11", ["item1", "item3"]),
        ],
    )
    def test_datetime_and_timezones(self, datetime2, expected):
        item1 = pystac.Item.from_dict(StacDummyBuilder.item(id="item1", datetime="2025-11-10T00:00:00Z"))
        item2 = pystac.Item.from_dict(StacDummyBuilder.item(id="item2", datetime=datetime2))
        item3 = pystac.Item.from_dict(StacDummyBuilder.item(id="item3", datetime="2025-11-11T00:00:00Z"))

        depuplicator = ItemDeduplicator()
        result = depuplicator.deduplicate([item1, item2, item3])
        assert [r.id for r in result] == expected

    def test_s1_sigma0_version_dedup(self):
        """Sentinel-1 SIGMA0 items with different processing versions (V110 vs V120) should be deduplicated,
        keeping the higher version."""
        common_properties = {
            "datetime": "2020-03-25T17:24:42Z",
            "platform": "sentinel-1a",
            "constellation": "sentinel-1",
            "sar:frequency_band": "C",
            "sar:instrument_mode": "IW",
            "sar:observation_direction": "right",
            "sar:polarizations": ["VV", "VH"],
            "sat:absolute_orbit": 31835,
            "sat:orbit_state": "ascending",
        }
        common_bbox = [2.0, 50.0, 4.0, 52.0]
        common_geometry = {
            "type": "Polygon",
            "coordinates": [[[2.0, 50.0], [4.0, 50.0], [4.0, 52.0], [2.0, 52.0], [2.0, 50.0]]],
        }
        common_datetime = "2020-03-25T17:24:42Z"

        item_v110 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V110",
                datetime=common_datetime,
                bbox=common_bbox,
                geometry=common_geometry,
                properties={**common_properties, "updated": "2025-07-06T06:06:09.620403Z"},
            )
        )
        item_v120 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V120",
                datetime=common_datetime,
                bbox=common_bbox,
                geometry=common_geometry,
                properties={**common_properties, "updated": "2025-07-06T06:06:09.708329Z"},
            )
        )

        deduplicator = ItemDeduplicator()
        result = deduplicator.deduplicate([item_v110, item_v120])
        assert [r.id for r in result] == [
            "S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V120"
        ]

        # Order of input should not matter
        result = deduplicator.deduplicate([item_v120, item_v110])
        assert [r.id for r in result] == [
            "S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V120"
        ]

    def test_score_property_preference(self):
        """score_property_preference prefers items by property value order, falling back to updated+id."""
        common_props = {"datetime": "2025-11-10T00:00:00Z"}
        item_v110 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-v110", properties={**common_props, "processing:version": 110})
        )
        item_v100 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-v100", properties={**common_props, "processing:version": 100})
        )
        item_unknown = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-unknown", properties={**common_props, "processing:version": 999, "updated": "2099-01-01T00:00:00Z"})
        )
        item_no_version = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-no-version", properties={**common_props, "updated": "2099-01-01T00:00:00Z"})
        )

        deduplicator = ItemDeduplicator(score_property_preference={"processing:version": [110, 100]})

        # v110 is preferred over v100 regardless of input order
        assert [r.id for r in deduplicator.deduplicate([item_v100, item_v110])] == ["item-v110"]
        assert [r.id for r in deduplicator.deduplicate([item_v110, item_v100])] == ["item-v110"]

        # v100 is preferred over an unknown version (not in preference list),
        # even if the unknown version has a later "updated" timestamp
        assert [r.id for r in deduplicator.deduplicate([item_unknown, item_v100])] == ["item-v100"]

        # Property absent → same fallback as unknown value: uses updated+id
        assert [r.id for r in deduplicator.deduplicate([item_no_version, item_v100])] == ["item-v100"]


class _OpenSearchClientDumper:
    """Helper to extract/dump OpenSearchClient contents for testing."""

    # TODO: move this to more general utility module?

    def scala_iterate(self, iterator):
        while iterator.hasNext():
            yield iterator.next()

    def dump_link(self, link: JavaObject) -> dict:
        """dump org.openeo.opensearch.OpenSearchResponses.Link"""
        return {
            # "toString": l.toString(),
            "href": link.href().toString(),
            "title": link.title().get(),
            "bandNames": list(self.scala_iterate(link.bandNames().get().iterator())),
        }

    def dump_feature(self, feature: JavaObject) -> dict:
        """dump org.openeo.opensearch.OpenSearchResponses.Feature"""
        return {
            "id": feature.id(),
            "links": [self.dump_link(link) for link in feature.links()],
        }

    def dump_opensearch_client_features(self, opensearch_client: JavaObject) -> List[dict]:
        """Dump all features from org.openeo.opensearch.OpenSearchClient"""
        feature_iterator = opensearch_client.features().iterator()
        return [self.dump_feature(feature) for feature in self.scala_iterate(feature_iterator)]


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

        dumper = _OpenSearchClientDumper()
        assert dumper.dump_opensearch_client_features(context.opensearch_client) == [
            {
                "id": "item-123",
                "links": expected_links,
            }
        ]

        assert context.metadata.band_names == expected_metadata_band_names
        assert list(context.pyramid_factory.openSearchLinkTitles()) == expected_link_titles

    @pytest.mark.parametrize(
        ["feature_flags", "url", "expected"],
        [
            ({"apply_sentinel2_reflectance_offset": False}, "https://stac.test/foo", False),
            ({"apply_sentinel2_reflectance_offset": True}, "https://stac.test/foo", True),
            ({}, "https://stac.test/foo", False),
            ({}, "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-2-l2a", True),
            ({}, "https://stac.terrascope.be/collections/terrascope-s2-toc-v2", True),
        ],
    )
    def test_get_apply_sentinel2_reflectance_offset(self, feature_flags, url, expected):
        apply_sentinel2_reflectance_offset = _get_apply_sentinel2_reflectance_offset(
            feature_flags=feature_flags, url=url
        )
        assert apply_sentinel2_reflectance_offset == expected

    @pytest.mark.parametrize(
        ["asset", "expected"],
        [
            (pystac.Asset(href="https://stac.test/B02.tiff"), False),
            (
                # Minimal match
                pystac.Asset(
                    href="https://stac.test/B02.tiff",
                    extra_fields={"bands": [{"eo:center_wavelength": 0.475}]},
                ),
                True,
            ),
            (
                # Based on B02 on CDSE https://stac.dataspace.copernicus.eu/v1/collections/sentinel-2-l2a
                pystac.Asset.from_dict(
                    {
                        "href": "s3://test/Sentinel-2/2025/09/01/T31UES_20250901T105041_B02_20m.jp2",
                        "bands": [
                            {
                                "name": "B02",
                                "eo:center_wavelength": 0.493,
                                "eo:full_width_half_max": 0.267,
                                "eo:common_name": "blue",
                            }
                        ],
                        "type": "image/jp2",
                        "roles": ["data", "reflectance", "sampling:downsampled", "gsd:20m"],
                        "raster:scale": 0.0001,
                        "raster:offset": -0.1,
                    }
                ),
                True,
            ),
            (
                # Based on WVP on CDSE https://stac.dataspace.copernicus.eu/v1/collections/sentinel-2-l2a
                pystac.Asset.from_dict(
                    {
                        "href": "s3://test/Sentinel-2/MSI/L2A/2025/09/01/T31UES_20250901T105041_WVP_10m.jp2",
                        "roles": ["data", "gsd:10m"],
                        "title": "Water vapour (WVP) - 10m",
                        "raster:scale": 0.0001,
                        "raster:offset": -0.1,
                    }
                ),
                False,
            ),
            (
                # Based on B02 on Terrascope https://stac.terrascope.be/collections/terrascope-s2-toc-v2
                pystac.Asset.from_dict(
                    {
                        "href": "https://terrascope.test/dl/Sentinel2/TOC_V2/2025/09/01/S2C_20250901T105041_31UES_TOC-B02_10M_V210.tif",
                        "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                        "title": "B02",
                        "roles": ["data"],
                        "raster:scale": 0.0001,
                        "raster:offset": 0.0,
                        "bands": [
                            {
                                "name": "B02",
                                "eo:common_name": "blue",
                                "eo:center_wavelength": 0.49,
                                "eo:full_width_half_max": 0.098,
                            }
                        ],
                    }
                ),
                True,
            ),
        ],
    )
    def test_is_sentinel2_reflectance_asset(self, asset, expected):
        assert _is_sentinel2_reflectance_asset(asset) == expected

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


class TestResolutionTracker:
    def test_empty(self):
        tracker = _ResolutionTracker()
        assert tracker.finest_for() == (set(), None)

    def test_no_keys(self):
        tracker = _ResolutionTracker()
        tracker.track(epsg=4326, res=(1, 2))
        assert tracker.finest_for() == ({4326}, (1, 2))

    def test_basic(self):
        tracker = _ResolutionTracker()
        tracker.track(key="B01", epsg=4326, res=(0.1, 0.1))
        tracker.track(key="B01", epsg=4326, res=(0.2, 0.2))
        tracker.track(key="B02", epsg=4326, res=(0.3, 0.3))

        assert tracker.finest_for(["B01"]) == ({4326}, (0.1, 0.1))
        assert tracker.finest_for(["B02"]) == ({4326}, (0.3, 0.3))
        assert tracker.finest_for(["B01", "B02"]) == ({4326}, (0.1, 0.1))
        assert tracker.finest_for(["B03"]) == (set(), None)
        assert tracker.finest_for(["B01", "B03"]) == ({4326}, (0.1, 0.1))

    def test_multi_epsg(self):
        tracker = _ResolutionTracker()
        tracker.track(key="B01", epsg=4326, res=(0.1, 0.1))
        tracker.track(key="B01", epsg=32631, res=(10, 10))

        assert tracker.finest_for(["B01"]) == ({4326, 32631}, None)

    def test_multi_utm(self):
        tracker = _ResolutionTracker()
        tracker.track(key="B01", epsg=32629, res=(10, 10))
        tracker.track(key="B01", epsg=32631, res=(10, 10))
        tracker.track(key="B02", epsg=32631, res=(20, 20))

        assert tracker.finest_for(["B01"]) == ({32629, 32631}, (10, 10))
        assert tracker.finest_for(["B02"]) == (
            {
                32631,
            },
            (20, 20),
        )
        assert tracker.finest_for(["B01", "B02"]) == (
            {
                32629,
                32631,
            },
            (10, 10),
        )
