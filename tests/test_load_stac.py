from typing import Iterator

import dirty_equals
import pystac
import pystac_client
from contextlib import nullcontext
import datetime
from unittest import mock

import pytest

import openeo.metadata
import responses
from openeo.testing.stac import StacDummyBuilder
from openeo_driver.ProcessGraphDeserializer import DEFAULT_TEMPORAL_EXTENT
from openeo_driver.backend import BatchJobMetadata, BatchJobs, LoadParameters
from openeo_driver.dummy.dummy_backend import DummyBatchJobs
from openeo_driver.errors import OpenEOApiException
from openeo_driver.users import User
from openeo_driver.util.date_math import now_utc
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv
from openeogeotrellis.backend import GpsBatchJobs
from openeogeotrellis.job_registry import InMemoryJobRegistry

from openeogeotrellis.load_stac import (
    extract_own_job_info,
    load_stac,
    _StacMetadataParser,
    _is_supported_raster_mime_type,
    _is_band_asset,
    _supports_item_search,
    _get_proj_metadata,
    _TemporalExtent,
    _SpatioTemporalExtent,
    _SpatialExtent,
    PropertyFilter,
    ItemCollection,
)
from openeogeotrellis.testing import gps_config_overrides, DummyStacApiServer


@pytest.mark.parametrize("url, user_id, job_info_id",
                         [
                             ("https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results", 'alice', 'j-20240201abc123'),
                             ("https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results", 'bob', None),
                             ("https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results/N2Q1MjMzODEzNzRiNjJlNmYyYWFkMWYyZjlmYjZlZGRmNjI0ZDM4MmE4ZjcxZGI2Z/095be1c7a37baf63b2044?expires=1707382334", 'alice', None),
                             ("https://oeo.net/openeo/1.1/jobs/j-20240201abc123/results/N2Q1MjMzODEzNzRiNjJlNmYyYWFkMWYyZjlmYjZlZGRmNjI0ZDM4MmE4ZjcxZGI2Z/095be1c7a37baf63b2044?expires=1707382334", 'bob', None),
                             ("https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a", 'alice', None)
                         ])
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
                        "y": {"from_parameter": "tile_id"}
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
            override_band_names=None,
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
    ["enable_by_catalog", "enable_by_eval_env"],
    [
        (True, {}),
        (False, {"load_stac_apply_lcfm_improvements": True}),
    ],
)
@pytest.mark.parametrize(
    ["band_names", "resolution", "expected_add_links"],
    [
        (
            ["AOT_10m"],
            10.0,
            [(dirty_equals.IsStr(regex=".*_AOT_10m.jp2"), "AOT_10m", -1000.0, ["AOT_10m"])],
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
            [(dirty_equals.IsStr(regex=".*_WVP_20m.jp2"), "WVP_20m", -1000.0, ["WVP_20m"])],
        ),
        (
            ["WVP_60m"],
            60.0,
            [(dirty_equals.IsStr(regex=".*_WVP_60m.jp2"), "WVP_60m", -1000.0, ["WVP_60m"])],
        ),
        (
            ["AOT_10m", "WVP_20m"],
            10.0,
            [
                (dirty_equals.IsStr(regex=".*_AOT_10m.jp2"), "AOT_10m", -1000.0, ["AOT_10m"]),
                (dirty_equals.IsStr(regex=".*_WVP_20m.jp2"), "WVP_20m", -1000.0, ["WVP_20m"]),
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
def test_lcfm_improvements(  # resolution and offset behind a feature flag; alphabetical head tags are tested elsewhere
    requests_mock,
    test_data,
    jvm_mock,
    band_names,
    resolution,
    enable_by_catalog,
    enable_by_eval_env,
    expected_add_links,
):
    stac_api_root_url = "https://stac.test"
    stac_collection_url = f"{stac_api_root_url}/collections/collection"

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
        env=EvalEnv(dict(enable_by_eval_env, pyramid_levels="highest")),
        layer_properties={},
        batch_jobs=None,
        apply_lcfm_improvements=enable_by_catalog,
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
                temporal_extent=DEFAULT_TEMPORAL_EXTENT,
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
                temporal_extent=DEFAULT_TEMPORAL_EXTENT,
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
        responses.post(stac_search_url, status=500, body="some transient error") for _ in range(4)  # does 4 attempts
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

    for resp in search_transient_error_resps:
        assert resp.call_count == 1


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


def test_get_proj_metadata_minimal():
    asset = pystac.Asset(href="https://example.com/asset.tif")
    item = pystac.Item.from_dict(StacDummyBuilder.item())
    assert _get_proj_metadata(asset, item=item) == (None, None, None)


def test_get_proj_metadata_from_asset():
    asset = pystac.Asset(
        href="https://example.com/asset.tif",
        extra_fields={"proj:epsg": 32631, "proj:shape": [12, 34], "proj:bbox": [12, 34, 56, 78]},
    )
    item = pystac.Item.from_dict(StacDummyBuilder.item())
    assert _get_proj_metadata(asset, item=item) == (32631, (12.0, 34.0, 56.0, 78.0), (12, 34))


class TestTemporalExtent:
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
            # TODO?
            # (
            #     {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
            #     "...",
            # ),
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
                        "datetime": "2025-09-04T10:00:00Z",
                        "roles": ["data"],
                        "href": "https://data.test/asset1.tif",
                        "bands": [{"name": "red"}],
                    },
                    "asset2": {
                        "bbox": [24, 34, 28, 38],
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
                    "stac_version": "1.0.0",
                    "id": "asset1",
                    "assets": {"asset1": {"eo:bands": [{"name": "red"}], "href": "https://data.test/asset1.tif"}},
                    "bbox": [20, 30, 25, 35],
                    "properties": {"datetime": "2025-09-04T10:00:00Z"},
                }
            ),
            2: dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
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
    def dummy_stac_api(self) -> Iterator[str]:
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

        with dummy_server.serve() as root_url:
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

    @pytest.mark.parametrize("use_filter_extension", ["cql2-json", "cql2-text"])
    def test_from_stac_api_property_filter(self, dummy_stac_api, use_filter_extension):
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
