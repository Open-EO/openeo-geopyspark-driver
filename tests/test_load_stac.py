import dirty_equals
import pystac
from contextlib import nullcontext

import mock
import pytest

import openeo.metadata
import responses
from openeo.testing.stac import StacDummyBuilder
from openeo_driver.ProcessGraphDeserializer import DEFAULT_TEMPORAL_EXTENT
from openeo_driver.backend import BatchJobMetadata, BatchJobs, LoadParameters
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.date_math import now_utc
from openeo_driver.utils import EvalEnv

from openeogeotrellis.load_stac import (
    extract_own_job_info,
    load_stac,
    _StacMetadataParser,
    _is_supported_raster_mime_type,
    _is_band_asset,
    _supports_item_search,
    _get_proj_metadata,
)


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
