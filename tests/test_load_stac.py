import datetime as dt
import json
from contextlib import nullcontext

import mock
import pytest
from openeo_driver.ProcessGraphDeserializer import DEFAULT_TEMPORAL_EXTENT
from openeo_driver.backend import BatchJobMetadata, BatchJobs, LoadParameters
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.date_math import now_utc
from openeo_driver.utils import EvalEnv

from openeogeotrellis.load_stac import extract_own_job_info, load_stac


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
        ("stac/issue609-api-temporal-bound-exclusive/item01.json",),
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
    ["band_names", "resolution"],
    [
        (["AOT_10m"], 10.0),
        (["WVP_20m"], 20.0),
        (["WVP_60m"], 60.0),
        (["AOT_10m", "WVP_20m"], 10.0)
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

    feature_builder.addLink.assert_any_call(
        "/eodata/Sentinel-2/MSI/L2A_N0500/2020/03/22/S2B_MSIL2A_20200322T074609_N0500_R135_T36NYH_20230612T214223.SAFE/GRANULE/L2A_T36NYH_A015891_20200322T075811/IMG_DATA/R60m/T36NYH_20200322T074609_B01_60m.jp2",
        "B01_60m",
        -1000.0,  # has "raster:scale": 0.0001 and "raster:offset": -0.1
        ["B01_60m"],
    )

    feature_builder.addLink.assert_any_call(
        "/eodata/Sentinel-2/MSI/L2A_N0500/2020/03/22/S2B_MSIL2A_20200322T074609_N0500_R135_T36NYH_20230612T214223.SAFE/GRANULE/L2A_T36NYH_A015891_20200322T075811/IMG_DATA/R20m/T36NYH_20200322T074609_SCL_20m.jp2",
        "SCL_20m",
        0.0,  # has neither "raster:scale" nor "raster:offset"
        ["SCL_20m"],
    )


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
def test_empty_cube_from_non_intersecting_item(requests_mock, test_data, featureflags, env, expectation):
    stac_item_url = "https://stac.test/item.json"

    requests_mock.get(stac_item_url, json=test_data.load_json("stac/item01.json"))

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
