import pytest
from openeo_driver.backend import LoadParameters
from openeo_driver.errors import OpenEOApiException
from openeo_driver.utils import EvalEnv

from openeogeotrellis.catalog.layer_catalog import LayerCatalog
from openeogeotrellis.catalog.load_request import PropertyFilters, resolve_load_request
from openeogeotrellis.constants import EVAL_ENV_KEY


def _load_params(**kwargs) -> LoadParameters:
    load_params = LoadParameters()
    load_params.spatial_extent = {}
    load_params.temporal_extent = (None, None)
    for k, v in kwargs.items():
        load_params[k] = v
    return load_params


S2_METADATA = {
    "id": "S2",
    "_vito": {"data_source": {"type": "file-s2", "opensearch_collection_id": "Sentinel2"}},
    "cube:dimensions": {
        "x": {"type": "spatial", "axis": "x", "reference_system": 4326},
        "y": {"type": "spatial", "axis": "y", "reference_system": 4326},
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["B02", "B03"]},
    },
    "summaries": {
        "eo:bands": [
            {"name": "B02", "common_name": "blue"},
            {"name": "B03", "common_name": "green"},
        ]
    },
}


def test_resolve_whole_world_fallback():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert (request.west, request.south, request.east, request.north) == (-180.0, -90, 180, 90)
    assert request.srs == "EPSG:4326"


def test_resolve_missing_bounds_raises_when_required():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    with pytest.raises(OpenEOApiException, match="No spatial filter"):
        resolve_load_request(
            collection_id="S2",
            load_params=_load_params(),
            env=EvalEnv({EVAL_ENV_KEY.REQUIRE_BOUNDS: True}),
            catalog=catalog,
            default_opensearch_endpoint="https://oscars.test",
        )


def test_resolve_band_aliases_and_normalized_band_selection():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(bands=["blue"]),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert request.bands == ["blue"]
    assert request.band_indices == [0]
    assert request.normalized_band_selection == ["B02"]
    assert request.metadata.band_names == ["blue"]


def test_resolve_per_band_gsd_override():
    metadata = {
        **S2_METADATA,
        "summaries": {
            "eo:bands": [
                {"name": "B02", "common_name": "blue", "openeo:gsd": {"unit": "m", "value": [10, 10]}},
                {"name": "B03", "common_name": "green", "openeo:gsd": {"unit": "m", "value": [20, 20]}},
            ]
        },
    }
    catalog = LayerCatalog(all_metadata=[metadata])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert (request.cell_width, request.cell_height) == (10.0, 10.0)


def test_resolve_target_resolution_override():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(target_resolution=(30.0, 30.0)),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert (request.cell_width, request.cell_height) == (30.0, 30.0)


def test_resolve_target_crs_auto42001():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(
            spatial_extent={"west": 5.0, "south": 51.0, "east": 5.1, "north": 51.1, "crs": "EPSG:4326"},
            target_resolution=(10.0, 10.0),
            target_crs={"id": {"authority": "OGC", "code": "Auto42001"}},
        ),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert request.target_epsg == 32631


def test_resolve_sar_backscatter_not_compatible_raises():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    with pytest.raises(OpenEOApiException, match="sar_backscatter"):
        resolve_load_request(
            collection_id="S2",
            load_params=_load_params(sar_backscatter=object()),
            env=EvalEnv(),
            catalog=catalog,
            default_opensearch_endpoint="https://oscars.test",
        )


def test_resolve_merged_by_common_name_delegates_to_catalog():
    upstream = {
        **S2_METADATA,
        "id": "S2_UPSTREAM",
    }
    merged = {
        "id": "S2_MERGED",
        "_vito": {"data_source": {"type": "merged_by_common_name", "merged_collections": ["S2_UPSTREAM"]}},
        "cube:dimensions": S2_METADATA["cube:dimensions"],
    }
    catalog = LayerCatalog(all_metadata=[merged, upstream])
    request = resolve_load_request(
        collection_id="S2_MERGED",
        load_params=_load_params(),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert request.collection_id == "S2_UPSTREAM"
    assert request.source_type == "file-s2"


def test_resolve_opensearch_endpoint_falls_back_to_default():
    catalog = LayerCatalog(all_metadata=[S2_METADATA])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert request.opensearch_endpoint == "https://oscars.test"


def test_resolve_opensearch_endpoint_from_source_info():
    metadata = {
        **S2_METADATA,
        "_vito": {"data_source": {"type": "file-s2", "opensearch_endpoint": "https://custom.test"}},
    }
    catalog = LayerCatalog(all_metadata=[metadata])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert request.opensearch_endpoint == "https://custom.test"


def test_derived_properties():
    metadata = {
        **S2_METADATA,
        "_vito": {"data_source": {"type": "file-s2", "is_utm": True, "catalog_type": "STAC"}},
    }
    catalog = LayerCatalog(all_metadata=[metadata])
    request = resolve_load_request(
        collection_id="S2",
        load_params=_load_params(featureflags={"experimental": True}),
        env=EvalEnv(),
        catalog=catalog,
        default_opensearch_endpoint="https://oscars.test",
    )
    assert request.is_utm is True
    assert request.catalog_type == "STAC"
    assert request.experimental is True


def test_property_filters_flattened_and_conditions():
    eq_cloud_cover = {
        "process_graph": {
            "eq1": {
                "process_id": "eq",
                "arguments": {"x": {"from_parameter": "value"}, "y": 50},
                "result": True,
            }
        }
    }
    property_filters = PropertyFilters.resolve(
        layer_properties={},
        custom_properties={"eo:cloud_cover": eq_cloud_cover},
        env=EvalEnv(),
    )
    assert property_filters.conditions() == {"eo:cloud_cover": {"eq": 50}}
    assert property_filters.flattened() == {"eo:cloud_cover": 50}


def test_property_filters_reflects_later_mutation_of_custom_properties():
    """
    The sentinel-hub PLANETSCOPE pyramid builder does
    `del load_params.properties['byoc_id']` between two property_filters calls
    and relies on the second call no longer returning it (doc 03 SS7.2).
    """
    eq_byoc_id = {
        "process_graph": {
            "eq1": {
                "process_id": "eq",
                "arguments": {"x": {"from_parameter": "value"}, "y": "my-byoc-id"},
                "result": True,
            }
        }
    }
    custom_properties = {"byoc_id": eq_byoc_id}
    property_filters = PropertyFilters.resolve(
        layer_properties={}, custom_properties=custom_properties, env=EvalEnv()
    )
    assert "byoc_id" in property_filters.conditions()
    del custom_properties["byoc_id"]
    assert "byoc_id" not in property_filters.conditions()
