import pytest
from openeo_driver.backend import LoadParameters
from openeo_driver.errors import OpenEOApiException

from openeogeotrellis.catalog.collection_metadata import GeopysparkCubeMetadata
from openeogeotrellis.catalog.layer_catalog import LayerCatalog


def _load_params(**kwargs) -> LoadParameters:
    load_params = LoadParameters()
    for k, v in kwargs.items():
        setattr(load_params, k, v)
    return load_params


def test_native_crs_epsg_int():
    metadata = GeopysparkCubeMetadata({
        "id": "S1",
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x", "reference_system": 4326},
            "y": {"type": "spatial", "axis": "y", "reference_system": 4326},
        },
    })
    assert LayerCatalog(all_metadata=[]).native_crs(metadata) == "EPSG:4326"


def test_native_crs_auto_utm():
    metadata = GeopysparkCubeMetadata({
        "id": "S1",
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x", "reference_system": {"id": {"authority": "OGC", "code": "Auto42001"}}},
            "y": {"type": "spatial", "axis": "y"},
        },
    })
    assert LayerCatalog(all_metadata=[]).native_crs(metadata) == "UTM"


def test_native_crs_no_dimensions_defaults_to_utm():
    metadata = GeopysparkCubeMetadata({"id": "S1"})
    assert LayerCatalog(all_metadata=[]).native_crs(metadata) == "UTM"


def test_derive_temporal_extent_no_constraint_uses_catalog_extent():
    catalog = LayerCatalog(all_metadata=[
        {"id": "S1", "extent": {"temporal": {"interval": [["2020-01-01", "2020-12-31"]]}}}
    ])
    assert catalog.derive_temporal_extent("S1", _load_params(temporal_extent=None)) == ["2020-01-01", "2020-12-31"]


def test_derive_temporal_extent_intersects_with_constraint():
    catalog = LayerCatalog(all_metadata=[
        {"id": "S1", "extent": {"temporal": {"interval": [["2020-01-01", "2020-12-31"]]}}}
    ])
    result = catalog.derive_temporal_extent("S1", _load_params(temporal_extent=("2020-06-01", "2021-01-01")))
    assert result == ("2020-06-01", "2020-12-31")


def test_estimate_number_of_temporal_observations_singular_time_step():
    catalog = LayerCatalog(all_metadata=[
        {
            "id": "S1",
            "extent": {"temporal": {"interval": [["2020-01-01", "2020-12-31"]]}},
            "_vito": {"data_source": {"consider_as_singular_time_step": True}},
        }
    ])
    assert catalog.estimate_number_of_temporal_observations("S1", _load_params(temporal_extent=None)) == 1


def test_estimate_number_of_temporal_observations_from_cube_dimensions_step():
    catalog = LayerCatalog(all_metadata=[
        {
            "id": "S1",
            "extent": {"temporal": {"interval": [["2020-01-01", "2020-01-31"]]}},
            "cube:dimensions": {"t": {"type": "temporal", "step": "P10D"}},
        }
    ])
    assert catalog.estimate_number_of_temporal_observations("S1", _load_params(temporal_extent=None)) == 3


def test_resolve_merged_by_common_name_picks_highest_priority_available():
    catalog = LayerCatalog(all_metadata=[
        {
            "id": "MERGED",
            "_vito": {"data_source": {"merged_collections": ["LOW", "HIGH"]}},
        },
        {
            "id": "LOW",
            "_vito": {"data_source": {"provider:backend": "low", "common_name_priority": 1}},
        },
        {
            "id": "HIGH",
            "_vito": {"data_source": {"provider:backend": "high", "common_name_priority": 2}},
        },
    ])
    metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata("MERGED"))
    resolved = catalog.resolve_merged_by_common_name(
        "MERGED", metadata, _load_params(properties={}), temporal_extent=("2020-01-01", "2020-02-01"),
        spatial_extent={"west": 0, "south": 0, "east": 1, "north": 1},
    )
    assert resolved.provider_backend() == "high"


def test_resolve_merged_by_common_name_no_fitting_provider_raises():
    catalog = LayerCatalog(all_metadata=[
        {"id": "MERGED", "_vito": {"data_source": {"merged_collections": []}}},
    ])
    metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata("MERGED"))
    with pytest.raises(OpenEOApiException):
        catalog.resolve_merged_by_common_name(
            "MERGED", metadata, _load_params(properties={}), temporal_extent=("2020-01-01", "2020-02-01"),
            spatial_extent={"west": 0, "south": 0, "east": 1, "north": 1},
        )


def test_get_collection_queryables_stac_redirect():
    catalog = LayerCatalog(all_metadata=[
        {"id": "STAC1", "_vito": {"data_source": {"type": "stac", "url": "https://example.test/collections/STAC1"}}}
    ])
    response = catalog.get_collection_queryables("STAC1")
    assert response.status_code == 302
    assert response.location == "https://example.test/collections/STAC1/queryables"
