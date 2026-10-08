import pytest
import shapely.geometry
from openeo_driver.backend import LoadParameters
from openeo_driver.errors import InternalException

from openeogeotrellis.catalog.validation import (
    _calculate_rough_area,
    _health_check_extent,
    check_missing_products,
    is_layer_too_large,
    potential_sentinelhub,
)


def test_calculate_rough_area_single_geometry():
    assert _calculate_rough_area([shapely.geometry.box(0, 0, 2, 3)]) == 6


def test_calculate_rough_area_multiple_geometries():
    geoms = [shapely.geometry.box(0, 0, 2, 3), shapely.geometry.box(0, 0, 1, 1)]
    assert _calculate_rough_area(geoms) == 7


def test_calculate_rough_area_multipolygon():
    multi = shapely.geometry.MultiPolygon([shapely.geometry.box(0, 0, 2, 3), shapely.geometry.box(0, 0, 1, 1)])
    assert _calculate_rough_area([multi]) == 7


@pytest.mark.parametrize(
    ["extent", "expected"],
    [
        ({"west": 0, "south": 0, "east": 1, "north": 1, "crs": "EPSG:4326"}, True),
        ({"west": 1, "south": 0, "east": 0, "north": 1, "crs": "EPSG:4326"}, False),  # west > east
        ({"west": -200, "south": 0, "east": 0, "north": 1, "crs": "EPSG:4326"}, False),  # out of lon-lat bounds
        ({"west": 0, "south": -95, "east": 1, "north": 1, "crs": "EPSG:4326"}, False),
        ({"west": 500000, "south": 4000000, "east": 600000, "north": 4100000, "crs": "EPSG:32631"}, True),
        ({"west": -1e9, "south": -1e9, "east": 1e9, "north": 1e9, "crs": "EPSG:32631"}, False),
        ({"west": 500000, "south": 4000000, "east": 600000, "north": 4100000, "crs": "Auto42001"}, True),
    ],
)
def test_health_check_extent(extent, expected):
    assert _health_check_extent(extent) == expected


def _load_params(**kwargs) -> LoadParameters:
    load_params = LoadParameters()
    load_params.spatial_extent = kwargs.pop(
        "spatial_extent", {"west": 0, "south": 0, "east": 1, "north": 1, "crs": "EPSG:4326"}
    )
    for k, v in kwargs.items():
        setattr(load_params, k, v)
    return load_params


def test_is_layer_too_large_small_extent():
    message = is_layer_too_large(
        load_params=_load_params(),
        number_of_temporal_observations=1,
        nr_bands=1,
        cell_width=10,
        cell_height=10,
        native_crs="EPSG:4326",
        threshold_pixels=10**11,
    )
    assert message is None


def test_is_layer_too_large_exceeds_threshold():
    message = is_layer_too_large(
        load_params=_load_params(
            spatial_extent={"west": 0, "south": 0, "east": 10, "north": 10, "crs": "EPSG:4326"}
        ),
        number_of_temporal_observations=1000,
        nr_bands=10,
        cell_width=0.00001,
        cell_height=0.00001,
        native_crs="EPSG:4326",
        threshold_pixels=10**6,
    )
    assert message is not None
    assert "too large" in message


def test_is_layer_too_large_invalid_extent():
    message = is_layer_too_large(
        load_params=_load_params(spatial_extent={"west": 10, "south": 0, "east": 0, "north": 10, "crs": "EPSG:4326"}),
        number_of_temporal_observations=1,
        nr_bands=1,
        cell_width=10,
        cell_height=10,
        native_crs="EPSG:4326",
    )
    assert message is not None
    assert "Unsupported spatial extent" in message


def test_is_layer_too_large_sync_job_pixel_cap():
    message = is_layer_too_large(
        load_params=_load_params(
            spatial_extent={"west": 0, "south": 0, "east": 10, "north": 10, "crs": "EPSG:4326"}
        ),
        number_of_temporal_observations=1,
        nr_bands=1,
        cell_width=0.0001,
        cell_height=0.0001,
        native_crs="EPSG:4326",
        threshold_pixels=10**20,
        sync_job=True,
    )
    assert message is not None
    assert "too large for a sync job" in message


class _FakeCatalog:
    def __init__(self, metadata_by_id: dict, merged: dict = None):
        self._metadata_by_id = metadata_by_id
        self._merged = merged or {}

    def get_collection_metadata(self, collection_id):
        return self._metadata_by_id[collection_id]


def test_potential_sentinelhub_true():
    catalog = _FakeCatalog({"S1": {"id": "S1", "_vito": {"data_source": {"provider:backend": "sentinelhub"}}}})
    assert potential_sentinelhub(catalog, "S1") is True


def test_potential_sentinelhub_false():
    catalog = _FakeCatalog({"S1": {"id": "S1", "_vito": {"data_source": {"type": "file-s2"}}}})
    assert potential_sentinelhub(catalog, "S1") is False


def test_potential_sentinelhub_via_merged_collections():
    catalog = _FakeCatalog({
        "MERGED": {
            "id": "MERGED",
            "_vito": {"data_source": {"type": "merged_by_common_name", "merged_collections": ["S1"]}},
        },
        "S1": {"id": "S1", "_vito": {"data_source": {"provider:backend": "sentinelhub"}}},
    })
    assert potential_sentinelhub(catalog, "MERGED") is True


def test_check_missing_products_no_check_data():
    result = check_missing_products(
        collection_metadata={"id": "FOO"},
        temporal_extent=("2020-01-01", "2020-02-01"),
        spatial_extent={"west": 0, "south": 0, "east": 1, "north": 1},
    )
    assert result is None


@pytest.mark.parametrize("method", ["creo", "terrascope"])
def test_check_missing_products_unsupported_method_returns_empty(method):
    result = check_missing_products(
        collection_metadata={"id": "FOO", "_vito": {"data_source": {"check_missing_products": {"method": method}}}},
        temporal_extent=("2020-01-01", "2020-02-01"),
        spatial_extent={"west": 0, "south": 0, "east": 1, "north": 1},
    )
    assert result == []


def test_check_missing_products_invalid_method_raises():
    with pytest.raises(InternalException):
        check_missing_products(
            collection_metadata={
                "id": "FOO",
                "_vito": {"data_source": {"check_missing_products": {"method": "bogus"}}},
            },
            temporal_extent=("2020-01-01", "2020-02-01"),
            spatial_extent={"west": 0, "south": 0, "east": 1, "north": 1},
        )
