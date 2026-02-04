"""
Integration test for S1BackscatterOrfeo STAC API queries.

This integration test verifies that S1BackscatterOrfeo correctly queries the live
Copernicus Dataspace STAC API and that the array_contains filter properly matches all
Sentinel-1 product types: IW_GRDH_1S (S1A), IW_GRDH_1S_B (S1B), and IW_GRDH_1S_C (S1C).

NOTE: This test requires network access to https://stac.dataspace.copernicus.eu/
and may be slow. It can be skipped with: pytest -m "not integration"
"""

import logging
import pytest
from openeogeotrellis.collections.s1backscatter_orfeo import S1BackscatterOrfeo
from openeogeotrellis.load_stac import construct_item_collection, _spatiotemporal_extent_from_load_params

_log = logging.getLogger(__name__)


@pytest.mark.integration
def test_s1backscatter_stac_api_queries_multiple_product_types():
    """
    Integration test: Verify that S1BackscatterOrfeo queries the live STAC API correctly
    and returns both Sentinel-1A (IW_GRDH_1S) and Sentinel-1B (IW_GRDH_1S_B) products.
    Sentinel-1C (IW_GRDH_1S_C) products only appear starting 2025.

    This test:
    1. Uses the default filter properties from S1BackscatterOrfeo
    2. Queries the live Copernicus Dataspace STAC API
    3. Verifies that both product types are returned (when available)
    4. Confirms the array_contains filter is working correctly

    Test parameters:
    - Date range: December 2021 (known to have good S1 coverage)
    - Area: Belgium (3.0°E to 4.0°E, 50.0°N to 51.0°N)
    """
    # Test parameters
    start_date = "2021-12-01"
    end_date = "2021-12-31"
    bbox = (3.0, 50.0, 4.0, 51.0)  # Belgium area

    _log.info("="*80)
    _log.info("Integration Test: S1BackscatterOrfeo STAC API Query")
    _log.info(f"Date range: {start_date} to {end_date}")
    _log.info(f"Area: Belgium ({bbox})")
    _log.info("="*80)

    # Build filter properties using the default S1BackscatterOrfeo method
    s1_backscatter = S1BackscatterOrfeo()
    filter_properties = s1_backscatter._build_filter_properties(
        extra_properties={},
        use_stac_client=True
    )

    _log.info("Default filter properties:")
    for key, value in filter_properties.items():
        _log.info(f"  {key}: {value}")

    # Verify the default includes all product types as an array
    assert "product:type" in filter_properties
    assert isinstance(filter_properties["product:type"], list)
    assert "IW_GRDH_1S" in filter_properties["product:type"]
    assert "IW_GRDH_1S_B" in filter_properties["product:type"]
    assert "IW_GRDH_1S_C" in filter_properties["product:type"]

    # Convert to property filter process graph
    property_filter_pg_map = s1_backscatter._filter_properties_to_pg_map(filter_properties)

    _log.info("Property filter process graph:")
    pg = property_filter_pg_map.get("product:type", {}).get("process_graph", {})
    for node_name, node in pg.items():
        _log.info(f"  {node_name}:")
        _log.info(f"    process_id: {node.get('process_id')}")
        _log.info(f"    arguments: {node.get('arguments')}")

    # Verify array_contains is used for the product:type filter
    assert "array_contains" in pg
    assert pg["array_contains"]["process_id"] == "array_contains"
    assert pg["array_contains"]["arguments"]["data"] == ["IW_GRDH_1S", "IW_GRDH_1S_B", "IW_GRDH_1S_C"]

    # Build spatiotemporal extent
    spatial_extent = {"west": bbox[0], "south": bbox[1], "east": bbox[2], "north": bbox[3]}
    temporal_extent = (f"{start_date}T00:00:00Z", f"{end_date}T23:59:59Z")

    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent
    )

    _log.info("Querying live STAC API...")
    _log.info("  URL: https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd")
    _log.info(f"  Temporal: {temporal_extent}")
    _log.info(f"  Spatial: {spatial_extent}")

    # Query the live STAC API
    url = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-1-grd"

    item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
        url=url,
        spatiotemporal_extent=spatiotemporal_extent,
        property_filter_pg_map=property_filter_pg_map,
    )

    _log.info("Query successful!")
    _log.info(f"  Total items returned: {len(item_collection.items)}")

    # Verify we got results
    assert len(item_collection.items) > 0, "Expected to find Sentinel-1 items for Belgium in Dec 2021"

    # Analyze the results
    product_types = {}
    platforms = {}

    for item in item_collection.items:
        # Get product type
        product_type = item.properties.get("product:type", "unknown")
        product_types[product_type] = product_types.get(product_type, 0) + 1

        # Get platform
        platform = item.properties.get("platform", "unknown")
        platforms[platform] = platforms.get(platform, 0) + 1

    _log.info("Product type distribution:")
    for pt, count in sorted(product_types.items()):
        _log.info(f"  {pt}: {count} items")

    _log.info("Platform distribution:")
    for pf, count in sorted(platforms.items()):
        _log.info(f"  {pf}: {count} items")

    # Verify we got the expected product types
    has_iw_grdh_1s = "IW_GRDH_1S" in product_types
    has_iw_grdh_1s_b = "IW_GRDH_1S_B" in product_types
    has_iw_grdh_1s_c = "IW_GRDH_1S_C" in product_types

    _log.info("="*80)
    _log.info("VERIFICATION RESULTS:")
    _log.info("="*80)

    # Assert that we got IW_GRDH_1S and IW_GRDH_1S_B (this is the key validation)
    # IW_GRDH_1S_C should NOT be found for Dec 2021 (S1C wasn't operational yet)
    assert has_iw_grdh_1s, "Expected to find IW_GRDH_1S (Sentinel-1A) products"
    assert has_iw_grdh_1s_b, "Expected to find IW_GRDH_1S_B (Sentinel-1B) products"
    assert not has_iw_grdh_1s_c, "Expected NOT to find IW_GRDH_1S_C (Sentinel-1C was not operational in Dec 2021)"

    _log.info("SUCCESS: Both IW_GRDH_1S and IW_GRDH_1S_B products found!")
    _log.info(f"  - IW_GRDH_1S (Sentinel-1A): {product_types.get('IW_GRDH_1S', 0)} items")
    _log.info(f"  - IW_GRDH_1S_B (Sentinel-1B): {product_types.get('IW_GRDH_1S_B', 0)} items")
    _log.info(f"  - IW_GRDH_1S_C (Sentinel-1C): {product_types.get('IW_GRDH_1S_C', 0)} items (expected 0 for Dec 2021)")
    _log.info("  The array_contains filter is working correctly with the live STAC API.")
    _log.info("="*80)

    # Verify only expected product types are returned
    unexpected_types = set(product_types.keys()) - {"IW_GRDH_1S", "IW_GRDH_1S_B", "IW_GRDH_1S_C"}
    assert not unexpected_types, f"Unexpected product types returned: {unexpected_types}"

    # Verify platform matches product type
    assert platforms.get("sentinel-1a", 0) > 0, "Expected Sentinel-1A items"
    assert platforms.get("sentinel-1b", 0) > 0, "Expected Sentinel-1B items"
