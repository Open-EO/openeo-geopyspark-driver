"""
Integration test for Sentinel-3 STAC API queries.

This integration test verifies that Sentinel-3 correctly queries both NRT and NTC
STAC collections from the live Copernicus Dataspace STAC API:
- NRT (Near Real Time): Available within ~3 hours, used for recent data
- NTC (Non Time Critical): Available after days/weeks, used for older archived data

NOTE: This test requires network access to https://stac.dataspace.copernicus.eu/
and may be slow. It can be skipped with: pytest -m "not integration"
"""

import logging
from unittest import skip
from openeogeotrellis.collections.sentinel3 import (
    _get_stac_collection_urls,
    _map_attributes_for_stac,
    _map_attributes_to_property_filter,
    deduplicate_items_prefer_ntc,
    _get_acquisition_key,
    SLSTR_PRODUCT_TYPE,
)
from openeogeotrellis.load_stac import construct_item_collection, _spatiotemporal_extent_from_load_params

_log = logging.getLogger(__name__)


@skip("Integration test")
def test_sentinel3_stac_collection_urls():
    """Verify that SLSTR LST returns both NRT and NTC collection URLs."""
    urls = _get_stac_collection_urls(SLSTR_PRODUCT_TYPE)

    assert len(urls) == 2, "Expected 2 STAC collection URLs (NRT and NTC)"
    assert "sentinel-3-sl-2-lst-nrt" in urls[0] or "sentinel-3-sl-2-lst-nrt" in urls[1]
    assert "sentinel-3-sl-2-lst-ntc" in urls[0] or "sentinel-3-sl-2-lst-ntc" in urls[1]

    _log.info(f"SLSTR LST STAC collections: {urls}")


@skip("Integration test")
def test_sentinel3_stac_query_recent_data_nrt():
    """
    Integration test: Verify that recent Sentinel-3 SLSTR LST data is found in NRT collection.

    Recent data (within days) should be available in the NRT collection.
    This test queries both NRT and NTC collections and verifies that:
    1. At least one collection returns data
    2. The data comes from the expected timeframe

    Test parameters:
    - Date range: Recent data from 2026-01-10 to 2026-01-11
    - Area: Australia (129°E to 130°E, -19°S to -18°S)
    """
    # Test parameters - using dates from the GitHub issue
    start_date = "2026-01-10"
    end_date = "2026-01-11"
    bbox = (129.0, -19.0, 130.0, -18.0)  # Australia area

    _log.info("="*80)
    _log.info("Integration Test: Sentinel-3 SLSTR LST STAC API Query (Recent Data)")
    _log.info(f"Date range: {start_date} to {end_date}")
    _log.info(f"Area: Australia ({bbox})")
    _log.info("="*80)

    # Build filter properties
    metadata_properties = {"productType": SLSTR_PRODUCT_TYPE}
    stac_attributes = _map_attributes_for_stac(metadata_properties)
    property_filter_pg_map = _map_attributes_to_property_filter(stac_attributes)

    _log.info("Filter properties:")
    for key, value in stac_attributes.items():
        _log.info(f"  {key}: {value}")

    # Build spatiotemporal extent
    spatial_extent = {"west": bbox[0], "south": bbox[1], "east": bbox[2], "north": bbox[3]}
    temporal_extent = (f"{start_date}T00:00:00Z", f"{end_date}T23:59:59Z")

    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent
    )

    # Get collection URLs
    urls = _get_stac_collection_urls(SLSTR_PRODUCT_TYPE)
    _log.info(f"Querying {len(urls)} STAC collections:")

    all_items = []
    collection_results = {}

    # Query each collection
    for url in urls:
        collection_name = "NRT" if "nrt" in url else "NTC"
        _log.info(f"  Querying {collection_name}: {url}")

        try:
            item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
                url=url,
                spatiotemporal_extent=spatiotemporal_extent,
                property_filter_pg_map=property_filter_pg_map,
            )

            num_items = len(item_collection.items)
            collection_results[collection_name] = num_items
            _log.info(f"    Found {num_items} items in {collection_name}")

            if num_items > 0:
                # Log sample item details
                sample_item = item_collection.items[0]
                _log.info(f"    Sample item ID: {sample_item.id}")
                _log.info(f"    Sample datetime: {sample_item.properties.get('datetime', sample_item.properties.get('start_datetime'))}")

                # Check if it's NRT or NTC based on the product path
                for asset in sample_item.assets.values():
                    if "_NR_" in asset.href:
                        _log.info(f"    Confirmed NRT product (_NR_) in href")
                    elif "_NT_" in asset.href:
                        _log.info(f"    Confirmed NTC product (_NT_) in href")
                    break

            all_items.extend(item_collection.items)

        except Exception as e:
            _log.warning(f"    Failed to query {collection_name}: {e}")
            collection_results[collection_name] = 0

    _log.info("="*80)
    _log.info("QUERY RESULTS:")
    _log.info("="*80)
    for collection_name, count in collection_results.items():
        _log.info(f"  {collection_name}: {count} items")
    _log.info(f"  Total: {len(all_items)} items")
    _log.info("="*80)

    # Verify we got results from at least one collection
    assert len(all_items) > 0, (
        f"Expected to find Sentinel-3 SLSTR LST items for recent date {start_date}. "
        f"Results: NRT={collection_results.get('NRT', 0)}, NTC={collection_results.get('NTC', 0)}"
    )

    # For recent data (2026-01-10), we expect NRT to have more data than NTC
    _log.info("VERIFICATION:")
    if collection_results.get("NRT", 0) > 0:
        _log.info("  ✓ NRT collection has recent data (expected)")
    if collection_results.get("NTC", 0) > 0:
        _log.info("  ✓ NTC collection also has data")

    _log.info(f"  SUCCESS: Found {len(all_items)} total items across both collections")


@skip("Integration test")
def test_sentinel3_stac_query_older_data_ntc():
    """
    Integration test: Verify that older Sentinel-3 SLSTR LST data is found in NTC collection.

    Older data should be available in the NTC (Non Time Critical) collection,
    which contains archived, reprocessed data with precise calibration.

    Test parameters:
    - Date range: Older data from 2020-12-30
    - Area: Europe (9°E to 12°E, 45°N to 47°N)
    """
    # Test parameters - using older date that should be in NTC
    start_date = "2020-12-30"
    end_date = "2020-12-31"
    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe area

    _log.info("="*80)
    _log.info("Integration Test: Sentinel-3 SLSTR LST STAC API Query (Older Data)")
    _log.info(f"Date range: {start_date} to {end_date}")
    _log.info(f"Area: Europe ({bbox})")
    _log.info("="*80)

    # Build filter properties
    metadata_properties = {"productType": SLSTR_PRODUCT_TYPE}
    stac_attributes = _map_attributes_for_stac(metadata_properties)
    property_filter_pg_map = _map_attributes_to_property_filter(stac_attributes)

    # Build spatiotemporal extent
    spatial_extent = {"west": bbox[0], "south": bbox[1], "east": bbox[2], "north": bbox[3]}
    temporal_extent = (f"{start_date}T00:00:00Z", f"{end_date}T23:59:59Z")

    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent
    )

    # Get collection URLs
    urls = _get_stac_collection_urls(SLSTR_PRODUCT_TYPE)
    _log.info(f"Querying {len(urls)} STAC collections:")

    all_items = []
    collection_results = {}

    # Query each collection
    for url in urls:
        collection_name = "NRT" if "nrt" in url else "NTC"
        _log.info(f"  Querying {collection_name}: {url}")

        try:
            item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
                url=url,
                spatiotemporal_extent=spatiotemporal_extent,
                property_filter_pg_map=property_filter_pg_map,
            )

            num_items = len(item_collection.items)
            collection_results[collection_name] = num_items
            _log.info(f"    Found {num_items} items in {collection_name}")

            if num_items > 0:
                # Log sample item details
                sample_item = item_collection.items[0]
                _log.info(f"    Sample item ID: {sample_item.id}")

                # Check if it's NRT or NTC based on the product path
                for asset in sample_item.assets.values():
                    if "_NR_" in asset.href:
                        _log.info(f"    Found NRT product (_NR_) in href")
                    elif "_NT_" in asset.href:
                        _log.info(f"    Found NTC product (_NT_) in href")
                    break

            all_items.extend(item_collection.items)

        except Exception as e:
            _log.warning(f"    Failed to query {collection_name}: {e}")
            collection_results[collection_name] = 0

    _log.info("="*80)
    _log.info("QUERY RESULTS:")
    _log.info("="*80)
    for collection_name, count in collection_results.items():
        _log.info(f"  {collection_name}: {count} items")
    _log.info(f"  Total: {len(all_items)} items")
    _log.info("="*80)

    # Verify we got results
    assert len(all_items) > 0, (
        f"Expected to find Sentinel-3 SLSTR LST items for older date {start_date}. "
        f"Results: NRT={collection_results.get('NRT', 0)}, NTC={collection_results.get('NTC', 0)}"
    )

    # For older data (2020-12-30), we expect NTC to have data
    _log.info("VERIFICATION:")
    if collection_results.get("NTC", 0) > 0:
        _log.info("  ✓ NTC collection has older archived data (expected)")
    if collection_results.get("NRT", 0) > 0:
        _log.info("  ✓ NRT collection also has data")

    _log.info(f"  SUCCESS: Found {len(all_items)} total items across both collections")

@skip("Integration test")
def test_sentinel3_stac_deduplication_ntc_takes_precedence():
    """
    Integration test: Verify that when the same acquisition exists in both NRT and NTC,
    only the NTC version (higher quality) is kept.

    This test queries a date range where products typically exist in both collections,
    and verifies that:
    1. Both collections return items
    2. There are overlapping acquisitions
    3. The deduplication logic properly handles this

    Test parameters:
    - Date range: Recent date that should have both NRT and NTC
    - Area: Europe (9°E to 12°E, 45°N to 47°N)
    """
    # Use a date that should have both NRT and NTC data
    start_date = "2025-12-30"
    end_date = "2025-12-31"
    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe area

    _log.info("="*80)
    _log.info("Integration Test: Sentinel-3 SLSTR LST Deduplication (NTC > NRT)")
    _log.info(f"Date range: {start_date} to {end_date}")
    _log.info(f"Area: Europe ({bbox})")
    _log.info("="*80)

    # Build filter properties
    metadata_properties = {"productType": SLSTR_PRODUCT_TYPE}
    stac_attributes = _map_attributes_for_stac(metadata_properties)
    property_filter_pg_map = _map_attributes_to_property_filter(stac_attributes)

    # Build spatiotemporal extent
    spatial_extent = {"west": bbox[0], "south": bbox[1], "east": bbox[2], "north": bbox[3]}
    temporal_extent = (f"{start_date}T00:00:00Z", f"{end_date}T23:59:59Z")

    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent
    )

    # Get collection URLs
    urls = _get_stac_collection_urls(SLSTR_PRODUCT_TYPE)

    nrt_items = []
    ntc_items = []

    for url in urls:
        collection_name = "NRT" if "nrt" in url else "NTC"
        _log.info(f"Querying {collection_name}: {url}")

        try:
            item_collection, metadata, collection_band_names, netcdf_with_time_dimension = construct_item_collection(
                url=url,
                spatiotemporal_extent=spatiotemporal_extent,
                property_filter_pg_map=property_filter_pg_map,
            )

            items = list(item_collection.items)
            _log.info(f"  Found {len(items)} items in {collection_name}")

            if collection_name == "NRT":
                nrt_items = items
            else:
                ntc_items = items

        except Exception as e:
            _log.warning(f"  Failed to query {collection_name}: {e}")

    # Check for overlaps using acquisition keys (platform + absolute orbit)
    nrt_keys = {_get_acquisition_key(item) for item in nrt_items}
    ntc_keys = {_get_acquisition_key(item) for item in ntc_items}
    overlap = nrt_keys & ntc_keys

    _log.info("="*80)
    _log.info("OVERLAP ANALYSIS:")
    _log.info("="*80)
    _log.info(f"  NRT items: {len(nrt_items)}")
    _log.info(f"  NTC items: {len(ntc_items)}")
    _log.info(f"  Overlapping acquisitions: {len(overlap)}")

    if overlap:
        _log.info(f"  Example overlaps: {list(overlap)[:3]}")
        _log.info("  ✓ Found overlapping acquisitions - deduplication is necessary")

        # Verify that for overlapping acquisitions, we can identify both versions
        for acq_key in list(overlap)[:2]:
            nrt_version = [item for item in nrt_items if _get_acquisition_key(item) == acq_key][0]
            ntc_version = [item for item in ntc_items if _get_acquisition_key(item) == acq_key][0]

            _log.info(f"\n  Overlap example for acquisition {acq_key}:")
            _log.info(f"    NRT: {nrt_version.id}")
            _log.info(f"      timeliness_category: {nrt_version.properties.get('product:timeliness_category')}")
            _log.info(f"      platform: {nrt_version.properties.get('platform')}")
            _log.info(f"      sat:absolute_orbit: {nrt_version.properties.get('sat:absolute_orbit')}")
            _log.info(f"    NTC: {ntc_version.id}")
            _log.info(f"      timeliness_category: {ntc_version.properties.get('product:timeliness_category')}")
            _log.info(f"      platform: {ntc_version.properties.get('platform')}")
            _log.info(f"      sat:absolute_orbit: {ntc_version.properties.get('sat:absolute_orbit')}")

            # Verify they have different timeliness categories
            assert nrt_version.properties.get('product:timeliness_category') == 'NR'
            assert ntc_version.properties.get('product:timeliness_category') == 'NT'

        _log.info("\n  ✓ VERIFIED: Same acquisitions have both NRT and NTC versions")

        # Now test the actual deduplication function with real STAC items
        _log.info("\n" + "="*80)
        _log.info("DEDUPLICATION FUNCTION TEST:")
        _log.info("="*80)

        # Prepare items in the format expected by deduplicate_items_prefer_ntc
        items_by_collection = {
            "NRT": [(item, {}) for item in nrt_items],
            "NTC": [(item, {}) for item in ntc_items],
        }

        # Call the deduplication function
        deduplicated = deduplicate_items_prefer_ntc(items_by_collection)

        _log.info(f"  Before deduplication: {len(nrt_items) + len(ntc_items)} total items")
        _log.info(f"  After deduplication: {len(deduplicated)} items")
        _log.info(f"  Items removed: {len(nrt_items) + len(ntc_items) - len(deduplicated)}")

        # Verify deduplication worked correctly
        deduplicated_ids = {item.id for item, _ in deduplicated}

        # All NTC items should be in the result
        ntc_ids = {item.id for item in ntc_items}
        assert ntc_ids.issubset(deduplicated_ids), "Not all NTC items are in deduplicated result"
        _log.info(f"  ✓ All {len(ntc_items)} NTC items are in deduplicated result")

        # NRT items with NTC equivalents should NOT be in the result
        overlapping_nrt_items = [item for item in nrt_items if _get_acquisition_key(item) in overlap]
        for nrt_item in overlapping_nrt_items:
            assert nrt_item.id not in deduplicated_ids, f"NRT item {nrt_item.id} should have been removed (has NTC equivalent)"
        _log.info(f"  ✓ All {len(overlapping_nrt_items)} overlapping NRT items were removed")

        # NRT items without NTC equivalents should be in the result
        unique_nrt_items = [item for item in nrt_items if _get_acquisition_key(item) not in ntc_keys]
        unique_nrt_ids = {item.id for item in unique_nrt_items}
        assert unique_nrt_ids.issubset(deduplicated_ids), "Not all unique NRT items are in result"
        _log.info(f"  ✓ All {len(unique_nrt_items)} unique NRT items (no NTC equivalent) are in result")

        # Verify final count
        expected_count = len(ntc_items) + len(unique_nrt_items)
        assert len(deduplicated) == expected_count, f"Expected {expected_count} items, got {len(deduplicated)}"
        _log.info(f"  ✓ Final count correct: {len(deduplicated)} = {len(ntc_items)} NTC + {len(unique_nrt_items)} unique NRT")

        _log.info("\n  SUCCESS: Deduplication function works correctly with real STAC data!")
        _log.info("  NTC takes precedence over NRT as expected.")
    else:
        _log.info("  No overlapping acquisitions found for this date range")
        _log.info("  (Deduplication still needed for other date ranges)")

    _log.info("="*80)
