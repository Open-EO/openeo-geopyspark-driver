"""
CDSE STAC API Assumptions Tests for Sentinel-3 SLSTR LST (sentinel-3-sl-2-lst)

This test suite explicitly verifies the assumptions we make about the Copernicus
Dataspace STAC API for Sentinel-3 SLSTR LST collections (NRT and NTC).

These tests query the LIVE STAC API to ensure our implementation matches reality.

NOTE: These tests require network access to https://stac.dataspace.copernicus.eu/.

Background:
- NRT (Near Real Time): Available within ~3 hours, for recent data
- NTC (Non Time Critical): Available after days/weeks, for archived data
- Same acquisition can exist in both collections
"""

import logging
from unittest import skip
import requests

_log = logging.getLogger(__name__)

# STAC collection URLs
NRT_COLLECTION_URL = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-sl-2-lst-nrt"
NTC_COLLECTION_URL = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-sl-2-lst-ntc"
STAC_SEARCH_URL = "https://stac.dataspace.copernicus.eu/v1/search"


def query_stac(collection_id: str, bbox: tuple, datetime: str, limit: int = 10) -> dict:
    """Helper to query STAC API."""
    params = {
        "collections": [collection_id],
        "bbox": list(bbox),
        "datetime": datetime,
        "limit": limit
    }
    response = requests.post(STAC_SEARCH_URL, json=params)
    response.raise_for_status()
    return response.json()


@skip("Integration test")
def test_assumption_both_collections_exist():
    """
    ASSUMPTION: Both NRT and NTC collections exist and are accessible.
    """
    _log.info("Testing that both NRT and NTC collections exist...")

    # Check NRT collection
    nrt_response = requests.get(NRT_COLLECTION_URL)
    assert nrt_response.status_code == 200, "NRT collection should be accessible"
    nrt_collection = nrt_response.json()
    assert nrt_collection["id"] == "sentinel-3-sl-2-lst-nrt"
    _log.info(f"NRT collection exists: {nrt_collection['title']}")

    # Check NTC collection
    ntc_response = requests.get(NTC_COLLECTION_URL)
    assert ntc_response.status_code == 200, "NTC collection should be accessible"
    ntc_collection = ntc_response.json()
    assert ntc_collection["id"] == "sentinel-3-sl-2-lst-ntc"
    _log.info(f"NTC collection exists: {ntc_collection['title']}")


@skip("Integration test")
def test_assumption_product_type_is_same():
    """
    ASSUMPTION: Both NRT and NTC items have the same product:type value (SL_2_LST___).

    This allows us to use a single productType filter for both collections.
    """
    _log.info("Testing that product:type is the same in NRT and NTC...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    # Query NRT
    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=3)
    assert len(nrt_data.get("features", [])) > 0, "Expected NRT items"
    nrt_product_types = {item["properties"].get("product:type") for item in nrt_data["features"]}

    # Query NTC
    ntc_data = query_stac("sentinel-3-sl-2-lst-ntc", bbox, datetime_range, limit=3)
    assert len(ntc_data.get("features", [])) > 0, "Expected NTC items"
    ntc_product_types = {item["properties"].get("product:type") for item in ntc_data["features"]}

    _log.info(f"  NRT product:type values: {nrt_product_types}")
    _log.info(f"  NTC product:type values: {ntc_product_types}")

    # Both should have "SL_2_LST___"
    assert "SL_2_LST___" in nrt_product_types, "NRT should have SL_2_LST___"
    assert "SL_2_LST___" in ntc_product_types, "NTC should have SL_2_LST___"
    _log.info("Both collections use product:type = 'SL_2_LST___'")


@skip("Integration test")
def test_assumption_acquisition_key_uniqueness():
    """
    ASSUMPTION: (platform, sat:absolute_orbit) uniquely identifies an acquisition.

    This is our deduplication key. We assume:
    - Same acquisition across NRT/NTC has same platform and sat:absolute_orbit
    - Different acquisitions have different (platform, sat:absolute_orbit) pairs
    """
    _log.info("Testing acquisition key uniqueness...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    # Query both collections
    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=10)
    ntc_data = query_stac("sentinel-3-sl-2-lst-ntc", bbox, datetime_range, limit=10)

    # Check that all items have the required properties
    for collection_name, data in [("NRT", nrt_data), ("NTC", ntc_data)]:
        for item in data.get("features", []):
            props = item["properties"]
            assert "platform" in props, f"{collection_name} item {item['id']} missing 'platform'"
            assert "sat:absolute_orbit" in props, f"{collection_name} item {item['id']} missing 'sat:absolute_orbit'"
            _log.debug(f"  {collection_name} {item['id']}: platform={props['platform']}, orbit={props['sat:absolute_orbit']}")

    # Build acquisition keys
    nrt_keys = {
        (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"]): item["id"]
        for item in nrt_data.get("features", [])
    }
    ntc_keys = {
        (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"]): item["id"]
        for item in ntc_data.get("features", [])
    }

    # Check for overlaps
    overlap_keys = set(nrt_keys.keys()) & set(ntc_keys.keys())
    _log.info(f"  Found {len(overlap_keys)} overlapping acquisition keys")

    if overlap_keys:
        for key in list(overlap_keys)[:2]:
            _log.info(f"  Example overlap: {key}")
            _log.info(f"    NRT: {nrt_keys[key]}")
            _log.info(f"    NTC: {ntc_keys[key]}")

    # Within each collection, keys should be unique
    assert len(nrt_keys) == len(nrt_data.get("features", [])), "NRT should have unique acquisition keys"
    assert len(ntc_keys) == len(ntc_data.get("features", [])), "NTC should have unique acquisition keys"
    _log.info("Acquisition keys (platform, sat:absolute_orbit) are unique within each collection")


@skip("Integration test")
def test_assumption_timeliness_category_distinguishes_nrt_ntc():
    """
    ASSUMPTION: product:timeliness_category is "NR" for NRT and "NT" for NTC.

    This allows us to verify we're deduplicating correctly.
    """
    _log.info("Testing timeliness category values...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    # Query NRT
    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=5)
    nrt_categories = {item["properties"].get("product:timeliness_category") for item in nrt_data.get("features", [])}

    # Query NTC
    ntc_data = query_stac("sentinel-3-sl-2-lst-ntc", bbox, datetime_range, limit=5)
    ntc_categories = {item["properties"].get("product:timeliness_category") for item in ntc_data.get("features", [])}

    _log.info(f"  NRT timeliness categories: {nrt_categories}")
    _log.info(f"  NTC timeliness categories: {ntc_categories}")

    assert nrt_categories == {"NR"}, "NRT should only have timeliness_category='NR'"
    assert ntc_categories == {"NT"}, "NTC should only have timeliness_category='NT'"
    _log.info("NRT has 'NR', NTC has 'NT' timeliness categories")


@skip("Integration test")
def test_assumption_datetimes_differ_slightly():
    """
    ASSUMPTION: start_datetime and end_datetime may differ by milliseconds between NRT and NTC.

    This is why we can't use exact datetime matching for deduplication.
    """
    _log.info("Testing datetime precision differences...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    # Query both collections
    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=10)
    ntc_data = query_stac("sentinel-3-sl-2-lst-ntc", bbox, datetime_range, limit=10)

    # Build acquisition key mappings
    nrt_items = {
        (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"]): item
        for item in nrt_data.get("features", [])
    }
    ntc_items = {
        (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"]): item
        for item in ntc_data.get("features", [])
    }

    # Find overlapping acquisitions
    overlap_keys = set(nrt_items.keys()) & set(ntc_items.keys())

    if len(overlap_keys) > 0:
        # Check datetime precision for overlapping items
        for key in list(overlap_keys)[:2]:
            nrt_item = nrt_items[key]
            ntc_item = ntc_items[key]

            nrt_start = nrt_item["properties"].get("start_datetime")
            ntc_start = ntc_item["properties"].get("start_datetime")

            _log.info(f"  Acquisition {key}:")
            _log.info(f"    NRT start_datetime: {nrt_start}")
            _log.info(f"    NTC start_datetime: {ntc_start}")

            if nrt_start != ntc_start:
                _log.info(f"    Datetimes differ (as expected)")
            else:
                _log.warning(f"    Datetimes are identical (unexpected but OK)")

        _log.info("  Verified that datetimes may differ between NRT and NTC")
    else:
        _log.info("  No overlapping acquisitions found, skipping datetime comparison")


@skip("Integration test")
def test_assumption_same_platform_values():
    """
    ASSUMPTION: Platform values are consistent (e.g., "sentinel-3a", "sentinel-3b").
    """
    _log.info("Testing platform values...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-01T00:00:00Z/2025-12-31T23:59:59Z"

    # Query both collections for a month
    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=20)
    ntc_data = query_stac("sentinel-3-sl-2-lst-ntc", bbox, datetime_range, limit=20)

    nrt_platforms = {item["properties"].get("platform") for item in nrt_data.get("features", [])}
    ntc_platforms = {item["properties"].get("platform") for item in ntc_data.get("features", [])}

    all_platforms = nrt_platforms | ntc_platforms
    _log.info(f"  Platforms found: {sorted(all_platforms)}")

    # Expected platforms
    expected_platforms = {"sentinel-3a", "sentinel-3b"}
    assert all_platforms.issubset(expected_platforms), f"Unexpected platforms: {all_platforms - expected_platforms}"
    _log.info(" Platform values are as expected (sentinel-3a, sentinel-3b)")


@skip("Integration test")
def test_assumption_orbit_numbers_are_integers():
    """
    ASSUMPTION: sat:absolute_orbit is an integer.
    """
    _log.info("Testing that sat:absolute_orbit is an integer...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=5)

    for item in nrt_data.get("features", []):
        orbit = item["properties"].get("sat:absolute_orbit")
        assert isinstance(orbit, int), f"sat:absolute_orbit should be int, got {type(orbit)}"
        assert orbit > 0, f"sat:absolute_orbit should be positive, got {orbit}"

    _log.info(" sat:absolute_orbit values are positive integers")


@skip("Integration test")
def test_assumption_overlaps_exist():
    """
    ASSUMPTION: Same acquisition exists in both NRT and NTC for certain date ranges.

    This verifies that deduplication is actually necessary.
    """
    _log.info("Testing that overlaps exist between NRT and NTC...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    # Use a date range where both NRT and NTC should have data
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=20)
    ntc_data = query_stac("sentinel-3-sl-2-lst-ntc", bbox, datetime_range, limit=20)

    nrt_keys = {
        (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"])
        for item in nrt_data.get("features", [])
    }
    ntc_keys = {
        (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"])
        for item in ntc_data.get("features", [])
    }

    overlap = nrt_keys & ntc_keys

    _log.info(f"  NRT items: {len(nrt_keys)}")
    _log.info(f"  NTC items: {len(ntc_keys)}")
    _log.info(f"  Overlapping acquisitions: {len(overlap)}")

    assert len(overlap) > 0, "Expected to find overlapping acquisitions between NRT and NTC"
    _log.info(f" Found {len(overlap)} overlapping acquisitions (deduplication is necessary)")


@skip("Integration test")
def test_assumption_assets_have_hrefs():
    """
    ASSUMPTIONS:
    1. Each item has at least one asset with href containing ".SEN3".
    2. That asset href starts with "s3://eodata/Sentinel-3/SLSTR/".
    """
    _log.info("Testing asset href assumptions...")

    bbox = (9.0, 45.0, 12.0, 47.0)
    datetime_range = "2025-12-30T00:00:00Z/2025-12-31T23:59:59Z"

    nrt_data = query_stac("sentinel-3-sl-2-lst-nrt", bbox, datetime_range, limit=2)

    for item in nrt_data.get("features", []):
        assert "assets" in item and item["assets"], f"Item {item['id']} should have assets"
        found = False
        for asset in item["assets"].values():
            href = asset.get("href", "")
            if ".SEN3" in href and href.startswith("s3://eodata/Sentinel-3/SLSTR/"):
                found = True
                _log.debug(f"  Asset href: {href[:80]}...")
                break
        assert found, f"Item {item['id']} should have at least one asset href with '.SEN3' and correct prefix"

    _log.info(" All items have at least one asset href with '.SEN3' and correct S3 prefix")

