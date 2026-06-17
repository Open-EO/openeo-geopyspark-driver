"""
CDSE STAC API Assumptions Tests for Sentinel-3 SYNERGY SYN (sentinel-3-syn-2-syn-ntc/stc)

This test suite explicitly verifies the assumptions we make about the Copernicus
Dataspace STAC API for Sentinel-3 SYNERGY SYN NTC and STC collections.

These tests query the LIVE STAC API to ensure our implementation matches reality.

NOTE: These tests require network access to https://stac.dataspace.copernicus.eu/.

Background:
- NTC (Non Time Critical): Consolidated processing, higher quality
- STC (Short Time Critical): Available within hours/days, rapid delivery
- NO NRT products exist for SYNERGY Level-2 products (only NTC and STC)
- Both NTC and STC products are returned by opensearch, requiring deduplication
- The same acquisition may be processed with both timeliness modes
"""
import datetime
import logging
from unittest import skip
import requests

_log = logging.getLogger(__name__)

# STAC collection URLs
NTC_COLLECTION_URL = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-syn-2-syn-ntc"
STC_COLLECTION_URL = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-syn-2-syn-stc"
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
    ASSUMPTION: Both NTC and STC collections exist and are accessible.

    We use both collections for SYNERGY SYN, as analysis showed
    the original opensearch query returns both NTC and STC products.
    """
    _log.info("Testing that both NTC and STC collections exist...")

    # Check NTC collection
    ntc_response = requests.get(NTC_COLLECTION_URL)
    assert ntc_response.status_code == 200, "NTC collection should be accessible"
    ntc_collection = ntc_response.json()
    assert ntc_collection["id"] == "sentinel-3-syn-2-syn-ntc"
    _log.info(f"NTC collection exists: {ntc_collection['title']}")

    # Check STC collection
    stc_response = requests.get(STC_COLLECTION_URL)
    assert stc_response.status_code == 200, "STC collection should be accessible"
    stc_collection = stc_response.json()
    assert stc_collection["id"] == "sentinel-3-syn-2-syn-stc"
    _log.info(f"STC collection exists: {stc_collection['title']}")


@skip("Integration test")
def test_assumption_product_type_is_sy_2_syn():
    """
    ASSUMPTION: Both NTC and STC items have product:type value of SY_2_SYN___.

    This matches the opensearch productType filter we're migrating from.
    """
    _log.info("Testing that product:type is SY_2_SYN___...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    today = datetime.datetime.utcnow().date()
    # range is last 7 days
    datetime_range = f"{today - datetime.timedelta(days=7)}T00:00:00Z/{today}T23:59:59Z"

    # Query NTC
    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=5)
    assert len(ntc_data.get("features", [])) > 0, "Expected NTC items"
    ntc_product_types = {item["properties"].get("product:type") for item in ntc_data["features"]}
    _log.info(f"  NTC product:type values: {ntc_product_types}")
    assert "SY_2_SYN___" in ntc_product_types, "NTC should have SY_2_SYN___"

    # Query STC
    stc_data = query_stac("sentinel-3-syn-2-syn-stc", bbox, datetime_range, limit=5)
    assert len(stc_data.get("features", [])) > 0, "Expected STC items"
    stc_product_types = {item["properties"].get("product:type") for item in stc_data["features"]}
    _log.info(f"  STC product:type values: {stc_product_types}")
    assert "SY_2_SYN___" in stc_product_types, "STC should have SY_2_SYN___"

    _log.info("Both collections use product:type = 'SY_2_SYN___'")


@skip("Integration test")
def test_assumption_acquisition_key_properties_exist():
    """
    ASSUMPTION: (platform, sat:absolute_orbit) properties exist and can be used for deduplication.

    These properties are required for deduplicating between NTC and STC collections.
    """
    _log.info("Testing acquisition key properties exist...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    datetime_range = "2024-01-01T00:00:00Z/2024-01-02T23:59:59Z"

    # Query both collections
    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=5)
    stc_data = query_stac("sentinel-3-syn-2-syn-stc", bbox, datetime_range, limit=5)

    # Check that all items have the required properties
    for item in ntc_data.get("features", []):
        props = item["properties"]
        assert "platform" in props, f"NTC item {item['id']} missing 'platform'"
        assert "sat:absolute_orbit" in props, f"NTC item {item['id']} missing 'sat:absolute_orbit'"
        _log.debug(f"  NTC {item['id']}: platform={props['platform']}, orbit={props['sat:absolute_orbit']}")

    for item in stc_data.get("features", []):
        props = item["properties"]
        assert "platform" in props, f"STC item {item['id']} missing 'platform'"
        assert "sat:absolute_orbit" in props, f"STC item {item['id']} missing 'sat:absolute_orbit'"
        _log.debug(f"  STC {item['id']}: platform={props['platform']}, orbit={props['sat:absolute_orbit']}")

    _log.info("All items have required properties (platform, sat:absolute_orbit)")


@skip("Integration test")
def test_assumption_timeliness_categories():
    """
    ASSUMPTION: product:timeliness_category is "NT" for NTC collection and "ST" for STC collection.

    This allows us to distinguish between the two collections.
    """
    _log.info("Testing timeliness category values...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    today = datetime.datetime.utcnow().date()
    datetime_range = f"{today - datetime.timedelta(days=7)}T00:00:00Z/{today}T23:59:59Z"

    # Query NTC
    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=5)
    ntc_categories = {item["properties"].get("product:timeliness_category") for item in ntc_data.get("features", [])}
    _log.info(f"  NTC timeliness categories: {ntc_categories}")
    assert ntc_categories == {"NT"}, "NTC should only have timeliness_category='NT'"

    # Query STC
    stc_data = query_stac("sentinel-3-syn-2-syn-stc", bbox, datetime_range, limit=5)
    stc_categories = {item["properties"].get("product:timeliness_category") for item in stc_data.get("features", [])}
    _log.info(f"  STC timeliness categories: {stc_categories}")
    assert stc_categories == {"ST"}, "STC should only have timeliness_category='ST'"

    _log.info("NTC has 'NT' and STC has 'ST' timeliness categories")


@skip("Integration test")
def test_assumption_no_nrt_products():
    """
    ASSUMPTION: No NRT (Near Real Time) products exist for SYNERGY Level-2.

    According to the Sentinel-3 handbook:
    "SYNERGY Level 2 products are processed from consolidated OLCI and SLSTR
    L1b NTC products. They are then only provided in NTC or STC timeliness."

    This test verifies that no sentinel-3-syn-2-syn-nrt collection exists.
    """
    _log.info("Testing that no NRT collection exists...")

    nrt_url = "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-syn-2-syn-nrt"
    nrt_response = requests.get(nrt_url)

    # Should get 404 Not Found since NRT doesn't exist for SYNERGY
    assert nrt_response.status_code == 404, "NRT collection should not exist for SYNERGY SYN"
    _log.info("Confirmed: No NRT collection exists (as expected per handbook)")

@skip("Integration test")
def test_assumption_same_platform_values():
    """
    ASSUMPTION: Platform values are consistent (e.g., "sentinel-3a", "sentinel-3b").
    """
    _log.info("Testing platform values...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    datetime_range = "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z"

    # Query NTC collection for a month
    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=20)

    ntc_platforms = {item["properties"].get("platform") for item in ntc_data.get("features", [])}

    _log.info(f"  Platforms found: {sorted(ntc_platforms)}")

    # Expected platforms
    expected_platforms = {"sentinel-3a", "sentinel-3b"}
    assert ntc_platforms.issubset(expected_platforms), f"Unexpected platforms: {ntc_platforms - expected_platforms}"
    _log.info("Platform values are as expected (sentinel-3a, sentinel-3b)")


@skip("Integration test")
def test_assumption_orbit_numbers_are_integers():
    """
    ASSUMPTION: sat:absolute_orbit is an integer.
    """
    _log.info("Testing that sat:absolute_orbit is an integer...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    datetime_range = "2024-01-01T00:00:00Z/2024-01-02T23:59:59Z"

    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=5)

    for item in ntc_data.get("features", []):
        orbit = item["properties"].get("sat:absolute_orbit")
        assert isinstance(orbit, int), f"sat:absolute_orbit should be int, got {type(orbit)}"
        assert orbit > 0, f"sat:absolute_orbit should be positive, got {orbit}"

    _log.info("sat:absolute_orbit values are positive integers")


@skip("Integration test")
def test_assumption_assets_have_hrefs():
    """
    ASSUMPTIONS:
    1. Each item has at least one asset with href containing ".SEN3".
    2. That asset href starts with "s3://eodata/Sentinel-3/SYNERGY/SY_2_SYN___/".
    """
    _log.info("Testing asset href assumptions...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    datetime_range = "2024-01-01T00:00:00Z/2024-01-02T23:59:59Z"

    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=2)

    for item in ntc_data.get("features", []):
        assert "assets" in item and item["assets"], f"Item {item['id']} should have assets"
        found = False
        for asset in item["assets"].values():
            href = asset.get("href", "")
            if ".SEN3" in href and href.startswith("s3://eodata/Sentinel-3/SYNERGY/SY_2_SYN___/"):
                found = True
                _log.debug(f"  Asset href: {href[:80]}...")
                break
        assert found, f"Item {item['id']} should have at least one asset href with '.SEN3' and correct prefix"

    _log.info("All items have at least one asset href with '.SEN3' and correct S3 prefix")


@skip("Integration test")
def test_assumption_data_availability_starts_2018():
    """
    ASSUMPTION: Data availability starts from 2018-09-22 based on STAC collection metadata.

    This is when Sentinel-3B SYNERGY products became available.
    """
    _log.info("Testing data availability start date...")

    # Check collection metadata
    response = requests.get(NTC_COLLECTION_URL)
    collection = response.json()

    extent = collection.get("extent", {}).get("temporal", {}).get("interval", [[]])
    start_date = extent[0][0] if extent and extent[0] else None

    _log.info(f"  Collection start date: {start_date}")

    # Should start around 2018-09-22 (Sentinel-3B launch + processing start)
    assert start_date is not None, "Collection should have a start date"
    assert "2018-09" in start_date, f"Expected start date around 2018-09, got {start_date}"
    _log.info("Data availability starts from 2018-09 (as expected)")


@skip("Integration test")
def test_assumption_deduplication_required():
    """
    ASSUMPTION: Deduplication is required between NTC and STC collections.

    The same acquisition may exist in both collections with different processing times.
    Within each collection, acquisition keys should be unique.
    """
    _log.info("Testing deduplication assumptions...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    datetime_range = "2024-01-01T00:00:00Z/2024-01-05T23:59:59Z"

    # Query both collections
    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=50)
    stc_data = query_stac("sentinel-3-syn-2-syn-stc", bbox, datetime_range, limit=50)

    # Build acquisition keys for each collection
    ntc_keys = []
    for item in ntc_data.get("features", []):
        key = (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"])
        ntc_keys.append(key)

    stc_keys = []
    for item in stc_data.get("features", []):
        key = (item["properties"]["platform"], item["properties"]["sat:absolute_orbit"])
        stc_keys.append(key)

    # Check for duplicates within each collection
    ntc_unique = set(ntc_keys)
    stc_unique = set(stc_keys)

    _log.info(f"  NTC items: {len(ntc_keys)}, unique: {len(ntc_unique)}")
    _log.info(f"  STC items: {len(stc_keys)}, unique: {len(stc_unique)}")

    assert len(ntc_keys) == len(ntc_unique), "NTC collection should have unique acquisition keys"
    assert len(stc_keys) == len(stc_unique), "STC collection should have unique acquisition keys"

    # Check for overlap between collections (requires deduplication)
    overlap = ntc_unique & stc_unique
    _log.info(f"  Overlapping acquisitions between NTC and STC: {len(overlap)}")

    if len(overlap) > 0:
        _log.info("Deduplication IS required - same acquisitions exist in both collections")
    else:
        _log.info("No overlap found in this sample - but deduplication should still be implemented")


@skip("Integration test")
def test_assumption_filename_contains_timeliness_marker():
    """
    ASSUMPTION: Product filenames contain timeliness markers.

    NTC products have "_NT_" marker, STC products have "_ST_" marker.
    Example NTC: S3A_SY_2_SYN____20201101T001959_..._O_NT_002.SEN3
    Example STC: S3A_SY_2_SYN____20201101T001959_..._O_ST_002.SEN3
    """
    _log.info("Testing that filenames contain timeliness markers...")

    bbox = (9.0, 45.0, 12.0, 47.0)  # Europe
    datetime_range = "2024-01-01T00:00:00Z/2024-01-02T23:59:59Z"

    # Test NTC collection
    ntc_data = query_stac("sentinel-3-syn-2-syn-ntc", bbox, datetime_range, limit=5)
    for item in ntc_data.get("features", []):
        # Extract filename from asset href
        for asset in item["assets"].values():
            href = asset.get("href", "")
            if ".SEN3" in href:
                # Example: s3://eodata/.../S3A_SY_2_SYN____..._O_NT_002.SEN3/file.nc
                if "_NT_" in href:
                    _log.debug(f"  Found _NT_ in {href}")
                    break
        else:
            # If we got here, no asset had _NT_
            assert False, f"Expected to find _NT_ in at least one asset href for {item['id']}"

    _log.info("All NTC items have _NT_ marker in filename")

    # Test STC collection
    stc_data = query_stac("sentinel-3-syn-2-syn-stc", bbox, datetime_range, limit=5)
    for item in stc_data.get("features", []):
        # Extract filename from asset href
        for asset in item["assets"].values():
            href = asset.get("href", "")
            if ".SEN3" in href:
                # Example: s3://eodata/.../S3A_SY_2_SYN____..._O_ST_002.SEN3/file.nc
                if "_ST_" in href:
                    _log.debug(f"  Found _ST_ in {href}")
                    break
        else:
            # If we got here, no asset had _ST_
            assert False, f"Expected to find _ST_ in at least one asset href for {item['id']}"

    _log.info("All STC items have _ST_ marker in filename")
