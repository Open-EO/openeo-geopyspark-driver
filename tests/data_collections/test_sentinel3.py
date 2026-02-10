import unittest
from pathlib import Path
from unittest import skip

import rasterio

from openeo_driver.backend import LoadParameters
from openeo_driver.utils import EvalEnv

if __name__ == "__main__":
    import openeogeotrellis.deploy.local

    # Allow to run from commandline:
    # /usr/bin/time -v python3 tests/data_collections/test_sentinel3.py
    # python3 -m memory_profiler tests/data_collections/test_sentinel3.py
    # The SparkContext is only needed to make imports work, but is actually not used for the tests
    openeogeotrellis.deploy.local.setup_environment()

from openeogeotrellis.collections.sentinel3 import *
from openeogeotrellis.collections.sentinel3 import _get_acquisition_key
from numpy.testing import assert_allclose
from unittest.mock import Mock

from tests.data import get_test_data_file

def test_read_single():

    tiles = [
        {
            "key":{
                "col":10,
                "row":10,
                "instant":100,

            },
            "key_extent":{
                "xmin":3.1,"xmax":7.2,"ymin":50.1,"ymax":55.2
            },
            "key_epsg":4326
        }
    ]
    result = read_product(
        (
            Path(__file__).parent.parent
            / "data/binary/Sentinel-3/S3A_SL_2_LST____20240129T100540_20240129T100840_20240129T121848_0179_108_236_2160_PS1_O_NR_004.SEN3",
            tiles,
        ),
        SLSTR_PRODUCT_TYPE,
        ["LST_in:LST"],
        tile_size=1024,
        resolution=0.008928571428571,
    )


    from rasterio.transform import from_bounds

    arr = result[0][1].cells

    transform = from_bounds(3.1, 50.1, 7.2, 55.2, arr.shape[2], arr.shape[1])


    # new_dataset = rasterio.open('test1.tif', 'w', driver='GTiff',
    #                             height=arr.shape[1], width=arr.shape[2],
    #                             count=1, dtype=str(arr.dtype),
    #                             crs=CRS.from_epsg(4326),
    #                             transform=transform)
    #
    # new_dataset.write(arr)
    # new_dataset.close()


def test_read_single_binned():
    tiles = [
        {
            "key":{
                "col":10,
                "row":10,
                "instant":100,

            },
            "key_extent":{
                "xmin":3.1,"xmax":7.2,"ymin":50.1,"ymax":55.2
            },
            "key_epsg":4326
        }
    ]
    result = read_product(
        (
            Path(__file__).parent.parent
            / "data/binary/Sentinel-3/S3A_SL_2_LST____20240129T100540_20240129T100840_20240129T121848_0179_108_236_2160_PS1_O_NR_004.SEN3",
            tiles,
        ),
        SLSTR_PRODUCT_TYPE,
        ["LST_in:LST"],
        tile_size=1024,
        resolution=0.008928571428571,
        reprojection_type=REPROJECTION_TYPE_BINNING,
        binning_args={
            KEY_SUPER_SAMPLING: 3,
        }
    )

    arr = result[0][1].cells
    return

def test_read_masked_binned():
    tiles = [
        {
            "key":{
                "col":10,
                "row":10,
                "instant":1717326516089,
            },
            "key_extent":{
                "xmin":13.857467,"xmax":18.10,"ymin":47.096,"ymax":47.597925
            },
            "key_epsg":4326
        }
    ]
    result = read_product(
        (
            Path(__file__).parent.parent
            / "data/binary/Sentinel-3/S3A_SL_2_LST____20240129T100540_20240129T100840_20240129T121848_0179_108_236_2160_PS1_O_NR_004.SEN3",
            tiles,
        ),
        SLSTR_PRODUCT_TYPE,
        ["LST_in:LST", "flags_in:cloud_in"],
        tile_size=1024,
        resolution=0.008928571428571,
        reprojection_type=REPROJECTION_TYPE_BINNING,
        binning_args={
            KEY_SUPER_SAMPLING: 3,
            KEY_FLAG_BAND: "cloud_in",
            KEY_FLAG_BITMASK: 0xff,
        }
    )

    arr = result[0][1].cells


def test_read_single_edge():

    tiles = [
        {
            "key":{
                "col":10,
                "row":10,
                "instant":100,

            },
            "key_extent":{
                "xmin":15.86,"xmax":18.10,"ymin":47.096,"ymax":47.597
            },
            "key_epsg":4326
        }
    ]
    product_dir = Path(
        __file__).parent.parent / "data/binary/Sentinel-3/S3A_SL_2_LST____20240129T100540_20240129T100840_20240129T121848_0179_108_236_2160_PS1_O_NR_004.SEN3"
    result = read_product(
        (product_dir, tiles),
        SLSTR_PRODUCT_TYPE,
        ["LST_in:LST", "geometry_tn:solar_zenith_tn"],
        tile_size=1024,
        resolution=0.008928571428571,
    )

    assert len(result) == 0

def test_read_single_edge_with_some_data():

    tiles = [
        {
            "key":{
                "col":10,
                "row":10,
                "instant":1717326516089,
            },
            "key_extent":{
                "xmin":13.857467,"xmax":18.10,"ymin":47.096,"ymax":47.597925
            },
            "key_epsg":4326
        }
    ]
    product_dir = Path(
        __file__).parent.parent / "data/binary/Sentinel-3/S3A_SL_2_LST____20240129T100540_20240129T100840_20240129T121848_0179_108_236_2160_PS1_O_NR_004.SEN3"
    result = read_product((product_dir, tiles), SLSTR_PRODUCT_TYPE, ["LST_in:LST",  "geometry_tn:solar_zenith_tn"], 1024, 0.008928571428571)

    assert len(result) == 1

    spacetimekey = result[0][0]

    #instant should be rounded to the minute
    assert spacetimekey.instant == datetime(2024, 6, 2, 11, 8,0)
    # from rasterio.transform import from_origin, from_bounds

    arr = result[0][1].cells

    # transform = from_bounds(13.857467, 47.096, 18.10, 47.597925, arr.shape[2], arr.shape[1])
    #
    # new_dataset = rasterio.open('ref_file_edge_shift.tif', 'w', driver='GTiff',
    #                              height=arr.shape[1], width=arr.shape[2],
    #                             count=2, dtype=str(arr.dtype),
    #                             crs=CRS.from_epsg(4326),
    #                             transform=transform)
    #
    # new_dataset.write(arr)
    # new_dataset.close()

    expected_path =  get_test_data_file("binary/Sentinel-3/sentinel3_edge_ref.tif")

    with rasterio.open(expected_path) as ds_ref:
        expected_result = ds_ref.read()
    #with rasterio.open(filename) as ds:
        actual_result = arr
        assert expected_result.shape == actual_result.shape
        assert_allclose(expected_result, actual_result, atol=0.00001)


@unittest.mock.patch("openeogeotrellis.collections.sentinel3.read_band")
def test_mask_before_binning(mock_read_band):
    value_to_be_masked = 5
    value_to_keep = 1
    fill_value = np.nan
    mask_set = 4

    shape = (4, 5)
    out_shape = shape

    band_data = np.full(shape, value_to_keep, dtype=np.float32)
    mask_data = np.zeros(shape, dtype=np.uint16)

    band_data[3, 3] = value_to_be_masked
    band_data[3, 4] = value_to_be_masked
    mask_data[3, 3] = mask_set
    mask_data[3, 4] = mask_set

    def return_value(_in_file, in_band, *args, **kwargs):
        data, attrs = None, {}
        if in_band == "CLOUD_flags":
            data = mask_data
            attrs["dtype"] = "uint8"
        elif in_band == "SDR_Oa04":
            data = band_data
            attrs["dtype"] = "float32"
        else:
            raise ValueError(f"in_band '{in_band}' not recognized")

        return data, {"dtype": "uint"}

    mock_read_band.side_effect = return_value
    #lat = np.tile(np.arange(shape[0], dtype=np.float64), (shape[1], 1))
    lat = np.tile(np.arange(shape[0], 0, -1, dtype=np.float64), (shape[1], 1)).T
    lon = np.tile(np.arange(shape[1], dtype=np.float64), (shape[0], 1))
    source_coordinates = np.stack([lon, lat], axis=0)
    target_coordinates = np.stack([lon, lat], axis=2)

    binned_data_vars = do_binning(
        product_type=SYNERGY_PRODUCT_TYPE,
        final_grid_resolution=1.0,
        creo_path=Path("fake_path"),
        band_names=["flags:CLOUD_flags", "Syn_Oa04_reflectance"],
        source_coordinates=source_coordinates,
        target_coordinates=target_coordinates,
        data_mask=None,
        angle_source_coordinates=None,
        target_coordinates_with_rim=None,
        angle_data_mask=None,
        digital_numbers=True,
        super_sampling=3,
        flag_bitmask=np.uint16(0xff),
        flag_band="CLOUD_flags",
    )

    assert binned_data_vars.shape == (2, *out_shape)
    binned_band = binned_data_vars[1]
    assert ((binned_band == value_to_keep) | np.isnan(binned_band)).all()
    assert np.count_nonzero(np.isfinite(binned_band)) > 0


@skip("requires mounting raster data.")
def test_slstr_load_collection_opensearch():
    # Full workflow test starting at load_collection.
    from openeogeotrellis.layercatalog import get_layer_catalog, GeoPySparkLayerCatalog

    catalog: GeoPySparkLayerCatalog = get_layer_catalog()

    load_params = LoadParameters(
        spatial_extent={"west": 3.1, "south": 50.1, "east": 7.2, "north": 55.2},
        temporal_extent=("2026-01-10T00:00:00", "2026-01-10T23:59:59"),
        bands=["LST", "LST_uncertainty", "sunAzimuthAngles", "confidence_in", "exception", "viewAzimuthAngles", "viewZenithAngles"],
    )
    env = EvalEnv(
        {"openeo_api_version": "1.0.0"}
    )
    catalog._load_collection_cached("SENTINEL3_SLSTR_L2_LST", load_params=load_params, env=env)


if __name__ == "__main__":
    # Allow to run from commandline
    test_read_single()


def test_get_acquisition_key():
    """Test getting acquisition key from STAC item properties."""
    # Create mock items with properties
    nrt_item = Mock()
    nrt_item.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}
    assert _get_acquisition_key(nrt_item) == ("sentinel-3a", 51421)

    # NTC item from same acquisition should have same key
    ntc_item = Mock()
    ntc_item.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}
    assert _get_acquisition_key(ntc_item) == ("sentinel-3a", 51421)

    # Different acquisition
    other_item = Mock()
    other_item.properties = {"platform": "sentinel-3b", "sat:absolute_orbit": 40027}
    assert _get_acquisition_key(other_item) == ("sentinel-3b", 40027)

    # Missing properties should return (None, None)
    incomplete_item = Mock()
    incomplete_item.properties = {}
    assert _get_acquisition_key(incomplete_item) == (None, None)


def test_deduplicate_items_prefer_ntc_no_overlap():
    """Test deduplication when NRT and NTC have different acquisitions (no overlap)."""
    # Create mock items with different orbits
    nrt_item1 = Mock()
    nrt_item1.id = "NRT_1"
    nrt_item1.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}

    nrt_item2 = Mock()
    nrt_item2.id = "NRT_2"
    nrt_item2.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51422}

    ntc_item1 = Mock()
    ntc_item1.id = "NTC_1"
    ntc_item1.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51420}

    items_by_collection = {
        "NRT": [(nrt_item1, {}), (nrt_item2, {})],
        "NTC": [(ntc_item1, {})],
    }

    result = deduplicate_items_prefer_ntc(items_by_collection)

    # Should have all items since there's no overlap
    assert len(result) == 3
    # NTC item should be included
    assert any(item.id == ntc_item1.id for item, _ in result)
    # Both NRT items should be included
    assert any(item.id == nrt_item1.id for item, _ in result)
    assert any(item.id == nrt_item2.id for item, _ in result)


def test_deduplicate_items_prefer_ntc_with_overlap():
    """Test deduplication when same acquisition exists in both NRT and NTC."""
    # Create mock items with same orbit (overlap)
    nrt_item = Mock()
    nrt_item.id = "NRT_overlapping"
    nrt_item.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}

    ntc_item = Mock()
    ntc_item.id = "NTC_overlapping"
    ntc_item.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}

    # Different acquisition in NRT
    nrt_item_unique = Mock()
    nrt_item_unique.id = "NRT_unique"
    nrt_item_unique.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51422}

    items_by_collection = {
        "NRT": [(nrt_item, {}), (nrt_item_unique, {})],
        "NTC": [(ntc_item, {})],
    }

    result = deduplicate_items_prefer_ntc(items_by_collection)

    # Should have 2 items: NTC version of overlapping + unique NRT
    assert len(result) == 2

    # NTC item should be included
    assert any(item.id == ntc_item.id for item, _ in result)

    # Unique NRT item should be included
    assert any(item.id == nrt_item_unique.id for item, _ in result)

    # NRT version of overlapping acquisition should NOT be included
    assert not any(item.id == nrt_item.id for item, _ in result)


def test_deduplicate_items_prefer_ntc_all_overlap():
    """Test deduplication when all NRT items have NTC equivalents."""
    # Create mock items - all have NTC equivalents
    nrt_item1 = Mock()
    nrt_item1.id = "NRT_1"
    nrt_item1.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}

    nrt_item2 = Mock()
    nrt_item2.id = "NRT_2"
    nrt_item2.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51422}

    ntc_item1 = Mock()
    ntc_item1.id = "NTC_1"
    ntc_item1.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}

    ntc_item2 = Mock()
    ntc_item2.id = "NTC_2"
    ntc_item2.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51422}

    items_by_collection = {
        "NRT": [(nrt_item1, {}), (nrt_item2, {})],
        "NTC": [(ntc_item1, {}), (ntc_item2, {})],
    }

    result = deduplicate_items_prefer_ntc(items_by_collection)

    # Should have only 2 items: both NTC versions
    assert len(result) == 2

    # Both NTC items should be included
    assert any(item.id == ntc_item1.id for item, _ in result)
    assert any(item.id == ntc_item2.id for item, _ in result)

    # No NRT items should be included
    assert not any(item.id == nrt_item1.id for item, _ in result)
    assert not any(item.id == nrt_item2.id for item, _ in result)


def test_deduplicate_items_prefer_ntc_empty_collections():
    """Test deduplication with empty collections."""
    # Empty both
    result = deduplicate_items_prefer_ntc({"NRT": [], "NTC": []})
    assert len(result) == 0

    # Only NRT
    nrt_item = Mock()
    nrt_item.id = "NRT_only"
    nrt_item.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}
    result = deduplicate_items_prefer_ntc({"NRT": [(nrt_item, {})], "NTC": []})
    assert len(result) == 1
    assert result[0][0].id == nrt_item.id

    # Only NTC
    ntc_item = Mock()
    ntc_item.id = "NTC_only"
    ntc_item.properties = {"platform": "sentinel-3a", "sat:absolute_orbit": 51421}
    result = deduplicate_items_prefer_ntc({"NRT": [], "NTC": [(ntc_item, {})]})
    assert len(result) == 1
    assert result[0][0].id == ntc_item.id
