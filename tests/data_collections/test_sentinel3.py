from pathlib import Path
import unittest

import rasterio
import numpy as np

if __name__ == "__main__":
    import openeogeotrellis.deploy.local

    # Allow to run from commandline:
    # /usr/bin/time -v python3 tests/data_collections/test_sentinel3.py
    # python3 -m memory_profiler tests/data_collections/test_sentinel3.py
    # The SparkContext is only needed to make imports work, but is actually not used for the tests
    openeogeotrellis.deploy.local.setup_environment()

from openeogeotrellis.collections.sentinel3 import *
from numpy.testing import assert_allclose

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
                "instant":100,

            },
            #{"type":"Polygon","coordinates":[[[-180,73.130594],[-152.392,69.3935],[-165.135,59.9629],[-180,62.346828],[-180,73.130594]]]}
            "key_extent":{
                "xmin":-180.0,"xmax":-176.0,"ymin":60.0,"ymax":64.0,
            },
            "key_epsg":4326
        }
    ]
    result = read_product(
        (
            Path(__file__).parent.parent
            / "data/binary/Sentinel-3/S3A_SY_2_SYN____20250516T222406_20250516T222706_20250518T150818_0179_126_058_1800_PS1_O_NT_002.SEN3",
            tiles,
        ),
        SYNERGY_PRODUCT_TYPE,
        ["flags:CLOUD_flags", "Syn_Oa01_reflectance"],
        tile_size=1024,
        resolution=0.008928571428571,
        reprojection_type=REPROJECTION_TYPE_BINNING,
        binning_args={
            KEY_SUPER_SAMPLING: 3,
            KEY_FLAG_BAND: "CLOUD_flags",
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
        data = None
        if in_band == "CLOUD_flags":
            data = mask_data
        elif in_band == "SDR_Oa04":
            data = band_data
        else:
            raise ValueError(f"in_band '{in_band}' not recognized")

        return data, {}

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

    assert binned_data_vars.shape == (1, *out_shape)
    assert ((binned_data_vars == value_to_keep) | np.isnan(binned_data_vars)).all()
    assert np.count_nonzero(np.isfinite(binned_data_vars)) > 0


if __name__ == "__main__":
    # Allow to run from commandline
    test_read_single()
