from pathlib import Path

import pytest
from pyproj import CRS
import rasterio

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


    import rasterio
    from rasterio.transform import from_bounds, from_origin

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


if __name__ == "__main__":
    # Allow to run from commandline
    test_read_single()
