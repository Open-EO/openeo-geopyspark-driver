from pathlib import Path

from pyproj import CRS

from openeogeotrellis.collections.sentinel3 import *
import pytest

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
        limit_python_memory=True,
        resolution=0.008928571428571,
    )


    import rasterio
    from rasterio.transform import from_origin, from_bounds

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
        limit_python_memory=True,
        resolution=0.008928571428571,
    )

    assert len(result) == 0
