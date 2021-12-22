import xarray
from openeo.udf import XarrayDataCube

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array: xarray.DataArray = cube.get_array()
    array += 1000
    # Shape (#dates, #bands, #rows, #cols)
    array.loc[:, 'red'] += 10
    array.loc[:, 'nir'] += 100
    return XarrayDataCube(array)
