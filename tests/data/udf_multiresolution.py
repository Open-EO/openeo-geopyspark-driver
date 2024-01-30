import xarray
import numpy as np
from openeo.udf import XarrayDataCube
from openeo.metadata import CollectionMetadata, SpatialDimension

def apply_metadata(metadata: CollectionMetadata,
                   context: dict) -> CollectionMetadata:
    """
    Modify metadata according to up-sampling factor
    """
    new_dimensions = metadata._dimensions.copy()
    for index, dim in enumerate(new_dimensions):
        if isinstance(dim, SpatialDimension):
            new_dim = SpatialDimension(name=dim.name,
                                       extent=dim.extent,
                                       crs=dim.crs,
                                       step=dim.step / 2.0)
            new_dimensions[index] = new_dim

    updated_metadata = metadata._clone_and_update(dimensions=new_dimensions)
    return updated_metadata


def fancy_upsample_function(array: np.array, factor: int = 2) -> np.array:
    assert array.ndim == 3
    return array.repeat(factor, axis=-1).repeat(factor, axis=-2)


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    cubearray: xarray.DataArray = cube.get_array().copy()

    # Pixel size of the original image
    init_pixel_size_x = cubearray.coords['x'][-1] - cubearray.coords['x'][-2]
    init_pixel_size_y = cubearray.coords['y'][-1] - cubearray.coords['y'][-2]

    if cubearray.data.ndim == 4 and cubearray.data.shape[0] == 1:
        cubearray = cubearray[0]
    predicted_array = fancy_upsample_function(cubearray.data, 2)

    coord_x = np.linspace(start=cube.get_array().coords['x'].min(), stop=cube.get_array().coords['x'].max() + init_pixel_size_x,
                          num=predicted_array.shape[-1], endpoint=False)
    coord_y = np.linspace(start=cube.get_array().coords['y'].min(), stop=cube.get_array().coords['y'].max() + init_pixel_size_y,
                          num=predicted_array.shape[-2], endpoint=False)

    if cube.get_array().data.ndim == 4:
        # Add a new dimension for time.
        predicted_array = np.expand_dims(predicted_array, axis = 0)
        coord_t = cube.get_array().coords['t']
        predicted_cube = xarray.DataArray(predicted_array, dims=['t', 'bands', 'y', 'x'], coords=dict(t=coord_t, x=coord_x, y=coord_y))
        return XarrayDataCube(predicted_cube)
    predicted_cube = xarray.DataArray(predicted_array, dims = ['bands', 'y', 'x'], coords = dict(x = coord_x, y = coord_y))
    return XarrayDataCube(predicted_cube)