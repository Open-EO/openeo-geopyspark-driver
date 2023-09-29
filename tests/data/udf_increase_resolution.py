import xarray
from openeo.udf import XarrayDataCube
from openeo.udf.debug import inspect
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
    # inspect(updated_metadata,"apply_metadata was invoked")
    return updated_metadata

def fancy_upsample_function(array: np.array, factor: int = 2) -> np.array:
    assert array.ndim == 4
    return array.repeat(factor, axis=-1).repeat(factor, axis=-2)

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    cubearray: xarray.DataArray = cube.get_array().copy() + 60
    cube.save_to_file(path="cube.json")
    return cube
    # cube.get_array().to_netcdf("cubegetarray.nc")

    # We make prediction and transform numpy array back to datacube
    # Pixel size of the original image
    coords_t = cubearray.coords['t']
    init_pixel_size_x = cubearray.coords['x'][-1] - cubearray.coords['x'][-2]
    init_pixel_size_y = cubearray.coords['y'][-1] - cubearray.coords['y'][-2]

    print(f"initial shape: {cubearray.data.shape}")
    predicted_array = fancy_upsample_function(cubearray.data, 2)
    print(f"predicted shape: {predicted_array.shape}")
    inspect(predicted_array, "test message")
    coord_x = np.linspace(start=cube.get_array().coords['x'].min(), stop=cube.get_array().coords['x'].max() + init_pixel_size_x,
                          num=predicted_array.shape[-2], endpoint=False) # TODO: Why -2
    coord_y = np.linspace(start=cube.get_array().coords['y'].max(), stop=cube.get_array().coords['y'].min() + init_pixel_size_y,
                          num=predicted_array.shape[-1], endpoint=False)
    predicted_cube = xarray.DataArray(predicted_array, dims=['t', 'bands', 'y', 'x'], coords=dict(t=coords_t, y=coord_y, x=coord_x))

    return XarrayDataCube(predicted_cube)