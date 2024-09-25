from shapely.geometry.base import BaseGeometry
from typing import Sequence

import datetime
import geopyspark.geotrellis
import json
import xarray
from geopyspark import TiledRasterLayer, LayerType, Bounds
from openeogeotrellis.ml.aggregatespatialvectorcube import AggregateSpatialVectorCube
from openeo_driver.errors import OpenEOApiException

from unittest.mock import MagicMock
import pytest

from openeo_driver.datacube import DriverVectorCube, DriverDataCube
from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import to_projected_polygons
from openeogeotrellis.vectorcube import AggregateSpatialResultCSV
from tests.data import get_test_data_file
import numpy as np
import numpy.testing as npt
import geopandas as gpd

from openeo_driver.utils import EvalEnv


@pytest.mark.parametrize(
    "file",[
    (
        "https://github.com/Open-EO/openeo-python-client/raw/master/tests/data/example_aoi.pq"
    ),
    (
        str(get_test_data_file("geometries/geoms.pq"))
    )
        ]
)
def test_from_parquet(file):

    cube = DriverVectorCube.from_fiona([file],driver="parquet",options={})
    #cube = DriverVectorCube.from_fiona(
    #    [testfile], driver="parquet",
    #    options={})
    assert cube.get_crs().to_epsg() == 4326
    mockjvm = MagicMock()
    to_projected_polygons(jvm=mockjvm, geometry=cube, crs="epsg:4326")
    fromWktMethod = mockjvm.org.openeo.geotrellis.ProjectedPolygons.fromWkt
    fromWktMethod.assert_called_once()
    assert fromWktMethod.call_args[0][1] == "EPSG:4326"


def test_filter_bands():
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    input_vector_cube = DriverVectorCube.from_geojson(geojson, columns_for_cube = DriverVectorCube.COLUMN_SELECTION_ALL)
    assert(input_vector_cube.get_cube().shape == (2,2))
    output_cube = input_vector_cube.filter_bands(["id"])
    cube = output_cube.get_cube()
    assert(cube.dims == ('geometry', 'properties'))
    labels = cube.properties.values
    assert(len(labels) == 1)
    assert(labels[0] == "id")
    assert(cube.shape == (2,1))


def test_vector_to_raster(imagecollection_with_two_bands_and_one_date):
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    target = imagecollection_with_two_bands_and_one_date

    input_vector_cube = DriverVectorCube.from_geojson(geojson, columns_for_cube = DriverVectorCube.COLUMN_SELECTION_NUMERICAL)
    # input_vector_cube = input_vector_cube.filter_bands(bands=["pop"]) TODO: filter_bands does not change dtype.
    input_cube = input_vector_cube.get_cube()
    assert(input_cube.shape == (2,1))
    assert(input_cube.dims == ('geometry', 'properties'))
    assert(input_cube.properties.values.tolist() == ['pop'])
    output_cube: GeopysparkDataCube = GeoPySparkBackendImplementation(use_job_registry=False).vector_to_raster(
        input_vector_cube = input_vector_cube,
        target = target
    )

    metadata: geopyspark.geotrellis.Metadata = output_cube.pyramid.levels[0].layer_metadata
    target_metadata: geopyspark.geotrellis.Metadata = target.pyramid.levels[0].layer_metadata
    target_bounds: Bounds = target_metadata.bounds
    min_spatial_key = geopyspark.SpatialKey(col = target_bounds.minKey.col, row = target_bounds.minKey.row)
    max_spatial_key = geopyspark.SpatialKey(col = target_bounds.maxKey.col, row = target_bounds.maxKey.row)
    expected_bounds = geopyspark.geotrellis.Bounds(minKey = min_spatial_key, maxKey = max_spatial_key)

    assert output_cube is not None
    assert metadata.crs == target_metadata.crs
    assert metadata.cell_type == "float64"
    assert metadata.extent == target_metadata.extent
    assert metadata.layout_definition == target_metadata.layout_definition
    assert metadata.bounds == expected_bounds
    assert metadata.tile_layout == target_metadata.tile_layout

    # output_cube.save_result(filename='test_vector_to_raster.tif', format='GTiff')
    result_layer: TiledRasterLayer = output_cube.pyramid.levels[0]
    assert result_layer.layer_type == LayerType.SPATIAL
    results_numpy = result_layer.to_numpy_rdd().collect()
    assert len(results_numpy) == 1
    tile = results_numpy[0][1][0]
    npt.assert_array_equal(np.unique(tile), np.array([1234.0, 5678.0, float('nan')]))


def test_vector_to_raster_no_numeric_bands(imagecollection_with_two_bands_and_one_date):
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    target = imagecollection_with_two_bands_and_one_date
    input_vector_cube = DriverVectorCube.from_geojson(geojson, columns_for_cube = ['id'])
    with pytest.raises(OpenEOApiException):
        GeoPySparkBackendImplementation(use_job_registry=False).vector_to_raster(
            input_vector_cube = input_vector_cube,
            target = target
        )


def test_aggregatespatialvectorcube_to_vectorcube(imagecollection_with_two_bands_spatial_only):
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    input_vector_cube: DriverVectorCube = DriverVectorCube.from_geojson(geojson, columns_for_cube = DriverVectorCube.COLUMN_SELECTION_ALL)
    # input_shapely: GeometryCollection = GeometryCollection(to_shapely(input_vector_cube.get_geometries().values).tolist())
    aggregate_result: AggregateSpatialVectorCube = imagecollection_with_two_bands_spatial_only.aggregate_spatial(
        input_vector_cube,
        {
            "mean1": {
                "process_id": "mean",
                "arguments": {
                    "data": {
                        "from_parameter": "data"
                    }
                },
                "result": True
            }
        }
    )
    assert isinstance(aggregate_result, AggregateSpatialVectorCube)

    # Convert the result to vector cube.
    output_vector_cube: DriverVectorCube = aggregate_result.to_driver_vector_cube()
    input_data: gpd.GeoDataFrame = aggregate_result._get_geodataframe()
    num_geometries = input_data.shape[0]
    assert input_data.shape == (num_geometries, 3)
    assert input_data.columns.tolist() == ['geometry', 'red', 'nir']

    # Check geometries.
    input_regions: gpd.GeoSeries = input_data.geometry
    output_regions: Sequence[BaseGeometry] = output_vector_cube.get_geometries()
    assert len(input_regions) == input_data.shape[0]
    for i in range(len(input_regions)):
        assert input_regions[i] == output_regions[i]

    # Check data.
    cube: xarray.DataArray = output_vector_cube.get_cube()
    assert cube.dims == ('geometry', 'bands')
    assert cube.shape == (num_geometries, 2)
    assert cube['bands'].values.tolist() == ['red', 'nir']
    assert cube.isel(geometry=0).values.tolist() == input_data.values[0, 1:].tolist()


def test_aggregatespatialresultcsv_to_vectorcube(imagecollection_with_two_bands_and_one_date):
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    input_vector_cube: DriverVectorCube = DriverVectorCube.from_geojson(geojson, columns_for_cube = DriverVectorCube.COLUMN_SELECTION_ALL)
    # input_shapely: GeometryCollection = GeometryCollection(to_shapely(input_vector_cube.get_geometries().values).tolist())
    aggregate_result: AggregateSpatialResultCSV = imagecollection_with_two_bands_and_one_date.aggregate_spatial(
        input_vector_cube,
        {
            "mean1": {
                "process_id": "mean",
                "arguments": {
                    "data": {
                        "from_parameter": "data"
                    }
                },
                "result": True
            }
        }
    )
    assert isinstance(aggregate_result, AggregateSpatialResultCSV)
    # Convert the result to vector cube.
    output_vector_cube: DriverVectorCube = aggregate_result.to_driver_vector_cube()
    input_data: dict[str, list[list[float]]] = aggregate_result.get_data()  # Shape (1,2,2) (t,geometry,bands)
    first_date = list(input_data.keys())[0]
    num_dates = len(input_data)
    num_geometries = len(input_data[first_date])
    num_bands = len(input_data[first_date][0])

    # Check geometries.
    input_regions = aggregate_result._regions.get_geometries()
    output_regions = output_vector_cube.get_geometries()
    assert len(input_regions) == num_geometries
    for i in range(num_geometries):
        assert input_regions[i] == output_regions[i]

    # Check data.
    cube: xarray.DataArray = output_vector_cube.get_cube()
    assert cube.dims == ('geometry', 't', 'bands')
    assert cube.shape == (num_geometries, num_dates, num_bands)  # (2, 1, 2)
    # Mean for band 1 and 2.
    assert cube.isel(geometry=0, t=0).values.tolist() == input_data[first_date][0]


def test_aggregatespatialresultcsv_vector_to_raster(imagecollection_with_two_bands_and_three_dates):
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    target = imagecollection_with_two_bands_and_three_dates
    input_vector_cube: DriverVectorCube = DriverVectorCube.from_geojson(geojson, columns_for_cube = DriverVectorCube.COLUMN_SELECTION_ALL)
    aggregate_result: AggregateSpatialResultCSV = imagecollection_with_two_bands_and_three_dates.aggregate_spatial(
        input_vector_cube,
        {
            "mean1": {
                "process_id": "mean",
                "arguments": {
                    "data": {
                        "from_parameter": "data"
                    }
                },
                "result": True
            }
        }
    )
    assert isinstance(aggregate_result, AggregateSpatialResultCSV)
    output_cube: DriverDataCube = GeoPySparkBackendImplementation(use_job_registry=False).vector_to_raster(
        input_vector_cube = aggregate_result,
        target = target
    )
    assert len(output_cube.metadata.band_dimension.bands) == 2
    assert output_cube.metadata.temporal_dimension.extent == ('2017-09-25T11:37:00Z', '2017-10-25T11:37:00Z')

    output_cube_np = output_cube.pyramid.levels[0].to_numpy_rdd().collect()
    assert len(output_cube_np) == 3
    expected_values = {
        geopyspark.SpaceTimeKey(col = 0, row = 0, instant = datetime.datetime(2017, 10, 25, 11, 37)): [2.0, 1.0],
        geopyspark.SpaceTimeKey(col = 0, row = 0, instant = datetime.datetime(2017, 9, 25, 11, 37)): [1.0, 2.0],
        geopyspark.SpaceTimeKey(col = 0, row = 0, instant = datetime.datetime(2017, 9, 30, 0, 37)): []
    }
    for i in range(3):
        space_time_key = output_cube_np[i][0]
        tile: geopyspark.Tile = output_cube_np[i][1]
        assert space_time_key in expected_values.keys()
        assert tile.cells.shape == (2, 4, 4)
        mean_band0 = np.unique(tile.cells[0]).tolist()
        mean_band1 = np.unique(tile.cells[1]).tolist()
        expected_band_values = expected_values[space_time_key]
        if len(expected_band_values) == 0:
            np.isnan(tile.cells[0]).all()
            np.isnan(tile.cells[1]).all()
        else:
            assert mean_band0[0] == expected_band_values[0] and np.isnan(mean_band0[1])
            assert mean_band1[0] == expected_band_values[1] and np.isnan(mean_band1[1])


def test_raster_to_vector_and_apply_udf(imagecollection_with_two_bands_and_three_dates, backend_implementation):
    vectorcube: DriverVectorCube = imagecollection_with_two_bands_and_three_dates.raster_to_vector()
    # Check raster_to_vector result.
    assert isinstance(vectorcube, DriverVectorCube)
    cube: xarray.DataArray = vectorcube.get_cube()
    num_geometries = vectorcube.geometry_count()
    nr_bands, nr_dates = (1, 2)
    assert cube is not None
    assert num_geometries > 1
    assert cube.shape == (num_geometries, nr_dates, nr_bands)
    assert cube.dims == ('geometry', 't', 'bands')
    assert vectorcube._geometries.shape == (num_geometries,1)

    date1_values = np.array([[np.nan], [np.nan], [np.nan], [np.nan], [ 1.], [ 1.], [ 1.], [ 1.]])
    date2_values = np.array([[2.], [2.], [2.], [2.], [np.nan], [np.nan], [np.nan], [np.nan]])
    assert np.allclose(cube.isel(t=0).values, date1_values, equal_nan=True)
    assert np.allclose(cube.isel(t=1).values, date2_values, equal_nan=True)

    # Test apply_dimension with UDF on raster_to_vector result.
    process = {
        "process_graph": {
            "process_id": "run_udf",
            "result": True,
            "arguments": {
                "data": {
                    "from_parameter": "data"
                },
                "udf": "import openeo\nprint(\"hello\")\ndef apply_vectorcube(geometries, cube: xarray.DataArray):\n\treturn (geometries, cube)",
                "runtime": "Python",
                "version": "1.0.0",
                "context": {}
            }
        }
    }
    env = EvalEnv(values = {
        "backend_implementation": backend_implementation,
    })
    vectorcube_udf_applied = vectorcube.apply_dimension(process=process, dimension=DriverVectorCube.DIM_GEOMETRY, env=env)
    assert isinstance(vectorcube_udf_applied, DriverVectorCube)
    cube_udf_applied = vectorcube_udf_applied.get_cube()
    assert cube_udf_applied.shape == cube.shape
    assert cube_udf_applied.dims == cube.dims
    assert cube_udf_applied.shape == cube.shape
    assert np.allclose(cube_udf_applied.values, cube.values, equal_nan=True)
    assert all(vectorcube_udf_applied.get_geometries() == vectorcube.get_geometries())
