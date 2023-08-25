import json
from geopyspark import TiledRasterLayer, LayerType
from openeo.metadata import SpatialDimension, CollectionMetadata

from unittest.mock import MagicMock
import pytest

from openeo_driver.datacube import DriverVectorCube, DriverDataCube
from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import to_projected_polygons
from tests.data import get_test_data_file
import numpy as np
import numpy.testing as npt


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


def test_vector_to_raster():
    with open(get_test_data_file("geometries/FeatureCollection02.json")) as f:
        geojson = json.load(f)
    dimension = SpatialDimension(name="sample", extent=[0, 0, 1, 1], crs="EPSG:4326", step=1)
    target_raster_cube = DriverDataCube(metadata = CollectionMetadata(metadata = {}, dimensions = [dimension]))
    input_vector_cube = DriverVectorCube.from_geojson(geojson, columns_for_cube = DriverVectorCube.COLUMN_SELECTION_ALL)
    input_cube = input_vector_cube.get_cube()
    assert(input_cube.shape == (2,2))
    assert(input_cube.dims == ('geometry', 'properties'))
    assert(input_cube.properties.values.tolist() == ['id', 'pop'])
    output_cube: GeopysparkDataCube = GeoPySparkBackendImplementation().vector_to_raster(
        input_vector_cube = input_vector_cube,
        target_raster_cube = target_raster_cube
    )
    metadata = output_cube.metadata
    assert metadata.band_dimension.name == "bands"
    assert len(metadata.band_dimension.bands) == 1
    assert metadata.band_dimension.bands[0].name == 'pop'
    assert metadata.spatial_extent == {'west': 1.0, 'east': 5.0, 'north': 4.0, 'south': 1.0}
    assert metadata.temporal_extent is None
    assert output_cube is not None

    # output_cube.save_result(filename='test_vector_to_raster.tif', format='GTiff')
    result_layer: TiledRasterLayer = output_cube.pyramid.levels[0]
    assert result_layer.layer_type == LayerType.SPATIAL
    results_numpy = result_layer.to_numpy_rdd().collect()
    assert len(results_numpy) == 1
    tile = results_numpy[0][1][0]
    npt.assert_array_equal(np.unique(tile), np.array([1234.0, 5678.0, float('nan')]))
