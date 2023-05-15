from unittest.mock import MagicMock
import pytest

from openeo_driver.datacube import DriverVectorCube
from openeogeotrellis.utils import to_projected_polygons
from tests.data import get_test_data_file


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
