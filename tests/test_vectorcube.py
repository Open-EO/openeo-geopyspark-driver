from unittest.mock import MagicMock

from openeo_driver.datacube import DriverVectorCube
from openeogeotrellis.utils import to_projected_polygons
from tests.data import get_test_data_file


class TestDriverVectorCube:


    def test_from_parquet(self):
        testfile = str(get_test_data_file("geometries/geoms.pq"))
        cube = DriverVectorCube.from_fiona([testfile],driver="parquet",options={})
        mockjvm = MagicMock()
        to_projected_polygons(jvm=mockjvm, geometry=cube, crs="epsg:4326")
        fromWktMethod = mockjvm.org.openeo.geotrellis.ProjectedPolygons.fromWkt
        fromWktMethod.assert_called_once()


