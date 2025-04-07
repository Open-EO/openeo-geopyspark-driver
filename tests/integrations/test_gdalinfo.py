import json

from openeogeotrellis.integrations.gdal import find_gdalinfo_differences
from tests.data import get_test_data_file


def test_find_gdalinfo_differences():
    gdalinfo_from_driver = json.load(
        get_test_data_file("gdalinfo-output/openEO_2017-03-07Z.tif_gdalinfo_from_driver.json").open()
    )
    gdalinfo_from_exectr = json.load(
        get_test_data_file("gdalinfo-output/openEO_2017-03-07Z.tif_gdalinfo_from_exectr.json").open()
    )
    assert find_gdalinfo_differences(gdalinfo_from_driver, gdalinfo_from_exectr) is None


def test_find_gdalinfo_differences_different():
    gdalinfo_from_driver = json.load(
        get_test_data_file("gdalinfo-output/openEO_2017-03-07Z.tif_gdalinfo_from_driver.json").open()
    )
    gdalinfo_from_exectr = json.load(
        get_test_data_file("gdalinfo-output/c_gls_LC100-COV-GRASSLAND_201501010000_AFRI_PROBAV_1.0.1.nc.json").open()
    )
    assert find_gdalinfo_differences(gdalinfo_from_driver, gdalinfo_from_exectr) == "Projection metadata differs"
