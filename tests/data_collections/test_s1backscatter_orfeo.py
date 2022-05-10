import contextlib
import datetime
import json
import os
import subprocess
import sys
import textwrap
import zipfile
from multiprocessing import Process
from pathlib import Path
from unittest import skip

import pytest
import rasterio
import threading
from numpy.testing import assert_array_almost_equal, assert_allclose

import signal
from shapely.geometry import GeometryCollection, shape

from openeo_driver.backend import LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.utils import EvalEnv
from openeogeotrellis.collections.s1backscatter_orfeo import S1BackscatterOrfeo, _import_orfeo_toolbox, \
    _instant_ms_to_day, S1BackscatterOrfeoV2



@pytest.mark.skipif(not os.environ.get("CREODIAS"), reason="Requires CREODIAS environment.")
@pytest.mark.parametrize(["spatial_extent", "temporal_extent", "expected_shape","implementation_version","ref_path"], [
    (
            dict(west=3.1, south=51.27, east=3.3, north=51.37),  # Zeebrugge
            ("2020-06-06T00:00:00", "2020-06-06T23:59:59"),
            (2, 1117, 1397),
            "2",
            "https://artifactory.vgt.vito.be/testdata-public/S1_backscatter_otb_zeebrugge.tiff"
    ),
    (
            dict(west=3.1, south=51.27, east=3.3, north=51.37),  # Zeebrugge
            ("2020-06-06T00:00:00", "2020-06-06T23:59:59"),
            (2, 1117, 1397),
            "1",
            "https://artifactory.vgt.vito.be/testdata-public/S1_backscatter_otb_zeebrugge.tiff"
    ),
    (
            dict(west=5.5, south=50.13, east=5.65, north=50.23),  # La Roche-en-Ardenne
            ("2020-07-29T00:00:00", "2020-07-29T23:59:59"),
            (2, 1150, 1110),
            "2",
            "https://artifactory.vgt.vito.be/testdata-public/S1_backscatter_otb_laroche.tiff"
    ),
])
def test_creodias_s1_backscatter(tmp_path, spatial_extent, temporal_extent, expected_shape,implementation_version,ref_path):
    """
    Depends on:
     /eodata/Sentinel-1/SAR/GRD/2020/06/06/S1B_IW_GRDH_1SDV_20200606T060615_20200606T060640_021909_029944_4C69.SAFE
     /eodata/Sentinel-1/SAR/GRD/2020/07/29/S1B_IW_GRDH_1SDV_20200729T172345_20200729T172410_022689_02B10A_E12B.SAFE

    @param tmp_path:
    @param spatial_extent:
    @param temporal_extent:
    @param expected_shape:
    @return:
    """

    extract_product(
        "/data/MTDA/CGS_S1/CGS_S1_GRD_L1/IW/HR/DV/2020/06/06/S1B_IW_GRDH_1SDV_20200606T060615_20200606T060640_021909_029944_4C69/S1B_IW_GRDH_1SDV_20200606T060615_20200606T060640_021909_029944_4C69.zip",
        "/eodata/Sentinel-1/SAR/GRD/2020/06/06"
    )
    extract_product(
        "/data/MTDA/CGS_S1/CGS_S1_GRD_L1/IW/HR/DV/2020/07/29/S1B_IW_GRDH_1SDV_20200729T172345_20200729T172410_022689_02B10A_E12B/S1B_IW_GRDH_1SDV_20200729T172345_20200729T172410_022689_02B10A_E12B.zip",
        "/eodata/Sentinel-1/SAR/GRD/2020/07/29"
    )
    from openeogeotrellis.layercatalog import GeoPySparkLayerCatalog
    catalog = GeoPySparkLayerCatalog(all_metadata=[{
        "id": "Creodias-S1-Backscatter",
        "_vito": {"data_source": {"type": 'creodias-s1-backscatter',"sar_backscatter_compatible":True}},
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x", "reference_system": {"$schema":"https://proj.org/schemas/v0.2/projjson.schema.json","type":"GeodeticCRS","name":"AUTO 42001 (Universal Transverse Mercator)","datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984","ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"area":"World","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"OGC","version":"1.3","code":"Auto42001"}}, "step": 10},
            "y": {"type": "spatial", "axis": "y", "reference_system": {"$schema":"https://proj.org/schemas/v0.2/projjson.schema.json","type":"GeodeticCRS","name":"AUTO 42001 (Universal Transverse Mercator)","datum":{"type":"GeodeticReferenceFrame","name":"World Geodetic System 1984","ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"area":"World","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"OGC","version":"1.3","code":"Auto42001"}}, "step": 10},
            "t": {"type": "temporal"},
            "bands": {"type": "bands", "values": ["VH", "VV"]}
        }
    }])

    load_params = LoadParameters(
        temporal_extent=temporal_extent, spatial_extent=spatial_extent,
        sar_backscatter=SarBackscatterArgs(
            coefficient="sigma0-ellipsoid",
            elevation_model="off",
            options={"dem_zoom_level": 6,"implementation_version":implementation_version,"debug":True}
        )
    )
    datacube = catalog.load_collection("Creodias-S1-Backscatter", load_params=load_params, env=EvalEnv())

    filename = tmp_path / "s1backscatter.tiff"
    datacube.save_result(filename, format="GTiff", format_options={'stitch': True})

    from urllib.request import urlretrieve
    expected_result = None
    expected_path = tmp_path / "expected.tiff"
    urlretrieve(ref_path, expected_path)
    with rasterio.open(expected_path) as ds_ref:
        expected_result = ds_ref.read()

    with rasterio.open(filename) as ds:
        actual_result = ds.read()
        assert actual_result.shape == expected_shape
        assert expected_result.shape == actual_result.shape
        assert_allclose(expected_result,actual_result)


samples = ('/data/S1A_IW_GRDH_1SDV_20200608T161203_20200608T161228_032928_03D06F_1F44.SAFE', [{'key': {'col': 707, 'row': 1086, 'instant': 1591718614827}, 'key_extent': {'xmin': 1809920.0, 'ymin': 6646280.0, 'xmax': 1812480.0, 'ymax': 6648840.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 702, 'row': 1109, 'instant': 1591718614827}, 'key_extent': {'xmin': 1797120.0, 'ymin': 6587400.0, 'xmax': 1799680.0, 'ymax': 6589960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 693, 'row': 1113, 'instant': 1591718614827}, 'key_extent': {'xmin': 1774080.0, 'ymin': 6577160.0, 'xmax': 1776640.0, 'ymax': 6579720.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 700, 'row': 1114, 'instant': 1591718614827}, 'key_extent': {'xmin': 1792000.0, 'ymin': 6574600.0, 'xmax': 1794560.0, 'ymax': 6577160.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 703, 'row': 1115, 'instant': 1591718614827}, 'key_extent': {'xmin': 1799680.0, 'ymin': 6572040.0, 'xmax': 1802240.0, 'ymax': 6574600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 701, 'row': 1117, 'instant': 1591718614827}, 'key_extent': {'xmin': 1794560.0, 'ymin': 6566920.0, 'xmax': 1797120.0, 'ymax': 6569480.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 701, 'row': 1123, 'instant': 1591718614827}, 'key_extent': {'xmin': 1794560.0, 'ymin': 6551560.0, 'xmax': 1797120.0, 'ymax': 6554120.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 699, 'row': 1142, 'instant': 1591718614827}, 'key_extent': {'xmin': 1789440.0, 'ymin': 6502920.0, 'xmax': 1792000.0, 'ymax': 6505480.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 710, 'row': 1090, 'instant': 1591718614827}, 'key_extent': {'xmin': 1817600.0, 'ymin': 6636040.0, 'xmax': 1820160.0, 'ymax': 6638600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 708, 'row': 1089, 'instant': 1591718614827}, 'key_extent': {'xmin': 1812480.0, 'ymin': 6638600.0, 'xmax': 1815040.0, 'ymax': 6641160.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 713, 'row': 1094, 'instant': 1591718614827}, 'key_extent': {'xmin': 1825280.0, 'ymin': 6625800.0, 'xmax': 1827840.0, 'ymax': 6628360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 718, 'row': 1090, 'instant': 1591718614827}, 'key_extent': {'xmin': 1838080.0, 'ymin': 6636040.0, 'xmax': 1840640.0, 'ymax': 6638600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 710, 'row': 1097, 'instant': 1591718614827}, 'key_extent': {'xmin': 1817600.0, 'ymin': 6618120.0, 'xmax': 1820160.0, 'ymax': 6620680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 711, 'row': 1096, 'instant': 1591718614827}, 'key_extent': {'xmin': 1820160.0, 'ymin': 6620680.0, 'xmax': 1822720.0, 'ymax': 6623240.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 719, 'row': 1103, 'instant': 1591718614827}, 'key_extent': {'xmin': 1840640.0, 'ymin': 6602760.0, 'xmax': 1843200.0, 'ymax': 6605320.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 721, 'row': 1100, 'instant': 1591718614827}, 'key_extent': {'xmin': 1845760.0, 'ymin': 6610440.0, 'xmax': 1848320.0, 'ymax': 6613000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 728, 'row': 1100, 'instant': 1591718614827}, 'key_extent': {'xmin': 1863680.0, 'ymin': 6610440.0, 'xmax': 1866240.0, 'ymax': 6613000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 707, 'row': 1107, 'instant': 1591718614827}, 'key_extent': {'xmin': 1809920.0, 'ymin': 6592520.0, 'xmax': 1812480.0, 'ymax': 6595080.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 708, 'row': 1111, 'instant': 1591718614827}, 'key_extent': {'xmin': 1812480.0, 'ymin': 6582280.0, 'xmax': 1815040.0, 'ymax': 6584840.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 716, 'row': 1104, 'instant': 1591718614827}, 'key_extent': {'xmin': 1832960.0, 'ymin': 6600200.0, 'xmax': 1835520.0, 'ymax': 6602760.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 711, 'row': 1119, 'instant': 1591718614827}, 'key_extent': {'xmin': 1820160.0, 'ymin': 6561800.0, 'xmax': 1822720.0, 'ymax': 6564360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 711, 'row': 1113, 'instant': 1591718614827}, 'key_extent': {'xmin': 1820160.0, 'ymin': 6577160.0, 'xmax': 1822720.0, 'ymax': 6579720.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 715, 'row': 1119, 'instant': 1591718614827}, 'key_extent': {'xmin': 1830400.0, 'ymin': 6561800.0, 'xmax': 1832960.0, 'ymax': 6564360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 715, 'row': 1117, 'instant': 1591718614827}, 'key_extent': {'xmin': 1830400.0, 'ymin': 6566920.0, 'xmax': 1832960.0, 'ymax': 6569480.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 724, 'row': 1108, 'instant': 1591718614827}, 'key_extent': {'xmin': 1853440.0, 'ymin': 6589960.0, 'xmax': 1856000.0, 'ymax': 6592520.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 724, 'row': 1109, 'instant': 1591718614827}, 'key_extent': {'xmin': 1853440.0, 'ymin': 6587400.0, 'xmax': 1856000.0, 'ymax': 6589960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 721, 'row': 1107, 'instant': 1591718614827}, 'key_extent': {'xmin': 1845760.0, 'ymin': 6592520.0, 'xmax': 1848320.0, 'ymax': 6595080.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 725, 'row': 1111, 'instant': 1591718614827}, 'key_extent': {'xmin': 1856000.0, 'ymin': 6582280.0, 'xmax': 1858560.0, 'ymax': 6584840.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 733, 'row': 1104, 'instant': 1591718614827}, 'key_extent': {'xmin': 1876480.0, 'ymin': 6600200.0, 'xmax': 1879040.0, 'ymax': 6602760.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 732, 'row': 1104, 'instant': 1591718614827}, 'key_extent': {'xmin': 1873920.0, 'ymin': 6600200.0, 'xmax': 1876480.0, 'ymax': 6602760.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 728, 'row': 1113, 'instant': 1591718614827}, 'key_extent': {'xmin': 1863680.0, 'ymin': 6577160.0, 'xmax': 1866240.0, 'ymax': 6579720.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 731, 'row': 1114, 'instant': 1591718614827}, 'key_extent': {'xmin': 1871360.0, 'ymin': 6574600.0, 'xmax': 1873920.0, 'ymax': 6577160.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 749, 'row': 1094, 'instant': 1591718614827}, 'key_extent': {'xmin': 1917440.0, 'ymin': 6625800.0, 'xmax': 1920000.0, 'ymax': 6628360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 749, 'row': 1092, 'instant': 1591718614827}, 'key_extent': {'xmin': 1917440.0, 'ymin': 6630920.0, 'xmax': 1920000.0, 'ymax': 6633480.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 746, 'row': 1095, 'instant': 1591718614827}, 'key_extent': {'xmin': 1909760.0, 'ymin': 6623240.0, 'xmax': 1912320.0, 'ymax': 6625800.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 749, 'row': 1097, 'instant': 1591718614827}, 'key_extent': {'xmin': 1917440.0, 'ymin': 6618120.0, 'xmax': 1920000.0, 'ymax': 6620680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 749, 'row': 1098, 'instant': 1591718614827}, 'key_extent': {'xmin': 1917440.0, 'ymin': 6615560.0, 'xmax': 1920000.0, 'ymax': 6618120.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 748, 'row': 1101, 'instant': 1591718614827}, 'key_extent': {'xmin': 1914880.0, 'ymin': 6607880.0, 'xmax': 1917440.0, 'ymax': 6610440.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 755, 'row': 1094, 'instant': 1591718614827}, 'key_extent': {'xmin': 1932800.0, 'ymin': 6625800.0, 'xmax': 1935360.0, 'ymax': 6628360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 760, 'row': 1088, 'instant': 1591718614827}, 'key_extent': {'xmin': 1945600.0, 'ymin': 6641160.0, 'xmax': 1948160.0, 'ymax': 6643720.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 758, 'row': 1097, 'instant': 1591718614827}, 'key_extent': {'xmin': 1940480.0, 'ymin': 6618120.0, 'xmax': 1943040.0, 'ymax': 6620680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 763, 'row': 1097, 'instant': 1591718614827}, 'key_extent': {'xmin': 1953280.0, 'ymin': 6618120.0, 'xmax': 1955840.0, 'ymax': 6620680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 740, 'row': 1107, 'instant': 1591718614827}, 'key_extent': {'xmin': 1894400.0, 'ymin': 6592520.0, 'xmax': 1896960.0, 'ymax': 6595080.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 739, 'row': 1109, 'instant': 1591718614827}, 'key_extent': {'xmin': 1891840.0, 'ymin': 6587400.0, 'xmax': 1894400.0, 'ymax': 6589960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 742, 'row': 1107, 'instant': 1591718614827}, 'key_extent': {'xmin': 1899520.0, 'ymin': 6592520.0, 'xmax': 1902080.0, 'ymax': 6595080.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 745, 'row': 1105, 'instant': 1591718614827}, 'key_extent': {'xmin': 1907200.0, 'ymin': 6597640.0, 'xmax': 1909760.0, 'ymax': 6600200.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 745, 'row': 1104, 'instant': 1591718614827}, 'key_extent': {'xmin': 1907200.0, 'ymin': 6600200.0, 'xmax': 1909760.0, 'ymax': 6602760.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 757, 'row': 1106, 'instant': 1591718614827}, 'key_extent': {'xmin': 1937920.0, 'ymin': 6595080.0, 'xmax': 1940480.0, 'ymax': 6597640.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 761, 'row': 1105, 'instant': 1591718614827}, 'key_extent': {'xmin': 1948160.0, 'ymin': 6597640.0, 'xmax': 1950720.0, 'ymax': 6600200.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 764, 'row': 1117, 'instant': 1591718614827}, 'key_extent': {'xmin': 1955840.0, 'ymin': 6566920.0, 'xmax': 1958400.0, 'ymax': 6569480.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 709, 'row': 1121, 'instant': 1591718614827}, 'key_extent': {'xmin': 1815040.0, 'ymin': 6556680.0, 'xmax': 1817600.0, 'ymax': 6559240.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 704, 'row': 1124, 'instant': 1591718614827}, 'key_extent': {'xmin': 1802240.0, 'ymin': 6549000.0, 'xmax': 1804800.0, 'ymax': 6551560.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 710, 'row': 1126, 'instant': 1591718614827}, 'key_extent': {'xmin': 1817600.0, 'ymin': 6543880.0, 'xmax': 1820160.0, 'ymax': 6546440.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 708, 'row': 1120, 'instant': 1591718614827}, 'key_extent': {'xmin': 1812480.0, 'ymin': 6559240.0, 'xmax': 1815040.0, 'ymax': 6561800.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 718, 'row': 1125, 'instant': 1591718614827}, 'key_extent': {'xmin': 1838080.0, 'ymin': 6546440.0, 'xmax': 1840640.0, 'ymax': 6549000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 704, 'row': 1134, 'instant': 1591718614827}, 'key_extent': {'xmin': 1802240.0, 'ymin': 6523400.0, 'xmax': 1804800.0, 'ymax': 6525960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 711, 'row': 1134, 'instant': 1591718614827}, 'key_extent': {'xmin': 1820160.0, 'ymin': 6523400.0, 'xmax': 1822720.0, 'ymax': 6525960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 714, 'row': 1133, 'instant': 1591718614827}, 'key_extent': {'xmin': 1827840.0, 'ymin': 6525960.0, 'xmax': 1830400.0, 'ymax': 6528520.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 726, 'row': 1122, 'instant': 1591718614827}, 'key_extent': {'xmin': 1858560.0, 'ymin': 6554120.0, 'xmax': 1861120.0, 'ymax': 6556680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 734, 'row': 1121, 'instant': 1591718614827}, 'key_extent': {'xmin': 1879040.0, 'ymin': 6556680.0, 'xmax': 1881600.0, 'ymax': 6559240.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 727, 'row': 1131, 'instant': 1591718614827}, 'key_extent': {'xmin': 1861120.0, 'ymin': 6531080.0, 'xmax': 1863680.0, 'ymax': 6533640.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 726, 'row': 1130, 'instant': 1591718614827}, 'key_extent': {'xmin': 1858560.0, 'ymin': 6533640.0, 'xmax': 1861120.0, 'ymax': 6536200.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 733, 'row': 1132, 'instant': 1591718614827}, 'key_extent': {'xmin': 1876480.0, 'ymin': 6528520.0, 'xmax': 1879040.0, 'ymax': 6531080.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 707, 'row': 1143, 'instant': 1591718614827}, 'key_extent': {'xmin': 1809920.0, 'ymin': 6500360.0, 'xmax': 1812480.0, 'ymax': 6502920.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 704, 'row': 1151, 'instant': 1591718614827}, 'key_extent': {'xmin': 1802240.0, 'ymin': 6479880.0, 'xmax': 1804800.0, 'ymax': 6482440.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 710, 'row': 1148, 'instant': 1591718614827}, 'key_extent': {'xmin': 1817600.0, 'ymin': 6487560.0, 'xmax': 1820160.0, 'ymax': 6490120.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 722, 'row': 1144, 'instant': 1591718614827}, 'key_extent': {'xmin': 1848320.0, 'ymin': 6497800.0, 'xmax': 1850880.0, 'ymax': 6500360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 743, 'row': 1125, 'instant': 1591718614827}, 'key_extent': {'xmin': 1902080.0, 'ymin': 6546440.0, 'xmax': 1904640.0, 'ymax': 6549000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 738, 'row': 1120, 'instant': 1591718614827}, 'key_extent': {'xmin': 1889280.0, 'ymin': 6559240.0, 'xmax': 1891840.0, 'ymax': 6561800.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 739, 'row': 1125, 'instant': 1591718614827}, 'key_extent': {'xmin': 1891840.0, 'ymin': 6546440.0, 'xmax': 1894400.0, 'ymax': 6549000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1125, 'instant': 1591718614827}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6546440.0, 'xmax': 1925120.0, 'ymax': 6549000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 738, 'row': 1135, 'instant': 1591718614827}, 'key_extent': {'xmin': 1889280.0, 'ymin': 6520840.0, 'xmax': 1891840.0, 'ymax': 6523400.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 747, 'row': 1134, 'instant': 1591718614827}, 'key_extent': {'xmin': 1912320.0, 'ymin': 6523400.0, 'xmax': 1914880.0, 'ymax': 6525960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 744, 'row': 1131, 'instant': 1591718614827}, 'key_extent': {'xmin': 1904640.0, 'ymin': 6531080.0, 'xmax': 1907200.0, 'ymax': 6533640.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 756, 'row': 1123, 'instant': 1591718614827}, 'key_extent': {'xmin': 1935360.0, 'ymin': 6551560.0, 'xmax': 1937920.0, 'ymax': 6554120.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 761, 'row': 1128, 'instant': 1591718614827}, 'key_extent': {'xmin': 1948160.0, 'ymin': 6538760.0, 'xmax': 1950720.0, 'ymax': 6541320.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 743, 'row': 1143, 'instant': 1591718614827}, 'key_extent': {'xmin': 1902080.0, 'ymin': 6500360.0, 'xmax': 1904640.0, 'ymax': 6502920.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 750, 'row': 1138, 'instant': 1591718614827}, 'key_extent': {'xmin': 1920000.0, 'ymin': 6513160.0, 'xmax': 1922560.0, 'ymax': 6515720.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 750, 'row': 1151, 'instant': 1591718614827}, 'key_extent': {'xmin': 1920000.0, 'ymin': 6479880.0, 'xmax': 1922560.0, 'ymax': 6482440.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 758, 'row': 1137, 'instant': 1591718614827}, 'key_extent': {'xmin': 1940480.0, 'ymin': 6515720.0, 'xmax': 1943040.0, 'ymax': 6518280.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 760, 'row': 1141, 'instant': 1591718614827}, 'key_extent': {'xmin': 1945600.0, 'ymin': 6505480.0, 'xmax': 1948160.0, 'ymax': 6508040.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 752, 'row': 1146, 'instant': 1591718614827}, 'key_extent': {'xmin': 1925120.0, 'ymin': 6492680.0, 'xmax': 1927680.0, 'ymax': 6495240.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 754, 'row': 1150, 'instant': 1591718614827}, 'key_extent': {'xmin': 1930240.0, 'ymin': 6482440.0, 'xmax': 1932800.0, 'ymax': 6485000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 758, 'row': 1144, 'instant': 1591718614827}, 'key_extent': {'xmin': 1940480.0, 'ymin': 6497800.0, 'xmax': 1943040.0, 'ymax': 6500360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 763, 'row': 1146, 'instant': 1591718614827}, 'key_extent': {'xmin': 1953280.0, 'ymin': 6492680.0, 'xmax': 1955840.0, 'ymax': 6495240.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 764, 'row': 1150, 'instant': 1591718614827}, 'key_extent': {'xmin': 1955840.0, 'ymin': 6482440.0, 'xmax': 1958400.0, 'ymax': 6485000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 767, 'row': 1147, 'instant': 1591718614827}, 'key_extent': {'xmin': 1963520.0, 'ymin': 6490120.0, 'xmax': 1966080.0, 'ymax': 6492680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 765, 'row': 1150, 'instant': 1591718614827}, 'key_extent': {'xmin': 1958400.0, 'ymin': 6482440.0, 'xmax': 1960960.0, 'ymax': 6485000.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 762, 'row': 1144, 'instant': 1591718614827}, 'key_extent': {'xmin': 1950720.0, 'ymin': 6497800.0, 'xmax': 1953280.0, 'ymax': 6500360.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 699, 'row': 1152, 'instant': 1591718614827}, 'key_extent': {'xmin': 1789440.0, 'ymin': 6477320.0, 'xmax': 1792000.0, 'ymax': 6479880.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 707, 'row': 1156, 'instant': 1591718614827}, 'key_extent': {'xmin': 1809920.0, 'ymin': 6467080.0, 'xmax': 1812480.0, 'ymax': 6469640.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 705, 'row': 1161, 'instant': 1591718614827}, 'key_extent': {'xmin': 1804800.0, 'ymin': 6454280.0, 'xmax': 1807360.0, 'ymax': 6456840.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 735, 'row': 1159, 'instant': 1591718614827}, 'key_extent': {'xmin': 1881600.0, 'ymin': 6459400.0, 'xmax': 1884160.0, 'ymax': 6461960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 733, 'row': 1152, 'instant': 1591718614827}, 'key_extent': {'xmin': 1876480.0, 'ymin': 6477320.0, 'xmax': 1879040.0, 'ymax': 6479880.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 736, 'row': 1158, 'instant': 1591718614827}, 'key_extent': {'xmin': 1884160.0, 'ymin': 6461960.0, 'xmax': 1886720.0, 'ymax': 6464520.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 746, 'row': 1158, 'instant': 1591718614827}, 'key_extent': {'xmin': 1909760.0, 'ymin': 6461960.0, 'xmax': 1912320.0, 'ymax': 6464520.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1159, 'instant': 1591718614827}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6459400.0, 'xmax': 1925120.0, 'ymax': 6461960.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1165, 'instant': 1591718614827}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6444040.0, 'xmax': 1925120.0, 'ymax': 6446600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 745, 'row': 1160, 'instant': 1591718614827}, 'key_extent': {'xmin': 1907200.0, 'ymin': 6456840.0, 'xmax': 1909760.0, 'ymax': 6459400.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1160, 'instant': 1591718614827}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6456840.0, 'xmax': 1925120.0, 'ymax': 6459400.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 761, 'row': 1156, 'instant': 1591718614827}, 'key_extent': {'xmin': 1948160.0, 'ymin': 6467080.0, 'xmax': 1950720.0, 'ymax': 6469640.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 753, 'row': 1164, 'instant': 1591718614827}, 'key_extent': {'xmin': 1927680.0, 'ymin': 6446600.0, 'xmax': 1930240.0, 'ymax': 6449160.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 758, 'row': 1165, 'instant': 1591718614827}, 'key_extent': {'xmin': 1940480.0, 'ymin': 6444040.0, 'xmax': 1943040.0, 'ymax': 6446600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 754, 'row': 1162, 'instant': 1591718614827}, 'key_extent': {'xmin': 1930240.0, 'ymin': 6451720.0, 'xmax': 1932800.0, 'ymax': 6454280.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 759, 'row': 1165, 'instant': 1591718614827}, 'key_extent': {'xmin': 1943040.0, 'ymin': 6444040.0, 'xmax': 1945600.0, 'ymax': 6446600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 757, 'row': 1160, 'instant': 1591718614827}, 'key_extent': {'xmin': 1937920.0, 'ymin': 6456840.0, 'xmax': 1940480.0, 'ymax': 6459400.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 754, 'row': 1165, 'instant': 1591718614827}, 'key_extent': {'xmin': 1930240.0, 'ymin': 6444040.0, 'xmax': 1932800.0, 'ymax': 6446600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 767, 'row': 1160, 'instant': 1591718614827}, 'key_extent': {'xmin': 1963520.0, 'ymin': 6456840.0, 'xmax': 1966080.0, 'ymax': 6459400.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 767, 'row': 1161, 'instant': 1591718614827}, 'key_extent': {'xmin': 1963520.0, 'ymin': 6454280.0, 'xmax': 1966080.0, 'ymax': 6456840.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 765, 'row': 1170, 'instant': 1591718614827}, 'key_extent': {'xmin': 1958400.0, 'ymin': 6431240.0, 'xmax': 1960960.0, 'ymax': 6433800.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 761, 'row': 1172, 'instant': 1591718614827}, 'key_extent': {'xmin': 1948160.0, 'ymin': 6426120.0, 'xmax': 1950720.0, 'ymax': 6428680.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 768, 'row': 1115, 'instant': 1591718614827}, 'key_extent': {'xmin': 1966080.0, 'ymin': 6572040.0, 'xmax': 1968640.0, 'ymax': 6574600.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 768, 'row': 1114, 'instant': 1591718614827}, 'key_extent': {'xmin': 1966080.0, 'ymin': 6574600.0, 'xmax': 1968640.0, 'ymax': 6577160.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 768, 'row': 1126, 'instant': 1591718614827}, 'key_extent': {'xmin': 1966080.0, 'ymin': 6543880.0, 'xmax': 1968640.0, 'ymax': 6546440.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}, {'key': {'col': 768, 'row': 1149, 'instant': 1591718614827}, 'key_extent': {'xmin': 1966080.0, 'ymin': 6485000.0, 'xmax': 1968640.0, 'ymax': 6487560.0}, 'bbox': {'xmin': 22.18062, 'ymin': 56.327839, 'xmax': 27.021925, 'ymax': 58.244179}, 'key_epsg': 32631}])
samples = ('/data/S1A_IW_GRDH_1SDV_20200608T161203_20200608T161228_032928_03D06F_1F44.SAFE', [{'key': {'col': 699, 'row': 1142, 'instant': 1591632723727}, 'key_extent': {'xmin': 1789440.0, 'ymin': 6502920.0, 'xmax': 1792000.0, 'ymax': 6505480.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 707, 'row': 1143, 'instant': 1591632723727}, 'key_extent': {'xmin': 1809920.0, 'ymin': 6500360.0, 'xmax': 1812480.0, 'ymax': 6502920.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 704, 'row': 1151, 'instant': 1591632723727}, 'key_extent': {'xmin': 1802240.0, 'ymin': 6479880.0, 'xmax': 1804800.0, 'ymax': 6482440.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 710, 'row': 1148, 'instant': 1591632723727}, 'key_extent': {'xmin': 1817600.0, 'ymin': 6487560.0, 'xmax': 1820160.0, 'ymax': 6490120.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 722, 'row': 1144, 'instant': 1591632723727}, 'key_extent': {'xmin': 1848320.0, 'ymin': 6497800.0, 'xmax': 1850880.0, 'ymax': 6500360.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 743, 'row': 1143, 'instant': 1591632723727}, 'key_extent': {'xmin': 1902080.0, 'ymin': 6500360.0, 'xmax': 1904640.0, 'ymax': 6502920.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 750, 'row': 1151, 'instant': 1591632723727}, 'key_extent': {'xmin': 1920000.0, 'ymin': 6479880.0, 'xmax': 1922560.0, 'ymax': 6482440.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 752, 'row': 1146, 'instant': 1591632723727}, 'key_extent': {'xmin': 1925120.0, 'ymin': 6492680.0, 'xmax': 1927680.0, 'ymax': 6495240.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 699, 'row': 1152, 'instant': 1591632723727}, 'key_extent': {'xmin': 1789440.0, 'ymin': 6477320.0, 'xmax': 1792000.0, 'ymax': 6479880.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 707, 'row': 1156, 'instant': 1591632723727}, 'key_extent': {'xmin': 1809920.0, 'ymin': 6467080.0, 'xmax': 1812480.0, 'ymax': 6469640.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 705, 'row': 1161, 'instant': 1591632723727}, 'key_extent': {'xmin': 1804800.0, 'ymin': 6454280.0, 'xmax': 1807360.0, 'ymax': 6456840.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 735, 'row': 1159, 'instant': 1591632723727}, 'key_extent': {'xmin': 1881600.0, 'ymin': 6459400.0, 'xmax': 1884160.0, 'ymax': 6461960.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 733, 'row': 1152, 'instant': 1591632723727}, 'key_extent': {'xmin': 1876480.0, 'ymin': 6477320.0, 'xmax': 1879040.0, 'ymax': 6479880.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 736, 'row': 1158, 'instant': 1591632723727}, 'key_extent': {'xmin': 1884160.0, 'ymin': 6461960.0, 'xmax': 1886720.0, 'ymax': 6464520.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 746, 'row': 1158, 'instant': 1591632723727}, 'key_extent': {'xmin': 1909760.0, 'ymin': 6461960.0, 'xmax': 1912320.0, 'ymax': 6464520.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1159, 'instant': 1591632723727}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6459400.0, 'xmax': 1925120.0, 'ymax': 6461960.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1165, 'instant': 1591632723727}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6444040.0, 'xmax': 1925120.0, 'ymax': 6446600.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 745, 'row': 1160, 'instant': 1591632723727}, 'key_extent': {'xmin': 1907200.0, 'ymin': 6456840.0, 'xmax': 1909760.0, 'ymax': 6459400.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 751, 'row': 1160, 'instant': 1591632723727}, 'key_extent': {'xmin': 1922560.0, 'ymin': 6456840.0, 'xmax': 1925120.0, 'ymax': 6459400.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}, {'key': {'col': 753, 'row': 1164, 'instant': 1591632723727}, 'key_extent': {'xmin': 1927680.0, 'ymin': 6446600.0, 'xmax': 1930240.0, 'ymax': 6449160.0}, 'bbox': {'xmin': 20.742722, 'ymin': 54.748413, 'xmax': 25.398483, 'ymax': 56.663048}, 'key_epsg': 32631}])
def test_run_orfeo_on_samples(tmp_path):
    pytest.skip("requires input data")
    args = SarBackscatterArgs(
        coefficient="sigma0-ellipsoid",

        options={"dem_zoom_level": 6, "implementation_version": "1", "debug": False, "tile_size":256}
    )
    features = samples[1]

    orfeo_function = S1BackscatterOrfeo._get_process_function(args, "float32", ["VH", "VV"])
    orfeo_function(samples)



def test_run_orfeo_on_single(tmp_path):
    pytest.skip("requires input data")
    #import faulthandler
    #faulthandler.enable()
    creo_path = Path("/data/S1B_IW_GRDH_1SDV_20191113T155500_20191113T155514_018911_023AAD_5A9C.SAFE")
    log_prefix = "bla"
    bands = ["VH"]
    band_tiffs = S1BackscatterOrfeo._creo_scan_for_band_tiffs(creo_path, log_prefix)
    tile_size = 256
    noise_removal=True
    key_epsg = 32635
    key_ext = {'xmin': 440320.0, 'ymin': 6203400.0, 'xmax': 442880.0, 'ymax': 6205960.0}


    for b, band in enumerate(bands):

        data, nodata = S1BackscatterOrfeo._orfeo_pipeline(
            input_tiff=band_tiffs[band.lower()],
            extent=key_ext, extent_epsg=key_epsg,
            dem_dir=None,
            extent_width_px=tile_size, extent_height_px=tile_size,
            sar_calibration_lut="sigma",
            noise_removal=noise_removal,
            elev_geoid="/opt/openeo-vito-aux-data/egm96.grd", elev_default=10,
            log_prefix=f"{log_prefix}-{band}"
        )
        print(data)



def test_run_orfeo(tmp_path):
    input = Path("/data/MTDA/CGS_S1/CGS_S1_GRD_L1/IW/HR/DV/2021/05/17/S1B_IW_GRDH_1SDV_20210517T054123_20210517T054148_026940_0337F2_2CE0/S1B_IW_GRDH_1SDV_20210517T054123_20210517T054148_026940_0337F2_2CE0.zip")
    target_location = r's1_grd'

    extract_product(input, target_location)

    extent = {'xmin':697534, 'ymin':5865437-80000, 'xmax':697534 + 10000, 'ymax':5865437-40000 }
    data, nodata = S1BackscatterOrfeoV2._orfeo_pipeline(
        input_tiff=Path("s1_grd/S1B_IW_GRDH_1SDV_20210517T054123_20210517T054148_026940_0337F2_2CE0.SAFE/measurement/s1b-iw-grd-vh-20210517t054123-20210517t054148-026940-0337f2-002.tiff"),
        extent=extent, extent_epsg=32631,
        dem_dir=None,
        extent_width_px=100, extent_height_px=120,
        sar_calibration_lut="gamma",
        noise_removal=True,
        elev_geoid=None, elev_default=0,
        log_prefix="test",
        orfeo_memory=512
    )

    #orfeo data is returned (y,x)
    assert (120,100) == data.shape


def extract_product(input, target_location):
    import zipfile
    import os
    with zipfile.ZipFile(input) as zip_file:
        for member in zip_file.namelist():
            if os.path.exists(target_location + r'/' + member) or os.path.isfile(target_location + r'/' + member):
                print('Error: ', member, ' exists.')
            else:
                zip_file.extract(member, target_location)


@pytest.mark.parametrize(["bbox", "bbox_epsg"], [
    ((3.1, 51.2, 3.5, 51.3), 4326),
    ((506986, 5672070, 534857, 5683305), 32631),
])
def test_creodias_dem_subset_geotiff(bbox, bbox_epsg):
    dirs = set()
    symlinks = {}
    with S1BackscatterOrfeo._creodias_dem_subset_geotiff(
            bbox=bbox, bbox_epsg=bbox_epsg, zoom=11,
            dem_path_tpl="/path/to/geotiff/{z}/{x}/{y}.tif"
    ) as temp_dir:
        temp_dir = Path(temp_dir)
        for path in temp_dir.glob("**/*"):
            relative = path.relative_to(temp_dir)
            if path.is_dir():
                dirs.add(str(relative))
            elif path.is_symlink():
                symlinks[str(relative)] = os.readlink(path)
            else:
                raise ValueError(path)
    assert dirs == {"11", "11/1041", "11/1042", "11/1043"}
    assert symlinks == {
        "11/1041/682.tif": "/path/to/geotiff/11/1041/682.tif",
        "11/1041/683.tif": "/path/to/geotiff/11/1041/683.tif",
        "11/1042/682.tif": "/path/to/geotiff/11/1042/682.tif",
        "11/1042/683.tif": "/path/to/geotiff/11/1042/683.tif",
        "11/1043/682.tif": "/path/to/geotiff/11/1043/682.tif",
        "11/1043/683.tif": "/path/to/geotiff/11/1043/683.tif",
    }
    assert not temp_dir.exists()


@pytest.mark.parametrize(["bbox", "bbox_epsg", "expected"], [
    ((3.1, 51.2, 3.5, 51.3), 4326, {"N51E003.hgt"}),
    ((506986, 5672070, 534857, 5683305), 32631, {"N51E003.hgt"}),
    ((3.8, 51.9, 4.2, 52.1), 4326, {"N51E003.hgt", "N51E004.hgt", "N52E003.hgt", "N52E004.hgt", }),
    ((-3.1, -51.2, -3.5, -51.3), 4326, {"S51W003.hgt"}),
])
def test_creodias_dem_subset_srtm_hgt_unzip(bbox, bbox_epsg, expected, tmp_path):
    # Set up fake zips.
    for name in ["N51E003", "N51E004", "N52E003", "N52E004", "S51W003"]:
        with zipfile.ZipFile(tmp_path / f"{name}.SRTMGL1.hgt.zip", mode="w") as z:
            with z.open(f"{name}.hgt", mode="w") as f:
                f.write(b"hgt data here")

    # Check _creodias_dem_subset_srtm_hgt_unzip
    with S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
            bbox=bbox, bbox_epsg=bbox_epsg, srtm_root=str(tmp_path)
    ) as temp_dem_dir:
        temp_dem_dir = Path(temp_dem_dir)
        assert set(os.listdir(temp_dem_dir)) == expected


def test_import_orfeo_toolbox(tmp_path, caplog):
    try:
        import otbApplication
        pytest.skip("`import otbApplication` works directly, so we can't (and don't need to) test the fallback mechanism.")
    except ImportError:
        pass

    # Set up fake otbApplication module
    otb_home = tmp_path / "otb_home"
    otb_module_path = otb_home / "lib/otb/python"
    otb_module_path.mkdir(parents=True)
    otb_module_src = textwrap.dedent("""\
        import os
        foo = os.environ["OTB_APPLICATION_PATH"]
    """)
    with (otb_module_path / "otbApplication.py").open("w") as f:
        f.write(otb_module_src)

    # Try importing (in a subprocess with isolated state regarding imported modules)
    test_script = textwrap.dedent("""
        from openeogeotrellis.collections.s1backscatter_orfeo import _import_orfeo_toolbox
        otb = _import_orfeo_toolbox()
        print(otb.foo)
    """)
    env = {**os.environ, **{"OTB_HOME": str(otb_home)}}
    p = subprocess.run(
        [sys.executable, "-c", test_script],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    expected_stdout = f"{otb_home}/lib/otb/applications\n"
    assert (p.returncode, p.stdout, p.stderr) == (0, expected_stdout.encode("utf8"), b"")


def test_instant_ms_to_day():
    assert _instant_ms_to_day(1479249799770) == datetime.datetime(2016, 11, 15)
