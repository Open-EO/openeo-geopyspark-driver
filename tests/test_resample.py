import datetime
import math
import geopyspark as gps
import pytest
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube


def test_resample_cube_spatial_single_level(imagecollection_with_two_bands_and_three_dates,imagecollection_with_two_bands_and_three_dates_webmerc):
    #print(imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch())
    resampled = imagecollection_with_two_bands_and_three_dates.resample_cube_spatial(imagecollection_with_two_bands_and_three_dates_webmerc,method='cube')

    assert resampled.pyramid.levels[0].layer_metadata.crs == '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs '

    stitched = resampled.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch()
    print(stitched)
    assert stitched.cells[0][0][0] == 1.0
    assert stitched.cells[1][0][0] == 2.0


def test_resample__spatial_single_level(imagecollection_with_two_bands_and_three_dates, tmp_path):
    #print(imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch())
    resampled = imagecollection_with_two_bands_and_three_dates.resample_spatial(resolution=100000,projection=3857)
    crs:str = resampled.pyramid.levels[0].layer_metadata.crs
    assert crs.startswith('+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m')

    path = tmp_path / "resampled.tiff"
    resampled.save_result(path, format="GTIFF")
    from osgeo.gdal import Info
    info = Info(str(path), format='json')
    print(info)
    assert math.floor(info['geoTransform'][1]) == 111319.0 #new resolution is rather approximate for some reason?

def test_compute_new_layout():

    extent = gps.Extent(xmin=32.7901792, ymin=3.01934520952576, xmax=48.79017919999744, ymax=15.2098214)
    target_res = 0.008928571428569999
    newLayout = GeopysparkDataCube._layout_for_resolution(extent, gps.TileLayout(layoutCols=21, layoutRows=16, tileCols=256, tileRows=256), None,
                                                          target_res)

    newExtent = newLayout.extent
    width = newExtent.xmax - newExtent.xmin
    height = newExtent.ymax - newExtent.ymin

    tileLayout = newLayout.tileLayout
    currentResolutionX = width / (tileLayout.tileCols * tileLayout.layoutCols)
    currentResolutionY = height / (tileLayout.tileRows * tileLayout.layoutRows)
    assert currentResolutionX == pytest.approx(target_res, 0.00000001)
    assert currentResolutionY == pytest.approx(target_res, 0.00000001)
