import datetime


def test_resample_cube_spatial_single_level(imagecollection_with_two_bands_and_three_dates,imagecollection_with_two_bands_and_three_dates_webmerc):
    #print(imagecollection_with_two_bands_and_three_dates.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch())
    resampled = imagecollection_with_two_bands_and_three_dates.resample_cube_spatial(imagecollection_with_two_bands_and_three_dates_webmerc,method='cube')

    assert resampled.pyramid.levels[0].layer_metadata.crs == '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs '

    stitched = resampled.pyramid.levels[0].to_spatial_layer(datetime.datetime(2017, 9, 25, 11, 37)).stitch()
    print(stitched)
    assert stitched.cells[0][0][0] == 1.0
    assert stitched.cells[1][0][0] == 2.0