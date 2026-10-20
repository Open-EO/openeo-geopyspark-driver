"""
Focused unit tests for `openeogeotrellis.stac.assets`, exercised through its
own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import pystac
import pytest

from openeogeotrellis.stac.assets import is_band_asset, is_supported_raster_mime_type


def test_is_supported_raster_mime_type():
    assert is_supported_raster_mime_type("image/tiff; application=geotiff")
    assert is_supported_raster_mime_type("image/tiff; application=geotiff; profile=cloud-optimized")
    assert is_supported_raster_mime_type("image/jp2")
    assert is_supported_raster_mime_type("application/x-hdf5")
    assert is_supported_raster_mime_type("application/x-hdf")
    assert not is_supported_raster_mime_type("text/html")


@pytest.mark.parametrize(
    ["data", "expected"],
    [
        ({"href": "https://stac.test/asset.tif"}, False),
        ({"href": "https://stac.test/asset.tif", "roles": ["data"]}, True),
        ({"href": "https://stac.test/asset.tif", "roles": ["data"], "type": "image/tiff; application=geotiff"}, True),
        ({"href": "https://stac.test/asset.tif", "type": "image/tiff; application=geotiff"}, False),
        ({"href": "https://stac.test/asset.tif", "type": "image/vnd.stac.geotiff; cloud-optimized=true"}, True),
        ({"href": "https://stac.test/asset.html", "roles": ["data"], "type": "text/html"}, False),
        ({"href": "https://stac.test/asset.png", "roles": ["thumbnail"]}, False),
        ({"href": "https://stac.test/asset.png", "bands": [{"name": "B02"}]}, True),
        ({"href": "https://stac.test/asset.png", "eo:bands": [{"name": "B02"}]}, True),
        ({"href": "https://stac.test/asset.png", "roles": [], "bands": [{"name": "B02"}]}, True),
    ],
)
def test_is_band_asset(data, expected):
    asset = pystac.Asset.from_dict(data)
    assert is_band_asset(asset) == expected
