from openeo_driver.util.geometry import BoundingBox
from openeogeotrellis.testing import create_dummy_geotiff

if __name__ == "__main__":
    create_dummy_geotiff(
        "asset-1.tiff",
        bbox=BoundingBox(2, 49, 3, 50, crs="EPSG:4326"),
        shape=(32, 32),
    )
    create_dummy_geotiff(
        "asset-2.tiff",
        bbox=BoundingBox(3, 50, 5, 51, crs="EPSG:4326"),
        shape=(2 * 32, 32),
    )
    create_dummy_geotiff(
        "asset-3.tiff",
        bbox=BoundingBox(4, 51, 7, 52, crs="EPSG:4326"),
        shape=(3 * 32, 32),
    )
