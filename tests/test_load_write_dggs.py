


def test_load_write_dggs(api100):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("OpenEO-GeoPySpark-Driver-Tests") \
        .getOrCreate()
    response = api100.check_result({
        "lc": {
            "process_id": "load_stac",
            "arguments": {
                "url": "https://stac.dataspace.copernicus.eu/v1/collections/sentinel-3-sl-2-lst-ntc",
                "temporal_extent": ["2021-01-01", "2021-01-10"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 1.0},
                "bands": ["LST_in"],
                "featureflags": {"dggs": {"latVariable":"latitude_in", "lonVariable":"longitude_in", "geoFileSuffix":"geodetic_in.nc", "resolution": 4, "cell_type": "raster", "assetVariables": {"LST_in": ["LST"]}}},
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "lc"}, "format": "ZARR"},
            "result": True,
        }
    })
    result = response.assert_status_code(200).json

