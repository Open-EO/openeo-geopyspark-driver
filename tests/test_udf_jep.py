from pathlib import Path

from osgeo import gdal

import openeo

UDF_CODE_PATH = Path(__file__).parent / "udf_code"


def _get_udf_code(filename: str):
    with (UDF_CODE_PATH / filename).open("r") as f:
        return f.read()


def test_run_udf_jep_with_apply_metadata(api100):
    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)

    spatial_extent = {"west": 0, "south": 0, "east": 8, "north": 8}
    datacube = openeo.DataCube.load_collection(
        "TestCollection-LonLat4x4",
        temporal_extent=["2020-01-01", "2020-01-09"],
        spatial_extent=spatial_extent,
        bands=["Longitude"],
        fetch_metadata=False,
    )
    udf_code = _get_udf_code("metadata_udf.py")
    datacube = datacube.apply_neighborhood(
        lambda data: data.run_udf(udf=udf_code, runtime="Python-Jep", context=dict()),
        size=[{"dimension": "x", "value": 32, "unit": "px"}, {"dimension": "y", "value": 32, "unit": "px"}],
        overlap=[],
    )
    result = api100.check_result(datacube)

    with open(output_dir / "test_run_udf_jep_with_apply_metadata.tiff", mode="wb") as f:
        f.write(result.data)

    data_gdalinfo = gdal.Info(str(output_dir / "test_run_udf_jep_code.tiff"))
    assert "Longitude_RENAMED" in data_gdalinfo
