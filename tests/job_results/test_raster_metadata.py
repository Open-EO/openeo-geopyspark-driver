import json

import pytest

from openeogeotrellis.job_results.raster_metadata import AssetRasterMetadata, _get_raster_statistics


@pytest.mark.parametrize("valid_percent", ["nan", "inf", "-inf"])
def test_gdal_invalid_band_statistics(valid_percent):
    gdal_info = {
        "bands": [
            {
                "band": 1,
                "metadata": {
                    "": {
                        "long_name": "B02",
                        "STATISTICS_MINIMUM": "-999",
                        "STATISTICS_MAXIMUM": "23208",
                        "STATISTICS_MEAN": "-5857.25",
                        "STATISTICS_STDDEV": "nan",
                        "STATISTICS_VALID_PERCENT": valid_percent,
                    }
                },
            }
        ]
    }
    statistics = _get_raster_statistics(gdal_info)
    metadata = AssetRasterMetadata(gdal_info=gdal_info, statistics=statistics).to_dict()
    assert metadata["raster:bands"] == [
        {
            "name": "B02",
            "statistics": {"minimum": -999.0, "maximum": 23208.0, "mean": -5857.25},
        }
    ]
    json.dumps(metadata, allow_nan=False)


@pytest.mark.parametrize("valid_percent", ["0", "53.125", "100", "-1", "100.88479289410685"])
def test_gdal_finite_band_statistics_are_preserved(valid_percent):
    gdal_info = {
        "bands": [
            {
                "band": 1,
                "metadata": {"": {"STATISTICS_VALID_PERCENT": valid_percent, "STATISTICS_STDDEV": "0"}},
            }
        ]
    }
    metadata = AssetRasterMetadata(gdal_info=gdal_info, statistics=_get_raster_statistics(gdal_info)).to_dict()
    assert metadata["raster:bands"] == [
        {
            "name": "1",
            "statistics": {"stddev": 0.0, "valid_percent": float(valid_percent)},
        }
    ]
