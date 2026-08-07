"""Tests for GeopysparkDataCube.run_udf (raster cube to STAC catalog for UDF input)."""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from openeogeotrellis.geopysparkdatacube import _write_stac_catalog


class TestWriteStacCatalog:
    """Test the _write_stac_catalog helper function."""

    def test_write_stac_catalog_basic(self, tmp_path):
        """Test that _write_stac_catalog writes a valid STAC catalog and item files."""
        items = {
            "item1": {
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
                "bbox": [0, 0, 1, 1],
                "properties": {"datetime": "2023-01-01T00:00:00Z"},
                "assets": {
                    "data": {
                        "href": "/tmp/data.tif",
                        "type": "image/tiff; application=geotiff",
                    }
                },
            }
        }

        stac_dir = str(tmp_path)
        _write_stac_catalog(stac_dir, items)

        # Check catalog file
        catalog_path = tmp_path / "collection.json"
        assert catalog_path.exists()
        catalog = json.loads(catalog_path.read_text())
        assert catalog["type"] == "Catalog"
        assert catalog["stac_version"] == "1.0.0"
        assert len(catalog["links"]) == 1
        assert catalog["links"][0]["rel"] == "item"
        assert catalog["links"][0]["href"] == "./item1.json"

        # Check item file
        item_path = tmp_path / "item1.json"
        assert item_path.exists()
        item = json.loads(item_path.read_text())
        assert item["type"] == "Feature"
        assert item["id"] == "item1"
        assert item["stac_version"] == "1.0.0"
        assert item["geometry"] == items["item1"]["geometry"]
        assert item["bbox"] == [0, 0, 1, 1]
        assert item["assets"]["data"]["href"] == "/tmp/data.tif"

    def test_write_stac_catalog_multiple_items(self, tmp_path):
        """Test writing a catalog with multiple items."""
        items = {
            "item_a": {
                "geometry": None,
                "bbox": None,
                "properties": {"datetime": "2023-01-01T00:00:00Z"},
                "assets": {"band1": {"href": "/tmp/a.tif"}},
            },
            "item_b": {
                "geometry": None,
                "bbox": None,
                "properties": {"datetime": "2023-01-02T00:00:00Z"},
                "assets": {"band1": {"href": "/tmp/b.tif"}},
            },
        }

        _write_stac_catalog(str(tmp_path), items)

        catalog = json.loads((tmp_path / "collection.json").read_text())
        assert len(catalog["links"]) == 2

        assert (tmp_path / "item_a.json").exists()
        assert (tmp_path / "item_b.json").exists()

    def test_write_stac_catalog_empty_items(self, tmp_path):
        """Test writing a catalog with no items."""
        _write_stac_catalog(str(tmp_path), {})

        catalog = json.loads((tmp_path / "collection.json").read_text())
        assert catalog["links"] == []

    def test_write_stac_catalog_missing_properties(self, tmp_path):
        """Test that missing properties default gracefully."""
        items = {
            "item1": {
                "assets": {"data": {"href": "/tmp/data.tif"}},
            }
        }

        _write_stac_catalog(str(tmp_path), items)

        item = json.loads((tmp_path / "item1.json").read_text())
        assert item["geometry"] is None
        assert item["bbox"] is None
        assert item["properties"] == {"datetime": None}
        assert item["assets"]["data"]["href"] == "/tmp/data.tif"


class TestGeopysparkDataCubeRunUdf:
    """Test GeopysparkDataCube.supports_udf and run_udf methods."""

    def test_supports_udf_python(self):
        """Test that supports_udf returns True for Python runtime."""
        from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

        cube = MagicMock(spec=GeopysparkDataCube)
        # Call the unbound method
        assert GeopysparkDataCube.supports_udf(cube, "some udf code", runtime="Python")
        assert GeopysparkDataCube.supports_udf(cube, "some udf code", runtime="python")

    def test_supports_udf_non_python(self):
        """Test that supports_udf returns False for non-Python runtime."""
        from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

        cube = MagicMock(spec=GeopysparkDataCube)
        assert not GeopysparkDataCube.supports_udf(cube, "some udf code", runtime="R")

    @patch("openeogeotrellis.geopysparkdatacube.run_udf_code")
    def test_run_udf_materializes_stac_and_passes_url(self, mock_run_udf_code):
        """Test that run_udf materializes datacube to STAC and passes URL to UDF."""
        import openeo.udf
        from openeo_driver.utils import EvalEnv
        from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube

        # Mock return from run_udf_code
        mock_result = MagicMock()
        mock_result.get_structured_data_list.return_value = [
            openeo.udf.StructuredData(data={"result": "processed"}, type="dict")
        ]
        mock_run_udf_code.return_value = mock_result

        # Mock the cube's write_assets method
        cube = MagicMock(spec=GeopysparkDataCube)
        cube.write_assets = MagicMock(
            return_value={
                "item1": {
                    "geometry": None,
                    "bbox": None,
                    "properties": {"datetime": "2023-01-01T00:00:00Z"},
                    "assets": {"data": {"href": "/tmp/fake.tif", "type": "image/tiff"}},
                }
            }
        )

        udf_code = "def apply(data): return data"
        env = EvalEnv()

        # Call the unbound method with our mock
        result = GeopysparkDataCube.run_udf(cube, udf=udf_code, runtime="Python", context={"key": "val"}, env=env)

        # Verify write_assets was called
        cube.write_assets.assert_called_once()
        call_args = cube.write_assets.call_args
        assert call_args[1]["format"] == "GTiff"

        # Verify run_udf_code was called with UdfData containing STAC catalog URL
        mock_run_udf_code.assert_called_once()
        udf_data = mock_run_udf_code.call_args[1]["data"]
        assert isinstance(udf_data, openeo.udf.UdfData)
        structured = udf_data.get_structured_data_list()
        assert len(structured) == 1
        assert "stac_catalog_url" in structured[0].data
        assert structured[0].data["stac_catalog_url"].endswith("collection.json")

        # Verify the result
        assert result == {"result": "processed"}
