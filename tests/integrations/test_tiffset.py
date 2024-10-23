import shutil
import textwrap
from pathlib import Path

from osgeo import gdal

from openeogeotrellis.integrations import tiffset
from tests.data import get_test_data_file


def test_embed_gdal_metadata(tmp_path):
    # don't mess with the original file
    geotiff_copy = tmp_path / "copy.tif"
    shutil.copy(
        get_test_data_file("binary/jobs/j-ec5d3e778ba5423d8d88a50b08cb9f63/openEO_2022-09-12Z.tif"),
        geotiff_copy
    )

    assert _processing_software(geotiff_copy) == "0.9.4a1"  # original

    gdal_metadata_xml = textwrap.dedent("""
    <GDALMetadata>
      <Item name="PROCESSING_SOFTWARE">0.45.0a1</Item>
      <Item name="DESCRIPTION" sample="0">CO</Item>
    </GDALMetadata>
    """)

    tiffset.embed_gdal_metadata(gdal_metadata_xml, geotiff_copy)

    assert _processing_software(geotiff_copy) == "0.45.0a1"  # updated


def _processing_software(geotiff: Path) -> str:
    ds = gdal.Open(str(geotiff))
    return ds.GetMetadata()["PROCESSING_SOFTWARE"]
