import subprocess
from pathlib import Path
from typing import Union


def embed_gdal_metadata(gdal_metadata_xml: str, geotiff_path: Union[Path, str]):
    # TODO: use gdal instead to avoid warnings or are they harmless?
    subprocess.check_call(["tiffset", "-s", "42112", gdal_metadata_xml, str(geotiff_path)])
