import subprocess
import tempfile
from pathlib import Path
from typing import Union


def embed_gdal_metadata(gdal_metadata_xml: str, geotiff_path: Union[Path, str]):
    # TODO: use gdal instead to avoid warnings or are they harmless?
    with tempfile.NamedTemporaryFile(prefix="GDALMetadata", suffix=".xml.tmp", mode="wt", encoding="ascii") as tmp:
        # use temporary file to work around segfault in container
        tmp.write(gdal_metadata_xml)
        tmp.flush()

        subprocess.check_call(["tiffset", "-sf", "42112", tmp.name, str(geotiff_path)])
