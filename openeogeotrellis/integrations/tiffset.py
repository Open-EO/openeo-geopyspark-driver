import subprocess
import tempfile
from pathlib import Path
from typing import Union


class TiffsetException(Exception):
    pass


def embed_gdal_metadata(gdal_metadata_xml: str, geotiff_path: Union[Path, str]):
    # TODO: use gdal instead to avoid warnings or are they harmless?
    with tempfile.NamedTemporaryFile(prefix="GDALMetadata_", suffix=".xml.tmp", mode="wt", encoding="ascii") as tmp:
        # use temporary file to work around segfault in container
        tmp.write(f"{gdal_metadata_xml}\n")
        tmp.flush()

        try:
            args = ["tiffset", "-sf", "42112", tmp.name, str(geotiff_path)]
            subprocess.check_output(args, stderr=subprocess.PIPE, text=True)
        except subprocess.CalledProcessError as e:
            raise TiffsetException(f"tiffset {geotiff_path} failed; stderr: {e.stderr.strip()!r}") from e
