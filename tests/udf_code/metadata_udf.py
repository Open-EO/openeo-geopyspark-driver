from openeo.metadata import CollectionMetadata, Band
from openeo.udf import XarrayDataCube


def apply_metadata(metadata: CollectionMetadata, context: dict) -> CollectionMetadata:
    print("apply_metadata: metadata.bands:" + repr(metadata.bands))
    renamed_bands = [openeo.metadata.Band(f"{x.name}_RENAMED", None, None) for x in metadata.bands]
    metadata.band_dimension.bands = [Band(b) for b in renamed_bands]
    return metadata


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    print("apply_datacube: cube:" + repr(cube))
    array = cube.get_array()
    return XarrayDataCube(array)
