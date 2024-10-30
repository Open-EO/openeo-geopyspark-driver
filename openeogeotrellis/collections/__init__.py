import datetime as dt
from typing import Callable

import geopyspark
from py4j.java_gateway import JavaObject


def convert_scala_metadata(
    metadata_sc: JavaObject, epoch_ms_to_datetime: Callable[[int], dt.datetime], logger
) -> geopyspark.Metadata:
    """
    Convert geotrellis TileLayerMetadata (Java) object to geopyspark Metadata object
    """
    logger.info("Convert {m!r} to geopyspark.Metadata".format(m=metadata_sc))
    crs_py = str(metadata_sc.crs())
    cell_type_py = str(metadata_sc.cellType())

    def convert_key(key_sc: JavaObject) -> geopyspark.SpaceTimeKey:
        return geopyspark.SpaceTimeKey(
            col=key_sc.col(), row=key_sc.row(), instant=epoch_ms_to_datetime(key_sc.instant())
        )

    bounds_sc = metadata_sc.bounds()
    bounds_py = geopyspark.Bounds(minKey=convert_key(bounds_sc.minKey()), maxKey=convert_key(bounds_sc.maxKey()))

    def convert_extent(extent_sc: JavaObject) -> geopyspark.Extent:
        return geopyspark.Extent(extent_sc.xmin(), extent_sc.ymin(), extent_sc.xmax(), extent_sc.ymax())

    extent_py = convert_extent(metadata_sc.extent())

    layout_definition_sc = metadata_sc.layout()
    tile_layout_sc = layout_definition_sc.tileLayout()
    tile_layout_py = geopyspark.TileLayout(
        layoutCols=tile_layout_sc.layoutCols(),
        layoutRows=tile_layout_sc.layoutRows(),
        tileCols=tile_layout_sc.tileCols(),
        tileRows=tile_layout_sc.tileRows(),
    )
    layout_definition_py = geopyspark.LayoutDefinition(
        extent=convert_extent(layout_definition_sc.extent()), tileLayout=tile_layout_py
    )

    return geopyspark.Metadata(
        bounds=bounds_py, crs=crs_py, cell_type=cell_type_py, extent=extent_py, layout_definition=layout_definition_py
    )
