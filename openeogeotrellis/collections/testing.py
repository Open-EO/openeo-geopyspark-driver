import logging
import math
from datetime import datetime
from typing import Dict, List, Tuple

import geopyspark
import numpy as np
from geopyspark import TiledRasterLayer, LayerType
from geopyspark.geotrellis import SpaceTimeKey, Tile, Metadata, Bounds, CellType, \
    LayoutDefinition, TileLayout
from pyspark import SparkContext

from openeo.util import rfc3339
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata

_log = logging.getLogger(__name__)


def dates_between(start: datetime, end: datetime) -> List[datetime]:
    """
    Generate dates between given start and end,
    with days ending on 5 (to simulate sparse observations)
    """
    dates = (datetime(y, m, d) for y in range(start.year, end.year + 1) for m in range(1, 13) for d in [5, 15, 25])
    return [d for d in dates if start <= d < end]


def load_test_collection(
        collection_id: str,
        collection_metadata: GeopysparkCubeMetadata,
        extent, srs: str,
        from_date: str, to_date: str,
        bands=None,
        correlation_id: str = "NA",
) -> Dict[int, geopyspark.TiledRasterLayer]:
    """
    Load synthetic data as test collection
    :param collection_id:
    :param collection_metadata:
    :param extent:
    :param srs:
    :param from_date:
    :param to_date:
    :param bands:
    :param correlation_id:
    :return:
    """
    # TODO: support more test collections
    assert collection_id == "TestCollection-LonLat4x4"
    grid_size: float = 1.0
    tile_size = 16

    # TODO: support other srs'es?
    assert srs == "EPSG:4326"

    # Get bounds of tiling layout
    extent = geopyspark.Extent(extent.xmin(), extent.ymin(), extent.xmax(), extent.ymax())
    # Column/row tiling starts from upper left corner,
    # column index increasing to the east (right, increasing longitude),
    # row index increasing to the south (down, decreasing latitude)
    tile_orig_col = int(math.floor(extent.xmin / grid_size))
    tile_orig_row = int(math.ceil(extent.ymax / grid_size))
    col_count = int(math.ceil(extent.xmax / grid_size)) - tile_orig_col
    row_count = tile_orig_row - int(math.floor(extent.ymin / grid_size))

    # Simulate sparse range of observation dates
    from_date = rfc3339.parse_datetime(rfc3339.datetime(from_date))
    to_date = rfc3339.parse_datetime(rfc3339.datetime(to_date))
    dates = dates_between(from_date, to_date)

    # Build RDD of tiles with requested bands.
    tile_builder = TestCollectionLonLat(
        tile_size=tile_size,
        grid_size=grid_size,
        tile_origin=(tile_orig_col, tile_orig_row),
    )
    bands = bands or [b.name for b in collection_metadata.bands]
    rdd_data = [
        (
            SpaceTimeKey(c, r, date),
            tile_builder.get_tile(bands=bands, col=c, row=r, date=date),
        )
        for c in range(col_count)
        for r in range(row_count)
        for date in dates
    ]
    rdd = SparkContext.getOrCreate().parallelize(rdd_data)

    metadata = Metadata(
        bounds=Bounds(
            SpaceTimeKey(0, 0, min(dates)),
            SpaceTimeKey(col_count - 1, row_count - 1, max(dates)),
        ),
        crs="+proj=longlat +datum=WGS84 +no_defs ",
        cell_type=CellType.FLOAT64,
        extent=extent,
        layout_definition=LayoutDefinition(
            extent=geopyspark.Extent(
                tile_orig_col * grid_size,
                (tile_orig_row - row_count) * grid_size,
                (tile_orig_col + col_count) * grid_size,
                tile_orig_row * grid_size,
            ),
            tileLayout=TileLayout(
                layoutCols=col_count,
                layoutRows=row_count,
                tileCols=tile_size,
                tileRows=tile_size,
            ),
        ),
    )

    _log.info(f"load_test_collection: {metadata=}")

    layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)
    return {0: layer}


class TestCollectionLonLat:
    """
    Tile builder for collections defined in LonLat
    """

    def __init__(
        self,
        tile_size: int = 4,
        grid_size: float = 1.0,
        tile_origin: Tuple[int, int] = (0, 0),
    ):
        # TODO: also allow non-square tiling to properly test tile handling.
        self.tile_size = tile_size
        self.grid_size = grid_size
        self.tile_orig_col, self.tile_orig_row = tile_origin

    def _flat(self, value=1) -> np.ndarray:
        """Tile with constant value"""
        return np.full((self.tile_size, self.tile_size), fill_value=value)

    def get_band_tile(self, band: str, col: int, row: int, date: datetime) -> np.ndarray:
        tz = self.tile_size
        if band.startswith("Flat:"):
            return self._flat(int(band.split(":")[1]))
        elif band == "TileCol":
            return self._flat(col)
        elif band == "TileRow":
            return self._flat(row)
        elif band.startswith("TileColRow"):
            # Combined col&row index, by default encoded decimally: Col=2 Row=3 -> value 23
            m = int(band.split(":")[1]) if ":" in band else 10
            return self._flat(m * col + row)
        elif band == "Longitude":
            # Second (inner) dimension of 2D numpy array is corresponds with longitude.
            return (
                self.tile_orig_col + col + (np.mgrid[1 : tz + 1, 0:tz][1] / tz)
            ) * self.grid_size
        elif band == "Latitude":
            # First (outer) dimension of 2D numpy array is corresonds with latitude.
            return (
                self.tile_orig_row - row - (np.mgrid[1 : tz + 1, 0:tz][0] / tz)
            ) * self.grid_size
        elif band == "Year":
            return self._flat(date.year)
        elif band == "Month":
            return self._flat(date.month)
        elif band == "Day":
            return self._flat(date.day)
        else:
            raise ValueError(band)

    def get_tile(self, bands: List[str], col: int, row: int, date: datetime) -> Tile:
        array = np.array([
            self.get_band_tile(band=band, col=col, row=row, date=date)
            for band in bands
        ])
        return Tile.from_numpy_array(array)
