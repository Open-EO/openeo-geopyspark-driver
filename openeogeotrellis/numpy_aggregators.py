import geopyspark as gps
from typing import Iterable
import numpy as np

def max_composite(v:Iterable[gps.Tile]):
    from functools import reduce
    cells = [tile.cells for tile in v]
    first_tile = v.__iter__().__next__()
    reduced = reduce(np.fmax, cells)
    print(reduced)
    return gps.Tile(cells=reduced, cell_type=first_tile.cell_type, no_data_value=first_tile.no_data_value)