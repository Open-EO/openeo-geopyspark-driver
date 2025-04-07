import geopyspark as gps
from typing import Iterable
import numpy as np


def max_composite(tiles: Iterable[gps.Tile]):
    return composite(np.fmax, tiles)  # ignores NaNs (<=> maximum)


def min_composite(tiles: Iterable[gps.Tile]):
    return composite(np.fmin, tiles)  # ignores NaN (<=> minimum)


def sum_composite(tiles: Iterable[gps.Tile]):
    def fadd(a_cells, b_cells):
        return np.add(np.nan_to_num(a_cells), np.nan_to_num(b_cells))

    return composite(fadd, tiles)  # ignores NaN (<=> add)


def product_composite(tiles: Iterable[gps.Tile]):
    def fadd(a_cells, b_cells):
        return np.multiply(np.nan_to_num(a_cells), np.nan_to_num(b_cells))

    return composite(fadd, tiles)  # ignores NaN

def var_composite(tiles: Iterable[gps.Tile]) -> gps.Tile:
    cube = np.array([tile.cells for tile in tiles])
    reduced = np.nanvar(cube, axis=0)  # ignores NaN (<=> var)
    first_tile = next(iter(tiles))
    return gps.Tile(cells=reduced, cell_type=first_tile.cell_type, no_data_value=first_tile.no_data_value)


def std_composite(tiles: Iterable[gps.Tile]) -> gps.Tile:
    cube = np.array([tile.cells for tile in tiles])
    reduced = np.nanstd(cube, axis=0)  # ignores NaN (<=> std)
    first_tile = next(iter(tiles))
    return gps.Tile(cells=reduced, cell_type=first_tile.cell_type, no_data_value=first_tile.no_data_value)

def median_composite(tiles: Iterable[gps.Tile]) -> gps.Tile:
    cube = np.array([tile.cells for tile in tiles])
    #TODO numpy nanpercentile is known to be slow
    #https://github.com/numpy/numpy/issues/16575
    reduced = np.nanpercentile(cube,50.0, axis=0)  # ignores NaN
    first_tile = next(iter(tiles))
    return gps.Tile(cells=reduced, cell_type=first_tile.cell_type, no_data_value=first_tile.no_data_value)


def composite(func, tiles: Iterable[gps.Tile]):
    from functools import reduce
    cells = [tile.cells for tile in tiles]
    first_tile = next(iter(tiles))
    reduced = reduce(func, cells)
    return gps.Tile(cells=reduced, cell_type=first_tile.cell_type, no_data_value=first_tile.no_data_value)
