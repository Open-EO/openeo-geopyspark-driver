# -*- coding: utf-8 -*-

from typing import Dict

from openeo_udf.api.datacube import DataCube

__license__ = "Apache License, Version 2.0"
__author__ = "Jeroen Dries"

def apply_datacube(cube: DataCube,context:Dict)-> DataCube:
    red = cube.array.sel(bands='red')
    nir = cube.array.sel(bands='nir')
    ndvi = (nir - red) / (nir + red)
    ndvi.name = "NDVI"

    return DataCube(array=ndvi)
