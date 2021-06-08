import functools
import logging
import math
from inspect import signature

import geopandas
import numpy
import pandas
import shapely
import xarray

from openeo_udf.api.datacube import DataCube
from openeo_udf.api.feature_collection import FeatureCollection
from openeo_udf.api.machine_learn_model import MachineLearnModelConfig
from openeo_udf.api.spatial_extent import SpatialExtent
from openeo_udf.api.structured_data import StructuredData
from openeo_udf.api.udf_data import UdfData

__license__ = "Apache License, Version 2.0"
__author__ = "Soeren Gebbert"
__copyright__ = "Copyright 2018, Soeren Gebbert"
__maintainer__ = "Soeren Gebbert"
__email__ = "soerengebbert@googlemail.com"

"""
This is a copy of run_code.py in the UDF api. It allows more easy experimentation inside this backend.
"""


_log = logging.getLogger(__name__)


def _build_default_execution_context():
    context = {
        'numpy': numpy,
        'xarray': xarray,
        'geopandas': geopandas,
        'pandas': pandas,
        'shapely': shapely,
        'math': math,
        'FeatureCollection': FeatureCollection,
        'SpatialExtent': SpatialExtent,
        'StructuredData': StructuredData,
        'MachineLearnModel': MachineLearnModelConfig,
        'DataCube': DataCube,
        'UdfData': UdfData
    }
    try:
        import torch
        context['torch'] = torch
        import torchvision
    except ImportError as e:
        _log.warning('torch not available for run_udf context')
    try:
        import tensorflow
        context['tensorflow'] = tensorflow
        import tensorboard
    except ImportError as e:
        _log.warning('tensorflow not available for run_udf context')

    return context

@functools.lru_cache(maxsize=100)
def load_module_from_string(code:str):
    """
    Experimental: avoid loading same UDF module more than once, to make caching inside the udf work.
    @param code:
    @return:
    """
    module = _build_default_execution_context()
    exec(code, module)
    return module

def run_user_code(code:str, data:UdfData) -> UdfData:

    module = load_module_from_string(code)

    functions = {t[0]:t[1] for t in module.items() if callable(t[1])}

    for func in functions.items():
        sig = signature(func[1])
        params = sig.parameters
        params_list = [t[1] for t in sig.parameters.items()]
        if(func[0] == 'apply_timeseries' and 'series' in params and 'context' in params and 'pandas.core.series.Series'
                in str(params['series'].annotation) and 'pandas.core.series.Series' in str(sig.return_annotation) ):
            #this is a UDF that transforms pandas series
            from openeo_udf.api.udf_wrapper import apply_timeseries_generic
            return apply_timeseries_generic(data, func[1])
        elif( (func[0] == 'apply_hypercube' or func[0] == 'apply_datacube' )  and 'cube' in params and 'context' in params and 'openeo_udf.api.datacube.DataCube'
              in str(params['cube'].annotation) and 'openeo_udf.api.datacube.DataCube' in str(sig.return_annotation) ):
            #found a datacube mapping function
            if len(data.get_datacube_list()) != 1:
                raise ValueError("The provided UDF expects exactly one datacube, but only: %s were provided." % len(data.get_datacube_list()))
            result_cube = func[1](data.get_datacube_list()[0], data.user_context)
            if not isinstance(result_cube,DataCube):
                raise ValueError("The provided UDF did not return a DataCube, but got: %s" %result_cube)
            data.set_datacube_list([result_cube])
            break
        elif len(params_list) == 1 and (params_list[0].annotation == 'openeo_udf.api.udf_data.UdfData' or params_list[0].annotation == UdfData) :
            #found a generic UDF function
            func[1](data)
            break

    return data
