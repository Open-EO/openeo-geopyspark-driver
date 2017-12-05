import base64
import json
import pickle
from typing import Dict

from openeo import ImageCollection
from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .testlayers import TestLayers


def getImageCollection(product_id:str, viewingParameters):
    #TODO create real rdd from accumulo
    print("Creating layer for: "+product_id)
    return GeotrellisTimeSeriesImageCollection(TestLayers().create_spacetime_layer())


def graphToRdd(processGraph:Dict, viewingParameters)->ImageCollection:
    if 'product_id' in processGraph:
        return getImageCollection(processGraph['product_id'],viewingParameters)
    elif 'process_id' in processGraph:
        return getProcessImageCollection(processGraph['process_id'],processGraph['args'],viewingParameters)
    else:
        raise AttributeError("Process should contain either product_id or process_id, but got: \n" + json.dumps(processGraph,indent=1))


def extract_arg(args:Dict,name:str)->str:
    try:
        return args[name]
    except KeyError:
        raise AttributeError(
            "Required argument " +name +" should not be null in band_arithmetic. Arguments: \n" + json.dumps(args,indent=1))


def band_arithmetic(input_collection:ImageCollection, args:Dict, viewingParameters)->ImageCollection:
    function = extract_arg(args,'function')
    bands = extract_arg(args,'bands')
    decoded_function = pickle.loads(base64.standard_b64decode(function))
    return input_collection.combinebands(bands,decoded_function)


def reduce_by_time(input_collection:ImageCollection, args:Dict, viewingParameters)->ImageCollection:
    function = extract_arg(args,'function')
    temporal_window = extract_arg(args,'temporal_window')
    decoded_function = pickle.loads(base64.standard_b64decode(function))
    return input_collection.reduceByTime(temporal_window,decoded_function)


def getProcessImageCollection( process_id:str, args:Dict, viewingParameters)->ImageCollection:

    imagery = extract_arg(args,'imagery')
    child_collection = graphToRdd(imagery,viewingParameters)

    print(globals().keys())
    process_function = globals()[process_id]
    if process_function is None:
        raise RuntimeError("No process found with name: "+process_id)
    return process_function(child_collection,args,viewingParameters)
