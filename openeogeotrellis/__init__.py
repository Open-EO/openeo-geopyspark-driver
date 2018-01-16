from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .testlayers import TestLayers




def getImageCollection(product_id:str, viewingParameters):
    #TODO create real rdd from accumulo
    print("Creating layer for: "+product_id)

    return GeotrellisTimeSeriesImageCollection(TestLayers().create_spacetime_layer())

