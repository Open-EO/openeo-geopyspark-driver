from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from .testlayers import TestLayers

def health_check():
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()
    count = sc.parallelize([1,2,3]).count()
    return 'Health check: ' + str(count)

def getImageCollection(product_id:str, viewingParameters):
    #TODO create real rdd from accumulo
    print("Creating layer for: "+product_id)

    return GeotrellisTimeSeriesImageCollection(TestLayers().create_spacetime_layer())

