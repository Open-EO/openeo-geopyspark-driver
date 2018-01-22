import os
import pandas as pd

from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection

if os.getenv("FLASK_DEBUG") == '1':
    import geopyspark as gps
    from pyspark import SparkContext
    conf = gps.geopyspark_conf("local[2]","OpenEO-Test")
    SparkContext.getOrCreate(conf)

def health_check():
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()
    count = sc.parallelize([1,2,3]).count()
    return 'Health check: ' + str(count)

def getImageCollection(product_id:str, viewingParameters):
    print("Creating layer for: "+product_id)
    import geopyspark as gps
    from_date = viewingParameters.get("from",None)
    to_date = viewingParameters.get("to",None)
    time_intervals = None
    if from_date is not None and to_date is not None:
        time_intervals = [pd.to_datetime(from_date),pd.to_datetime(to_date)]

    left = viewingParameters.get("left",None)
    right = viewingParameters.get("right",None)
    top = viewingParameters.get("top",None)
    bottom = viewingParameters.get("bottom",None)
    srs = viewingParameters.get("srs",None)
    bbox = None
    if(left is not None and right is not None and top is not None and bottom is not None):
        bbox = gps.Extent(left,bottom,right,top)

    return GeotrellisTimeSeriesImageCollection(gps.query(uri="accumulo://epod6.vgt.vito.be:2181/hdp-accumulo-instance",layer_name=product_id,query_geom=bbox,query_proj=srs,time_intervals=time_intervals))


