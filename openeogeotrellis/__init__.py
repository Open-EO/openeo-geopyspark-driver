from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection

def health_check():
    from pyspark import SparkContext
    sc = SparkContext.getOrCreate()
    count = sc.parallelize([1,2,3]).count()
    return 'Health check: ' + str(count)

def getImageCollection(product_id:str, viewingParameters):
    print("Creating layer for: "+product_id)
    import geopyspark as gps
    return GeotrellisTimeSeriesImageCollection(gps.query(uri="accumulo://epod6.vgt.vito.be:2181/hdp-accumulo-instance",layer_name=product_id))

