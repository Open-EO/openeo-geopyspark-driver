from openeo.sessions import Session
from .GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection

class GeoPySparkSession(Session):
    def __init__(self):
        pass

    def imagecollection(self, image_collection_id) -> 'ImageCollection':
        from openeo.imagecollection import ImageCollection
        return GeotrellisTimeSeriesImageCollection(image_collection_id)
