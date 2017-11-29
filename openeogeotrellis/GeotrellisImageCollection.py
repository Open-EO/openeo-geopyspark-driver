from typing import Dict, List

from shapely.geometry import Point

from geopyspark import TiledRasterLayer
from openeo.imagecollection import ImageCollection


class GeotrellisTimeSeriesImageCollection(ImageCollection):

    rdd: TiledRasterLayer

    def __init__(self, image_collection_id):
        self.image_collection_id = image_collection_id
        #TODO load real layer rdd

    def __init__(self, parent_layer: TiledRasterLayer):
        self.rdd = parent_layer

    def combinebands(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        mapped_rdd = self.rdd.map_cells(bandfunction)

        return GeotrellisTimeSeriesImageCollection(mapped_rdd)

    def meanseries(self, x,y, srs="EPSG:4326") -> Dict:
        points = [
            Point(x, y),
        ]
        values = self.rdd.get_point_values(points)
        result = {}
        for v in values:
            result[v[1].isoformat()]=v[2]
        return result

