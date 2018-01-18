from typing import Dict, List

import geopyspark as gps
from geopyspark import TiledRasterLayer, TMS
from openeo.imagecollection import ImageCollection
from pandas import Series
from shapely.geometry import Point


class GeotrellisTimeSeriesImageCollection(ImageCollection):


    def __init__(self, image_collection_id):
        self.image_collection_id = image_collection_id
        self.tms = None
        #TODO load real layer rdd

    def __init__(self, parent_layer: TiledRasterLayer):
        self.rdd = parent_layer
        self.tms = None

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        mapped_rdd = self.rdd.map_cells(bandfunction)
        return GeotrellisTimeSeriesImageCollection(mapped_rdd)

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        #group keys
        #reduce
        pass

    def min_time(self) -> 'ImageCollection':
        min_rdd = self.rdd.aggregate_by_cell("Min")
        return GeotrellisTimeSeriesImageCollection(min_rdd)

    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        points = [
            Point(x, y),
        ]
        values = self.rdd.get_point_values(points)
        result = {}
        for v in values:
            if "isoformat" in dir(v):
                result[v[1].isoformat()]=v[2]
            else:
                result[v[1]]=v[2]
        return result


    def tiled_viewing_service(self) -> Dict:
        pyramid = self.rdd.pyramid()
        if(self.tms is None):
            self.tms = TMS.build(source=pyramid,display = gps.ColorMap.nlcd_colormap())
            self.tms.bind(requested_port=0)

        #TODO handle cleanup of tms'es
        return {
            "type":"TMS",
            "url":self.tms.url_pattern
        }