from typing import Dict, List, Union

import geopyspark as gps
from datetime import datetime, date
from geopyspark import TiledRasterLayer, TMS
from pandas import Series
import pandas as pd
from shapely.geometry import Point

from openeo.imagecollection import ImageCollection


class GeotrellisTimeSeriesImageCollection(ImageCollection):


    def __init__(self, image_collection_id):
        self.image_collection_id = image_collection_id
        self.tms = None
        #TODO load real layer rdd

    def __init__(self, parent_layer: TiledRasterLayer):
        self.rdd = parent_layer
        self.tms = None

    def date_range_filter(self, start_date: Union[str, datetime, date],
                          end_date: Union[str, datetime, date]) -> 'ImageCollection':
        return GeotrellisTimeSeriesImageCollection(self.rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))

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
        min_rdd = self.rdd.to_spatial_layer().aggregate_by_cell("Min")
        return GeotrellisTimeSeriesImageCollection(min_rdd)

    def max_time(self) -> 'ImageCollection':
        max_rdd = self.rdd.to_spatial_layer().aggregate_by_cell("Max")
        return GeotrellisTimeSeriesImageCollection(max_rdd)

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

    def download(self,outputfile:str, bbox="", time="",outputformat="geotiff") -> str:
        """Extraxts a geotiff from this image collection."""
        #geotiffs = self.rdd.merge().to_geotiff_rdd(compression=gps.Compression.DEFLATE_COMPRESSION).collect()
        #TODO better timeseries support, bbox and time is currently ignored
        filename = outputfile
        if outputfile is None:
            import tempfile
            (file,filename) = tempfile.mkstemp()
        else:
            filename = outputfile
        spatial_rdd = self.rdd
        if self.rdd.layer_type != gps.LayerType.SPATIAL:
            spatial_rdd = self.rdd.to_spatial_layer()
        spatial_rdd.save_stitched(filename)

        return filename


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