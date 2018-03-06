from typing import Dict, List, Union

import geopyspark as gps
from datetime import datetime, date
from geopyspark import TiledRasterLayer, TMS, Pyramid
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
        self.pyramid = parent_layer
        self.tms = None

    def apply_to_levels(self, func):
        pyramid = Pyramid({k:func( l ) for k,l in self.pyramid.levels.items()})
        return GeotrellisTimeSeriesImageCollection(pyramid)

    def date_range_filter(self, start_date: Union[str, datetime, date],end_date: Union[str, datetime, date]) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd: rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))


    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        return self.apply_to_levels(lambda rdd: rdd.map_cells(bandfunction))

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        #group keys
        #reduce
        pass

    def min_time(self) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd:rdd.to_spatial_layer().aggregate_by_cell("Min"))

    def max_time(self) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd:rdd.to_spatial_layer().aggregate_by_cell('Max'))

    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        points = [
            Point(x, y),
        ]
        values = self.rdd.get_point_values(points)
        result = {}
        if isinstance(values[0][1],List):
            values = values[0][1]
        for v in values:
            if isinstance(v,float):
                result["NoDate"]=v
            elif "isoformat" in dir(v[0]):
                result[v[0].isoformat()]=v[1]
            else:
                print("unexpected value: "+v)

        return result

    def download(self,outputfile:str, bbox="", time="",**format_options) -> str:
        """Extracts a geotiff from this image collection."""
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
            spatial_rdd = self.apply_to_levels(lambda rdd: rdd.to_spatial_layer()).rdd
        spatial_rdd.save_stitched(filename)

        return filename


    def tiled_viewing_service(self) -> Dict:
        spatial_rdd = self
        if self.pyramid.layer_type != gps.LayerType.SPATIAL:
            spatial_rdd = self.apply_to_levels(lambda rdd: rdd.to_spatial_layer())
        spatial_rdd = spatial_rdd.apply_to_levels(lambda rdd: rdd.partitionBy(gps.SpatialPartitionStrategy(num_partitions=40,bits=2)).persist())

        def render_rgb(tile):
            import numpy as np
            from PIL import Image
            rgba = np.dstack([tile.cells[0]*(255.0/2000.0), tile.cells[1]*(255.0/2000.0), tile.cells[2]*(255.0/2000.0)]).astype('uint8')
            img = Image.fromarray(rgba, mode='RGB')
            return img

        pyramid = spatial_rdd.pyramid
        if(self.tms is None):
            self.tms = TMS.build(source=pyramid,display = render_rgb)
            self.tms.bind(host="0.0.0.0",requested_port=0)


        level_bounds = {level: tiled_raster_layer.layer_metadata.bounds for level, tiled_raster_layer in pyramid.levels.items()}
        #TODO handle cleanup of tms'es
        return {
            "type":"TMS",
            "url":self.tms.url_pattern,
            "bounds":level_bounds
        }