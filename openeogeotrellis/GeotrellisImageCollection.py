from typing import Dict, List, Union, Iterable

import geopyspark as gps
from datetime import datetime, date
from geopyspark import TiledRasterLayer, TMS, Pyramid
from geopyspark.geotrellis.constants import CellType
from geopyspark.geotrellis import color
import numpy as np

from pandas import Series
import pandas as pd
from shapely.geometry import Point,Polygon,MultiPolygon
import json
import os

from openeo.imagecollection import ImageCollection


class GeotrellisTimeSeriesImageCollection(ImageCollection):

    def __init__(self, image_collection_id):
        super().__init__()
        self.image_collection_id = image_collection_id
        self.tms = None
        #TODO load real layer rdd

    def __init__(self, pyramid: Pyramid):
        self.pyramid = pyramid
        self.tms = None

    def apply_to_levels(self, func):
        pyramid = Pyramid({k:func( l ) for k,l in self.pyramid.levels.items()})
        return GeotrellisTimeSeriesImageCollection(pyramid)

    def date_range_filter(self, start_date: Union[str, datetime, date],end_date: Union[str, datetime, date]) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd: rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))


    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        return self.apply_to_levels(lambda rdd: rdd.convert_data_type(CellType.FLOAT64).map_cells(bandfunction))

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        #group keys
        #reduce
        pass

    def min_time(self) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd:rdd.to_spatial_layer().aggregate_by_cell("Min"))

    def max_time(self) -> 'ImageCollection':
        from .numpy_aggregators import max_composite
        return self._aggregate_over_time_numpy(max_composite)

    def _aggregate_over_time_numpy(self,numpy_function) -> 'ImageCollection':
        """
        Aggregate over time.
        :param numpy_function:
        :return:
        """
        def aggregate_spatial_rdd(rdd):
            grouped_numpy_rdd = rdd.to_spatial_layer().convert_data_type(CellType.FLOAT32).to_numpy_rdd().groupByKey()

            composite = grouped_numpy_rdd.mapValues(numpy_function)
            aggregated_layer = TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, composite, rdd.layer_metadata)
            return aggregated_layer
        return self.apply_to_levels(aggregate_spatial_rdd)

    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        max_level = self.pyramid.levels[self.pyramid.max_zoom]
        import pyproj
        (x_layer,y_layer) = pyproj.transform(pyproj.Proj(init=srs),pyproj.Proj(max_level.layer_metadata.crs),x,y)
        points = [
            Point(x_layer, y_layer),
        ]
        values = max_level.get_point_values(points)
        result = {}
        if isinstance(values[0][1],List):
            values = values[0][1]
        for v in values:
            if isinstance(v,float):
                result["NoDate"]=v
            elif "isoformat" in dir(v[0]):
                result[v[0].isoformat()]=v[1]
            elif v[0] is None:
                #empty timeseries
                pass
            else:
                print("unexpected value: "+str(v))

        return result

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon]) -> Dict:
        import pyproj
        from shapely.ops import transform

        max_level = self.pyramid.levels[self.pyramid.max_zoom].persist()

        source_crs = pyproj.Proj(init='EPSG:4326')
        target_crs = pyproj.Proj(max_level.layer_metadata.crs)

        reprojected_polygon = transform(lambda x, y, z=None: pyproj.transform(source_crs, target_crs, x, y, z), polygon)

        def mean(timestamp):
            spatial_layer = max_level.to_spatial_layer(timestamp)
            return spatial_layer.polygonal_mean(reprojected_polygon)

        timestamps = map(lambda key: key.instant, set(max_level.collect_keys()))

        return {timestamp.isoformat(): mean(timestamp) for timestamp in timestamps}

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
        spatial_rdd = self.pyramid.levels[self.pyramid.max_zoom]
        if spatial_rdd.layer_type != gps.LayerType.SPATIAL:
            spatial_rdd = spatial_rdd.to_spatial_layer()
        spatial_rdd.save_stitched(filename)

        return filename

    def _proxy_tms(self,tms):
        if 'TRAVIS' in os.environ:
            return tms.url_pattern
        else:
            from kazoo.client import KazooClient
            zk = KazooClient(hosts='epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181')
            zk.start()
            zk.ensure_path("discovery/services/openeo-viewer-test")
            #id = uuid.uuid4()
            #print(id)
            id = 0
            zk.ensure_path("discovery/services/openeo-viewer-test/"+str(id))
            zk.set("discovery/services/openeo-viewer-test/"+str(id),str.encode(json.dumps({"name":"openeo-viewer-test","id":str(id),"address":tms.host,"port":tms.port,"sslPort":None,"payload":None,"registrationTimeUTC":datetime.utcnow().strftime('%s'),"serviceType":"DYNAMIC"})))
            zk.stop()
            zk.close()
            url = "http://openeo.vgt.vito.be/tile/{z}/{x}/{y}.png"
            return url

    def tiled_viewing_service(self) -> Dict:
        spatial_rdd = self
        if self.pyramid.layer_type != gps.LayerType.SPATIAL:
            spatial_rdd = self.apply_to_levels(lambda rdd: rdd.to_spatial_layer())
        print("Creating persisted pyramid.")
        spatial_rdd = spatial_rdd.apply_to_levels(lambda rdd: rdd.partitionBy(gps.SpatialPartitionStrategy(num_partitions=40,bits=2)).persist())
        print("Pyramid ready.")
        def render_rgb(tile):
            import numpy as np
            from PIL import Image
            rgba = np.dstack([tile.cells[0]*(255.0/2000.0), tile.cells[1]*(255.0/2000.0), tile.cells[2]*(255.0/2000.0)]).astype('uint8')
            img = Image.fromarray(rgba, mode='RGB')
            return img

        #greens = color.get_colors_from_matplotlib(ramp_name="YlGn")
        greens = gps.ColorMap.build(breaks=[x/100.0 for x in range(0,100)], colors="YlGn")

        pyramid = spatial_rdd.pyramid
        if(self.tms is None):
            self.tms = TMS.build(source=pyramid,display = greens)
            self.tms.bind(host="0.0.0.0",requested_port=0)

        url = self._proxy_tms(self.tms)

        level_bounds = {level: tiled_raster_layer.layer_metadata.bounds for level, tiled_raster_layer in pyramid.levels.items()}
        #TODO handle cleanup of tms'es
        return {
            "type":"TMS",
            "url":url,
            "bounds":level_bounds
        }