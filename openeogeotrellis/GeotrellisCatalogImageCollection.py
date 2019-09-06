from typing import Dict, List, Union, Iterable

import geopyspark as gps
from datetime import datetime, date
from geopyspark import TiledRasterLayer, TMS, Pyramid,TileRender

from geopyspark.geotrellis.constants import CellType
from geopyspark.geotrellis import color
import numpy as np

from pandas import Series
import pandas as pd
from shapely.geometry import Point,Polygon,MultiPolygon
import json
import os

from openeo.imagecollection import ImageCollection
from openeogeotrellis.configparams import ConfigParams

# TODO this is deprecated apparently: remove it?

class GeotrellisCatalogImageCollection(ImageCollection):


    def __init__(self, image_collection_id = ""):
        self.image_collection_id = image_collection_id
        self.tms = None
        self.transform_tile = []
        #TODO load real layer rdd


    def date_range_filter(self, start_date: Union[str, datetime, date],end_date: Union[str, datetime, date]) -> 'ImageCollection':

        return self.apply_to_levels(lambda rdd: rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))


    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        self.transform_tile.append(bandfunction)
        return self

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        #group keys
        #reduce
        pass

    def min_time(self) -> 'ImageCollection':
        raise NotImplementedError

    def max_time(self) -> 'ImageCollection':
        raise NotImplementedError


    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        raise NotImplementedError


    def download(self,outputfile:str, **format_options) -> str:
        """Extracts a geotiff from this image collection."""
        raise NotImplementedError

    def _proxy_tms(self,tms):
        if 'TRAVIS' in os.environ:
            return tms.url_pattern
        else:
            from kazoo.client import KazooClient
            zk = KazooClient(hosts=','.join(ConfigParams().zookeepernodes))
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

    def tiled_viewing_service(self,**kwargs) -> Dict:
        type = kwargs['type']
        def render_rgb(tile):
            transformed_cells = tile.cells
            for function in self.transform_tile:
                transformed_cells = function(transformed_cells)
            import numpy as np
            from PIL import Image
            rgba = np.dstack([tile.cells[2]*(255.0/2000.0), tile.cells[1]*(255.0/2000.0), tile.cells[0]*(255.0/2000.0)]).astype('uint8')
            img = Image.fromarray(rgba, mode='RGB')
            return img

        #greens = color.get_colors_from_matplotlib(ramp_name="YlGn")
        greens = gps.ColorMap.build(breaks=[x for x in range(0,250)], colors="YlGn")

        if type.lower() == "tms":
            if(self.tms is None):
                pysc = gps.get_spark_context()
                gps._ensure_callback_gateway_initialized(pysc._gateway)
                reader = pysc._gateway.jvm.geopyspark.geotrellis.tms.TileReaders.createCatalogReader("accumulo://"+ConfigParams().zookeepernodes[0]+"/hdp-accumulo-instance", self.image_collection_id, False)
                route = pysc._jvm.geopyspark.geotrellis.tms.TMSServerRoutes.temporalRenderingTileRoute(reader, TileRender(render_rgb))
                self.tms = TMS(pysc._jvm.geopyspark.geotrellis.tms.TMSServer.createServer(route))
                #self.tms = TMS.build(source=("accumulo://"+ConfigParams().zookeepernodes[0]+"/hdp-accumulo-instance","S2_FAPAR_V101"),display = greens)
                self.tms.bind(host="0.0.0.0",requested_port=8000)

            #url = self._proxy_tms(self.tms)

           # level_bounds = {level: tiled_raster_layer.layer_metadata.bounds for level, tiled_raster_layer in pyramid.levels.items()}
            #TODO handle cleanup of tms'es
            return {
                "type":"TMS",
                "url":self.tms.url_pattern
            #    "bounds":level_bounds
            }
        else:
            if('wmts' in self.__dir__()):
                self.wmts.stop()
            pysc = gps.get_spark_context()
            self.wmts = pysc._jvm.be.vito.eodata.gwcgeotrellis.wmts.WMTSServer.createServer()
            self.wmts.addAccumuloLayer(self.image_collection_id, True, "hdp-accumulo-instance", ','.join(ConfigParams().zookeepernodes));

            return {
                "type": "WMTS",
                "url": "http://localhost:" + str(self.wmts.getPort()) + "/service/wmts"
                #    "bounds":level_bounds
            }