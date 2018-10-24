from typing import Dict, List, Union, Iterable, Tuple

import geopyspark as gps
from datetime import datetime, date
from geopyspark import TiledRasterLayer, TMS, Pyramid,Tile,SpaceTimeKey,Metadata,Extent
from geopyspark.geotrellis.constants import CellType
from geopyspark.geotrellis import color
import numpy as np
from openeo_udf.api.base import UdfData,RasterCollectionTile,SpatialExtent

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

    def __init__(self, pyramid: Pyramid, metadata:Dict=None):
        self.pyramid = pyramid
        self.tms = None
        self.metadata = metadata

    def apply_to_levels(self, func):
        """
        Applies a function to each level of the pyramid. The argument provided to the function is of type TiledRasterLayer
        :param func:
        :return:
        """
        pyramid = Pyramid({k:func( l ) for k,l in self.pyramid.levels.items()})
        return GeotrellisTimeSeriesImageCollection(pyramid)

    def date_range_filter(self, start_date: Union[str, datetime, date],end_date: Union[str, datetime, date]) -> 'ImageCollection':
        return self.apply_to_levels(lambda rdd: rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))

    def bbox_filter(self, left: float, right: float, top: float, bottom: float, srs: str) -> 'ImageCollection':
        return self

    def apply_pixel(self, bands:List, bandfunction) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        return self.apply_to_levels(lambda rdd: rdd.convert_data_type(CellType.FLOAT64).map_cells(bandfunction))

    @classmethod
    def _mapTransform(cls,layoutDefinition, spatialKey):
        ex = layoutDefinition.extent
        x_range = ex.xmax - ex.xmin
        xinc = x_range / layoutDefinition.tileLayout.layoutCols
        yrange = ex.ymax - ex.ymin
        yinc = yrange / layoutDefinition.tileLayout.layoutRows
        return SpatialExtent(ex.ymax - yinc * spatialKey.row,ex.ymax - yinc * (spatialKey.row + 1),ex.xmin + xinc * (spatialKey.col + 1),ex.xmin + xinc * spatialKey.col,layoutDefinition.tileLayout.tileCols,layoutDefinition.tileLayout.tileRows)

    @classmethod
    def _tile_to_rastercollectiontile(cls, bands_numpy: np.ndarray, extent: SpatialExtent, bands_metadata, start_times=None):
        """
        Convert from geopyspark tile format into rastercollection tile format
        Geopyspark: A tile is a 3D array, associated with a time instant: (time_instant,(bands,x,y))
        RasterCollectionTile: one tile per band: (band,(time_instant,x,y))

        :param bands_numpy:
        :param extent:
        :param bands_metadata:
        :return:
        """
        if len(bands_numpy.shape) == 3 or len(bands_numpy.shape) == 4:
            if len(bands_numpy.shape) == 4:
                #swap from time,bands,y,x to bands,time,y,x
                bands_numpy = np.swapaxes(bands_numpy,0,1)
            result = []
            i = 1
            for band in bands_numpy:
                name = "B" + str(i)
                wavelength = None
                if bands_metadata is not None and len(bands_metadata) == bands_numpy.shape[0]:
                    name = bands_metadata[i-1]['name']
                    wavelength = bands_metadata[i-1]['wavelength_nm']
                if len(bands_numpy.shape) == 3:
                    print(band.shape)
                    rc_tile = RasterCollectionTile(name, extent, np.array([band]),wavelength=wavelength,start_times=start_times)
                elif len(bands_numpy.shape) == 4:
                    print("4D data: " + str(band.shape))
                    print(start_times)
                    rc_tile = RasterCollectionTile(name, extent, np.array(band), wavelength=wavelength,start_times=start_times)
                result.append(rc_tile)
                i = i+1
            return result

        else:
            raise ValueError("Expected tile to have 3 or 4 dimensions (bands, x, y) or (time, bands, x, y)")

    def apply_tiles_spatiotemporal(self,function) -> ImageCollection:
        """
        Apply a function to a group of tiles with the same spatial key.
        :param function:
        :return:
        """

        def tilefunction(metadata:Metadata,openeo_metadata,geotrellis_tile:Tuple[SpaceTimeKey,Tile]):

            key = geotrellis_tile[0]
            extent = GeotrellisTimeSeriesImageCollection._mapTransform(metadata.layout_definition,key)

            data = UdfData({"EPSG":900913}, GeotrellisTimeSeriesImageCollection._tile_to_rastercollectiontile(geotrellis_tile[1].cells, extent,bands_metadata = openeo_metadata.get('bands',None) ))

            from openeo_udf.api.base import RasterCollectionTile
            exec(function,{'data':data,'RasterCollectionTile':RasterCollectionTile})
            result = data.raster_collection_tiles
            return (key,Tile(result[0].get_data(),geotrellis_tile[1].cell_type,geotrellis_tile[1].no_data_value))

        def rdd_function(openeo_metadata,rdd):
            floatrdd = rdd.convert_data_type(CellType.FLOAT64).to_numpy_rdd()
            floatrdd.srdd.map().groupByKey()
            return gps.TiledRasterLayer.from_numpy_rdd(rdd.layer_type,
                                                       floatrdd.map(
                                                    partial(tilefunction, rdd.layer_metadata, openeo_metadata)),
                                                       rdd.layer_metadata)
        from functools import partial
        return self.apply_to_levels(partial(rdd_function,self.metadata))


    def apply_tiles(self, function) -> 'ImageCollection':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)

        def tilefunction(metadata:Metadata,openeo_metadata,geotrellis_tile:Tuple[SpaceTimeKey,Tile]):

            key = geotrellis_tile[0]
            extent = GeotrellisTimeSeriesImageCollection._mapTransform(metadata.layout_definition,key)

            data = UdfData({"EPSG":900913}, GeotrellisTimeSeriesImageCollection._tile_to_rastercollectiontile(geotrellis_tile[1].cells, extent,bands_metadata = openeo_metadata.get('bands',None) ))

            from openeo_udf.api.base import RasterCollectionTile
            exec(function,{'data':data,'RasterCollectionTile':RasterCollectionTile})
            result = data.raster_collection_tiles
            return (key,Tile(result[0].get_data(),geotrellis_tile[1].cell_type,geotrellis_tile[1].no_data_value))

        def rdd_function(openeo_metadata,rdd):
            return gps.TiledRasterLayer.from_numpy_rdd(rdd.layer_type,
                                                rdd.convert_data_type(CellType.FLOAT64).to_numpy_rdd().map(
                                                    partial(tilefunction, rdd.layer_metadata, openeo_metadata)),
                                                rdd.layer_metadata)
        from functools import partial
        return self.apply_to_levels(partial(rdd_function,self.metadata))

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


    @classmethod
    def __reproject_polygon(cls, polygon: Union[Polygon, MultiPolygon], srs, dest_srs):
        import pyproj
        from shapely.ops import transform
        from functools import partial

        project = partial(
            pyproj.transform,
            pyproj.Proj(srs),  # source coordinate system
            pyproj.Proj(dest_srs))  # destination coordinate system

        return transform(project, polygon)  # apply projection



    def mask(self, polygon: Union[Polygon, MultiPolygon], srs="EPSG:4326") -> 'ImageCollection':
        max_level = self.pyramid.levels[self.pyramid.max_zoom]
        layer_crs = max_level.layer_metadata.crs
        reprojected_polygon = GeotrellisTimeSeriesImageCollection.__reproject_polygon(polygon,"+init="+srs,layer_crs)

        #TODO should we warn when masking generates an empty collection?
        return self.apply_to_levels(lambda rdd: rdd.to_spatial_layer().mask(
            reprojected_polygon,
             partition_strategy=None,
             options=gps.RasterizerOptions()))

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

    def zonal_statistics(self, regions, func, scale=1000, interval="day") -> 'Dict':
        return self.polygonal_mean_timeseries(regions)

    def polygonal_mean_timeseries(self, polygon: Union[Polygon, MultiPolygon]) -> Dict:
        max_level = self.pyramid.levels[self.pyramid.max_zoom]

        masked_layer = max_level.mask(polygon)

        def combine_cells(acc: List[Tuple[int, int]], tile) -> List[Tuple[int, int]]:  # [(sum, count)]
            n_bands = len(tile.cells)

            if not acc:
                acc = [(0, 0)] * n_bands

            for i in range(n_bands):
                grid = tile.cells[i]

                sum = grid[grid != tile.no_data_value].sum()
                count = (grid != tile.no_data_value).sum()

                acc[i] = acc[i][0] + sum, acc[i][1] + count

            return acc

        def combine_values(l1: List[Tuple[int, int]], l2: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
            for i in range(len(l2)):
                l1[i] = l1[i][0] + l2[i][0], l1[i][1] + l2[i][1]

            return l1

        polygon_mean_by_timestamp = masked_layer.to_numpy_rdd() \
            .map(lambda pair: (pair[0].instant, pair[1])) \
            .aggregateByKey([], combine_cells, combine_values)

        def to_mean(values: Tuple[int, int]) -> float:
            sum, count = values
            return sum / count

        return {timestamp.isoformat(): list(map(to_mean, values)) for timestamp, values in polygon_mean_by_timestamp.collect()}

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

        tiled = (format_options.get("format", "GTiff") == "GTiff" and
                 format_options.get("parameters", {}).get("tiled", False))

        if tiled:
            self.save_stitched_tiled(spatial_rdd, filename)
        else:
            spatial_rdd.save_stitched(filename)

        return filename

    def save_stitched_tiled(self, spatial_rdd, filename):
        import rasterio as rstr
        from affine import Affine
        import rasterio._warp as rwarp
        from math import log

        max_level = self.pyramid.levels[self.pyramid.max_zoom]

        spatial_rdd = spatial_rdd.persist()

        sorted_keys = sorted(spatial_rdd.collect_keys())

        upper_left_coords = GeotrellisTimeSeriesImageCollection._mapTransform(max_level.layer_metadata.layout_definition, sorted_keys[0])
        lower_right_coords = GeotrellisTimeSeriesImageCollection._mapTransform(max_level.layer_metadata.layout_definition, sorted_keys[-1])

        data = spatial_rdd.stitch()

        bands, w, h = data.cells.shape
        nodata = max_level.layer_metadata.no_data_value
        dtype = data.cells.dtype
        ex = Extent(xmin=upper_left_coords.left, ymin=lower_right_coords.bottom, xmax=lower_right_coords.right, ymax=upper_left_coords.top)
        cw, ch = (ex.xmax - ex.xmin) / w, (ex.ymax - ex.ymin) / h
        overview_level = int(log(w) / log(2) - 8)

        with rstr.io.MemoryFile() as memfile, open(filename, 'wb') as f:
            with memfile.open(driver='GTiff',
                              count=bands,
                              width=w,
                              height=h,
                              transform=Affine(cw, 0.0, ex.xmin,
                                               0.0, -ch, ex.ymax),
                              crs=rstr.crs.CRS.from_epsg(4326),
                              nodata=nodata,
                              dtype=dtype,
                              compress='lzw',
                              tiled=True) as mem:
                windows = list(mem.block_windows(1))
                for _, w in windows:
                    segment = data.cells[:, w.row_off:(w.row_off + w.height), w.col_off:(w.col_off + w.width)]
                    mem.write(segment, window=w)
                    mask_value = np.all(segment != nodata, axis=0).astype(np.uint8) * 255
                    mem.write_mask(mask_value, window=w)

                    overviews = [2 ** j for j in range(1, overview_level + 1)]
                    mem.build_overviews(overviews, rwarp.Resampling.nearest)
                    mem.update_tags(ns='rio_oveview', resampling=rwarp.Resampling.nearest.value)

            while True:
                chunk = memfile.read(8192)
                if not chunk:
                    break

                f.write(chunk)

    def _proxy_tms(self,tms):
        if 'TRAVIS' in os.environ:
            return tms.url_pattern
        else:
            host = tms.host
            port = tms.port
            self._proxy(host, port)
            url = "http://openeo.vgt.vito.be/tile/{z}/{x}/{y}.png"
            return url

    def _proxy(self, host, port):
        from kazoo.client import KazooClient
        zk = KazooClient(hosts='epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181')
        zk.start()
        zk.ensure_path("discovery/services/openeo-viewer-test")
        # id = uuid.uuid4()
        # print(id)
        id = 0
        zk.ensure_path("discovery/services/openeo-viewer-test/" + str(id))
        zk.set("discovery/services/openeo-viewer-test/" + str(id), str.encode(json.dumps(
            {"name": "openeo-viewer-test", "id": str(id), "address": host, "port": port, "sslPort": None,
             "payload": None, "registrationTimeUTC": datetime.utcnow().strftime('%s'), "serviceType": "DYNAMIC"})))
        zk.stop()
        zk.close()

    def tiled_viewing_service(self,**kwargs) -> Dict:
        type = kwargs['type']

        if type.lower() == "tms":
            if self.pyramid.layer_type != gps.LayerType.SPATIAL:
                spatial_rdd = self.apply_to_levels(lambda rdd: rdd.to_spatial_layer())
            print("Creating persisted pyramid.")
            spatial_rdd = spatial_rdd.apply_to_levels(lambda rdd: rdd.partitionBy(gps.SpatialPartitionStrategy(num_partitions=40,bits=2)).persist())
            print("Pyramid ready.")

            pyramid = spatial_rdd.pyramid
            def render_rgb(tile):
                import numpy as np
                from PIL import Image
                rgba = np.dstack([tile.cells[0] * (255.0 / 2000.0), tile.cells[1] * (255.0 / 2000.0),
                                  tile.cells[2] * (255.0 / 2000.0)]).astype('uint8')
                img = Image.fromarray(rgba, mode='RGB')
                return img

            # greens = color.get_colors_from_matplotlib(ramp_name="YlGn")
            greens = gps.ColorMap.build(breaks=[x / 100.0 for x in range(0, 100)], colors="YlGn")

            if(self.tms is None):
                self.tms = TMS.build(source=pyramid,display = greens)
                self.tms.bind(host="0.0.0.0",requested_port=0)

            url = self._proxy_tms(self.tms)
            level_bounds = {level: tiled_raster_layer.layer_metadata.bounds for level, tiled_raster_layer in
                            pyramid.levels.items()}
            # TODO handle cleanup of tms'es
            return {
                "type": "TMS",
                "url": url,
                "bounds": level_bounds
            }
        else:
            if ('wmts' in self.__dir__()):
                self.wmts.stop()
            pysc = gps.get_spark_context()

            self.wmts = pysc._jvm.be.vito.eodata.gwcgeotrellis.wmts.WMTSServer.createServer()

            if(kwargs.get("style") is not None):
                color_map = gps.ColorMap.nlcd_colormap()
                srdd_dict = {k: v.srdd.rdd() for k, v in self.pyramid.levels.items()}
                self.wmts.addPyramidLayer("RDD", srdd_dict,color_map.cmap)
            else:
                srdd_dict = {k: v.srdd.rdd() for k, v in self.pyramid.levels.items()}
                self.wmts.addPyramidLayer("RDD", srdd_dict)

            import socket
            host = [l for l in
                              ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                               [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                                 [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]])
                              if l][0][0]


            self._proxy(host, self.wmts.getPort())
            print(self.wmts.getURI())
            url ="http://openeo.vgt.vito.be/tile/service/wmts"

            return {
                "type": "WMTS",
                "url": url
                #    "bounds":level_bounds
            }
