import collections
import collections.abc
import json
import logging
import math
import os
import pathlib
import subprocess
import tempfile
from datetime import datetime, date
from functools import partial
from typing import Dict, List, Union, Tuple, Iterable, Callable

import geopyspark as gps
import numpy as np
import pandas as pd
import pyproj
import pytz
import xarray as xr
from geopyspark import TiledRasterLayer, Pyramid, Tile, SpaceTimeKey, SpatialKey, Metadata
from geopyspark.geotrellis import Extent, ResampleMethod, crs_to_proj4
from geopyspark.geotrellis.constants import CellType
from pandas import Series
from py4j.java_gateway import JVMView
from shapely.geometry import Point, Polygon, MultiPolygon, GeometryCollection

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.metadata import CollectionMetadata, Band, Dimension
from openeo.udf import UdfData, run_udf_code
from openeo.udf.xarraydatacube import XarrayDataCube, XarrayIO
from openeo.util import dict_no_none
from openeo_driver.datacube import DriverDataCube
from openeo_driver.datastructs import ResolutionMergeArgs
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.errors import FeatureUnsupportedException, OpenEOApiException, InternalException, \
    ProcessParameterInvalidException
from openeo_driver.save_result import AggregatePolygonResult, AggregatePolygonSpatialResult, AggregatePolygonResultCSV
from openeo_driver.utils import EvalEnv
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.ml_model import AggregateSpatialVectorCube
from openeogeotrellis.utils import to_projected_polygons, log_memory
from openeogeotrellis._version import __version__ as softwareversion
from shapely.geometry.base import BaseGeometry

_log = logging.getLogger(__name__)


class GeopysparkCubeMetadata(CollectionMetadata):
    """
    GeoPySpark Cube metadata (additional tracking of spatial and temporal extent
    """
    # TODO move to python driver?

    def __init__(
            self, metadata: dict, dimensions: List[Dimension] = None,
            spatial_extent: dict = None, temporal_extent: tuple = None
    ):
        super().__init__(metadata=metadata, dimensions=dimensions)
        self._spatial_extent = spatial_extent
        self._temporal_extent = temporal_extent
        if(self.has_temporal_dimension()):
            self.temporal_dimension.extent = temporal_extent


    def _clone_and_update(
            self, metadata: dict = None, dimensions: List[Dimension] = None,
            spatial_extent: dict = None, temporal_extent: tuple = None, **kwargs
    ) -> 'GeopysparkCubeMetadata':
        # noinspection PyTypeChecker
        return super()._clone_and_update(
            metadata=metadata, dimensions=dimensions,
            spatial_extent=spatial_extent or self._spatial_extent,
            temporal_extent=temporal_extent or self._temporal_extent,
            **kwargs
        )

    def filter_bbox(self, west, south, east, north, crs) -> 'GeopysparkCubeMetadata':
        """Create new metadata instance with spatial extent"""
        # TODO take intersection with existing extent
        return self._clone_and_update(
            spatial_extent={"west": west, "south": south, "east": east, "north": north, "crs": crs}
        )

    @property
    def spatial_extent(self) -> dict:
        return self._spatial_extent

    def filter_temporal(self, start, end) -> 'GeopysparkCubeMetadata':
        """Create new metadata instance with temporal extent"""
        # TODO take intersection with existing extent
        return self._clone_and_update(temporal_extent=(start, end))

    @property
    def temporal_extent(self) -> tuple:
        return self._temporal_extent

    @property
    def opensearch_link_titles(self) -> List[str]:
        """Get opensearch_link_titles from band dimension"""
        names_with_aliases = zip(self.band_dimension.band_names, self.band_dimension.band_aliases)
        return [n[1][0] if n[1] else n[0] for n in names_with_aliases]


SpatialExtent = collections.namedtuple("SpatialExtent", ["top", "bottom", "right", "left", "height", "width"])


class GeopysparkDataCube(DriverDataCube):

    metadata: GeopysparkCubeMetadata = None

    def __init__(
            self, pyramid: Pyramid,
            metadata: GeopysparkCubeMetadata = None
    ):
        super().__init__(metadata=metadata or GeopysparkCubeMetadata({}))
        self.pyramid = pyramid

    def _get_jvm(self) -> JVMView:
        # TODO: cache this?
        return gps.get_spark_context()._gateway.jvm

    def _is_spatial(self):
        return self.get_max_level().layer_type == gps.LayerType.SPATIAL

    def apply_to_levels(self, func, metadata: GeopysparkCubeMetadata = None) -> 'GeopysparkDataCube':
        """
        Applies a function to each level of the pyramid. The argument provided to the function is of type TiledRasterLayer

        :param func:
        :return:
        """
        pyramid = Pyramid({k: func(l) for k, l in self.pyramid.levels.items()})
        return GeopysparkDataCube(pyramid=pyramid, metadata=metadata or self.metadata)

    @staticmethod
    def _convert_celltype(layer: TiledRasterLayer, cell_type: CellType):
        tiled_raster_layer = TiledRasterLayer(layer.layer_type, layer.srdd.convertDataType(cell_type))
        tiled_raster_layer = GeopysparkDataCube._transform_metadata(tiled_raster_layer, cellType=cell_type)
        return tiled_raster_layer

    def _create_tilelayer(self,contextrdd, layer_type, zoom_level):
        jvm = self._get_jvm()
        spatial_tiled_raster_layer = jvm.geopyspark.geotrellis.SpatialTiledRasterLayer
        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer

        if layer_type == gps.LayerType.SPATIAL:
            srdd = spatial_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom_level),contextrdd)
        else:
            srdd = temporal_tiled_raster_layer.apply(jvm.scala.Option.apply(zoom_level),contextrdd)

        return gps.TiledRasterLayer(layer_type, srdd)

    def _apply_to_levels_geotrellis_rdd(self, func, metadata: GeopysparkCubeMetadata = None, target_type = None):
        """
        Applies a function to each level of the pyramid. The argument provided to the function is the Geotrellis ContextRDD.

        :param func:
        :return:
        """
        pyramid = Pyramid({
            k: self._create_tilelayer(func(l.srdd.rdd(), k), l.layer_type if target_type==None else target_type , k)
            for k, l in self.pyramid.levels.items()
        })
        return GeopysparkDataCube(pyramid=pyramid, metadata=metadata or self.metadata)


    def _data_source_type(self):
        return self.metadata.get("_vito", "data_source", "type", default="Accumulo")

    # TODO: deprecated
    def date_range_filter(
            self, start_date: Union[str, datetime, date], end_date: Union[str, datetime, date]
    ) -> 'GeopysparkDataCube':
        return self.apply_to_levels(lambda rdd: rdd.filter_by_times([pd.to_datetime(start_date),pd.to_datetime(end_date)]))

    def filter_temporal(self, start: str, end: str) -> 'GeopysparkDataCube':
        # TODO: is this necessary? Temporal range is handled already at load_collection time
        return self.apply_to_levels(
            lambda rdd: rdd.filter_by_times([pd.to_datetime(start), pd.to_datetime(end)]),
            metadata=self.metadata.filter_temporal(start, end)
        )

    def filter_bbox(self, west, east, north, south, crs=None, base=None, height=None) -> 'GeopysparkDataCube':
        # Bbox is handled at load_collection time
        return GeopysparkDataCube(
            pyramid=self.pyramid,
            metadata=self.metadata.filter_bbox(west=west, south=south, east=east, north=north, crs=crs)
        )

    def filter_spatial(self, geometries: Union[Polygon, MultiPolygon]) -> 'GeopysparkDataCube':
        # TODO: support more geometry types but geopyspark.geotrellis.layer.TiledRasterLayer.mask doesn't seem to work
        #  with e.g. GeometryCollection

        # TODO: update metadata?
        return self.mask_polygon(geometries)

    def filter_bands(self, bands) -> 'GeopysparkDataCube':
        band_indices = [self.metadata.get_band_index(b) for b in bands]
        _log.info("filter_bands({b!r}) -> indices {i!r}".format(b=bands, i=band_indices))
        return self.apply_to_levels(lambda rdd: rdd.bands(band_indices), metadata=self.metadata.filter_bands(bands))

    def rename_dimension(self, source: str, target: str) -> 'GeopysparkDataCube':
        return GeopysparkDataCube(pyramid=self.pyramid, metadata=self.metadata.rename_dimension(source, target))

    def apply(self, process: str, arguments: dict = {}) -> 'GeopysparkDataCube':
        from openeogeotrellis.backend import SingleNodeUDFProcessGraphVisitor, GeoPySparkBackendImplementation
        if isinstance(process, dict):
            process = GeoPySparkBackendImplementation.accept_process_graph(process)

        if isinstance(process, GeotrellisTileProcessGraphVisitor):
            #apply should leave metadata intact, so can do a simple call?
            # Note: It's not obvious from its name, but `reduce_bands` not only supports reduce operations,
            # also `apply` style local unary mapping operations.
            return  self._apply_bands_dimension(process)
        if isinstance(process, SingleNodeUDFProcessGraphVisitor):
            udf = process.udf_args.get('udf', None)
            context = process.udf_args.get('context', {})
            if not isinstance(udf, str):
                raise ValueError(
                    "The 'run_udf' process requires at least a 'udf' string argument, but got: '%s'." % udf)
            return self.apply_tiles(udf,context)
        elif isinstance(process,str):
            #old 04x code path
            if 'y' in arguments:
                raise NotImplementedError("Apply only supports unary operators,"
                                          " but got {p!r} with {a!r}".format(p=process, a=arguments))
            applyProcess = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().applyProcess
            return self._apply_to_levels_geotrellis_rdd(lambda rdd, k: applyProcess(rdd, process))
        else:
            raise FeatureUnsupportedException(f"Unsupported: apply with {process}")

    def apply_dimension(self, process, dimension: str, target_dimension: str = None,
                        context: dict = None, env: EvalEnv = None) -> 'DriverDataCube':
        from openeogeotrellis.backend import SingleNodeUDFProcessGraphVisitor, GeoPySparkBackendImplementation
        if isinstance(process, dict):
            process = GeoPySparkBackendImplementation.accept_process_graph(process)
        if isinstance(process, GeotrellisTileProcessGraphVisitor):
            if self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name:
                from openeo_driver.ProcessGraphDeserializer import convert_node
                context = convert_node(context, env=env)
                pysc = gps.get_spark_context()
                if target_dimension == self.metadata.band_dimension.name:
                    #reduce the time dimension into the bands dimension
                    result_collection = self._apply_to_levels_geotrellis_rdd(
                        lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().applyTimeDimensionTargetBands(rdd,
                                                                                                                process.builder,
                                                                                                                context if isinstance(
                                                                                                                    context,
                                                                                                                    dict) else {}), target_type=gps.LayerType.SPATIAL)
                    result_collection.metadata = result_collection.metadata.reduce_dimension(dimension)
                    return result_collection
                else:
                    return self._apply_to_levels_geotrellis_rdd(
                        lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().applyTimeDimension(rdd,process.builder,context if isinstance(context,dict) else {}))
            elif self.metadata.has_band_dimension() and dimension == self.metadata.band_dimension.name:
                return self._apply_bands_dimension(process)
            else:
                raise FeatureUnsupportedException(f"apply_dimension along dimension {dimension} is not supported. These dimensions are available: " + str(self.metadata.dimension_names()))
        if isinstance(process, SingleNodeUDFProcessGraphVisitor):
            udf = process.udf_args.get('udf', None)
            return self._run_udf_dimension(udf, context, dimension, env)
        raise FeatureUnsupportedException(f"Unsupported: apply_dimension with {process}")

    def reduce(self, reducer: str, dimension: str) -> 'GeopysparkDataCube':
        # TODO: rename this to reduce_temporal (because it only supports temporal reduce)?
        from .numpy_aggregators import var_composite, std_composite, min_composite, max_composite, sum_composite, median_composite, product_composite

        reducer = self._normalize_temporal_reducer(dimension, reducer)

        if reducer == 'Variance':
            return self._aggregate_over_time_numpy(var_composite)
        elif reducer == 'StandardDeviation':
            return self._aggregate_over_time_numpy(std_composite)
        elif reducer == 'Min':
            return self._aggregate_over_time_numpy(min_composite)
        elif reducer == 'Max':
            return self._aggregate_over_time_numpy(max_composite)
        elif reducer == 'Sum':
            return self._aggregate_over_time_numpy(sum_composite)
        elif reducer == 'Product':
            return self._aggregate_over_time_numpy(product_composite)
        elif reducer == 'Median':
            return self._aggregate_over_time_numpy(median_composite)
        else:
            return self.apply_to_levels(lambda layer: layer.to_spatial_layer().aggregate_by_cell(reducer))

    def reduce_bands(self, pgVisitor: GeotrellisTileProcessGraphVisitor) -> 'GeopysparkDataCube':
        """
        TODO Define in super class? API is not yet ready for client side...
        :param pgVisitor:
        :return:
        """
        result = self._apply_bands_dimension(pgVisitor)
        if result.metadata.has_band_dimension():
            result.metadata = result.metadata.reduce_dimension(result.metadata.band_dimension.name)
        return result

    def _apply_bands_dimension(self, pgVisitor: GeotrellisTileProcessGraphVisitor) -> 'GeopysparkDataCube':
        """
        Apply a process graph to every tile, with tile.bands (List[Tile]) as process input.
        """
        # All processing is done with float32 as cell type.
        float_datacube = self.apply_to_levels(lambda layer: layer.convert_data_type("float32"))

        # Apply process to every tile, with tile.bands (List[Tile]) as process input.
        # This is done for the entire pyramid.
        pysc = gps.get_spark_context()
        result_cube: GeopysparkDataCube = float_datacube._apply_to_levels_geotrellis_rdd(
            lambda rdd, level:
                pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mapBands(rdd, pgVisitor.builder)
        )

        # Convert/Restrict cell type after processing.
        target_cell_type = pgVisitor.builder.getOutputCellType().name()
        return result_cube.apply_to_levels(lambda layer: self._convert_celltype(layer, target_cell_type))

    def _normalize_temporal_reducer(self, dimension: str, reducer: str) -> str:
        if dimension != self.metadata.temporal_dimension.name:
            raise FeatureUnsupportedException('Reduce on dimension {d!r} not supported'.format(d=dimension))
        if reducer.upper() in ["MIN", "MAX", "SUM", "MEAN", "VARIANCE", "MEDIAN", "FIRST", "LAST", "PRODUCT"]:
            reducer = reducer.lower().capitalize()
        elif reducer.upper() == "SD":
            reducer = "StandardDeviation"
        else:
            raise FeatureUnsupportedException('Reducer {r!r} not supported'.format(r=reducer))
        return reducer

    def add_dimension(self, name: str, label: str, type: str = None):
        return GeopysparkDataCube(
            pyramid=self.pyramid,
            metadata=self.metadata.add_dimension(name=name, label=label, type=type)
        )

    def drop_dimension(self, name: str):
        if name not in self.metadata.dimension_names():
            raise OpenEOApiException(status_code=400, code="DimensionNotAvailable", message=
                """Dimension with name '{}' does not exist""".format(name))

        if self.metadata.has_temporal_dimension() and self.metadata.temporal_dimension.name == name:
            pyramid = Pyramid(map(lambda l: l.to_spatial_layer(), self.pyramid.levels))
            return GeopysparkDataCube(
                pyramid=pyramid,
                metadata=self.metadata.drop_dimension(name=name)
            )
        elif self.metadata.has_band_dimension() and self.metadata.band_dimension.name == name:
            if not len(self.metadata.bands) == 1:
                raise OpenEOApiException(status_code=400, code="DimensionLabelCountMismatch", message=
                    """Band dimension can only be dropped if there is only 1 band left in the datacube""")
            else:
                return GeopysparkDataCube(
                    pyramid=self.pyramid,
                    metadata=self.metadata.drop_dimension(name=name)
                )
        else:
            raise OpenEOApiException(status_code=400, code="DimensionNotAvailable", message=
                """'drop_dimension' is only supported for dimension types 'bands' and 'temporal'.""")

    def dimension_labels(self, dimension: str):
        if dimension not in self.metadata.dimension_names():
            raise OpenEOApiException(status_code=400, code="DimensionNotAvailable", message=
                """Dimension with name '{}' does not exist""".format(dimension))

        if self.metadata.has_temporal_dimension() and self.metadata.temporal_dimension.name == dimension:
            return sorted(set(map(lambda k: k.instant, self.pyramid.levels[self.pyramid.max_zoom].collect_keys())))
        elif self.metadata.has_band_dimension() and self.metadata.band_dimension.name == dimension:
            return self.metadata.band_names
        else:
            raise OpenEOApiException(status_code=400, code="DimensionNotAvailable", message=
                """'dimension_labels' is only supported for dimension types 'bands' and 'temporal'.""")

    def rename_labels(self, dimension: str, target: list, source: list=None) -> 'GeopysparkDataCube':
        """ Renames the labels of the specified dimension in the data cube from source to target.

            :param dimension: Dimension name
            :param target: The new names for the labels.
            :param source: The names of the labels as they are currently in the data cube.

            :return: An GeopysparkDataCube instance
        """
        return GeopysparkDataCube(
            pyramid=self.pyramid,
            metadata=self.metadata.rename_labels(dimension,target,source)
        )

    @classmethod
    def _mapTransform(cls, layoutDefinition, spatialKey) -> SpatialExtent:
        ex = layoutDefinition.extent
        x_range = ex.xmax - ex.xmin
        xinc = x_range / layoutDefinition.tileLayout.layoutCols
        yrange = ex.ymax - ex.ymin
        yinc = yrange / layoutDefinition.tileLayout.layoutRows
        return SpatialExtent(
            top=ex.ymax - yinc * spatialKey.row,
            bottom=ex.ymax - yinc * (spatialKey.row + 1),
            right=ex.xmin + xinc * (spatialKey.col + 1),
            left=ex.xmin + xinc * spatialKey.col,
            height=layoutDefinition.tileLayout.tileCols,
            width=layoutDefinition.tileLayout.tileRows
        )

    @classmethod
    def _numpy_to_xarraydatacube(
            cls,
            bands_numpy: np.ndarray, extent: SpatialExtent,
            band_coordinates: List[str],
            time_coordinates: pd.DatetimeIndex = None
    ) -> XarrayDataCube:
        """
        Converts a numpy array representing a tile to an XarrayDataCube by adding coordinates and dimension labels.

        :param bands_numpy:
            The numpy array with shape = (a,b,c,d).
            With,
                a: time     (#dates) (if exists)
                b: bands    (#bands) (if exists)
                c: y-axis   (#cells)
                d: x-axis   (#cells)
            E.g. (5,2,256,256) has tiles of 256x256 cells, each with 2 bands for 5 given dates.
        :param extent:
            The SpatialExtent of the tile in order to calculate the coordinates for the x and y dimensions.
            If None then the resulting xarray will have no x,y coordinates.
            E.g. SpatialExtent(bottom: 140, top: 145, left: 60, right: 65, height: 256, width: 256)
        :param band_coordinates: A list of band names to act as coordinates for the band dimension (if exists).
        :param time_coordinates: A list of dates to act as coordinates for the time dimension (if exists).
        :return: An XarrayDatacube containing the given numpy array with the correct labels and coordinates.
        """

        coords = {}
        dims = ('bands','y', 'x')

        # time coordinates if exists
        if len(bands_numpy.shape) == 4:
            #we have a temporal dimension
            coords = {'t':time_coordinates}
            dims = ('t' ,'bands','y', 'x')

        # Set the band names as xarray coordinates if exists.
        if band_coordinates:
            # TODO: also use the band dimension name (`band_dimension.name`) instead of hardcoded "bands"?
            coords['bands'] = band_coordinates
            # Make sure the numpy array has the right shape.
            band_count = bands_numpy.shape[dims.index('bands')]
            if band_count != len(band_coordinates):
                raise OpenEOApiException(status_code=400,message=
                """In run_udf, the data has {b} bands, while the 'bands' dimension has {len_dim} labels. 
                These labels were set on the dimension: {labels}. Please investigate if dimensions and labels are correct."""
                                         .format(b=band_count, len_dim = len(band_coordinates), labels=str(band_coordinates)))

        # Set the X and Y coordinates.
        # this is tricky because if apply_neighborhood is used, then extent is the area without overlap
        # in addition the coordinates are computed to cell center.
        #
        # VERY IMPORTANT NOTE: building x,y coordinates assumes that extent and bands_numpy is compatible
        # as if it is concatenating an image:
        #  * spatial dimension order is [y,x] <- row-column order
        #  * origin is upper left corner
        #
        # NOTE.2.: for optimization reasons the y coordinate is computed decreasing instead of flipping the datacube (expensive)
        # NOTE.3.: if extent is None, no coordinates will be generated (UDF's dominantly don't use x&y)
        if extent is not None: 
            gridx=(extent.right-extent.left)/extent.width
            gridy=(extent.top-extent.bottom)/extent.height
            xdelta=gridx*0.5*(bands_numpy.shape[-1]-extent.width) 
            ydelta=gridy*0.5*(bands_numpy.shape[-2]-extent.height)
            xmin=extent.left   -xdelta 
            xmax=extent.right  +xdelta 
            ymin=extent.bottom -ydelta 
            ymax=extent.top    +ydelta 
            coords['x']=np.linspace(xmin+0.5*gridx,xmax-0.5*gridx,bands_numpy.shape[-1],dtype=np.float32)
            coords['y']=np.linspace(ymax-0.5*gridy,ymin+0.5*gridy,bands_numpy.shape[-2],dtype=np.float32)

        the_array = xr.DataArray(bands_numpy, coords=coords,dims=dims,name="openEODataChunk")
        return XarrayDataCube(the_array)

    def apply_tiles_spatiotemporal(self, function: str, context={}) -> 'GeopysparkDataCube':
        """
        Group tiles by SpatialKey, then apply a Python function to every group of tiles.
        :param function: A string containing a Python function that handles groups of tiles, each labeled by date.
        :return: The original data cube with its tiles transformed by the function.
        """

        # Early compile to detect syntax errors
        compiled_code = compile(function,'UDF.py',mode='exec')

        def tile_function(metadata:Metadata,
                          openeo_metadata: GeopysparkCubeMetadata,
                          tiles: Tuple[gps.SpatialKey, List[Tuple[SpaceTimeKey, Tile]]]
            ) -> 'List[Tuple[gps.SpatialKey, List[Tuple[SpaceTimeKey, Tile]]]]':
            tile_list = list(tiles[1])
            # Sort by instant
            tile_list.sort(key=lambda tup: tup[0].instant)
            dates = map(lambda t: t[0].instant, tile_list)
            arrays = map(lambda t: t[1].cells, tile_list)
            multidim_array = np.array(list(arrays))

            extent = GeopysparkDataCube._mapTransform(metadata.layout_definition, tile_list[0][0])

            datacube: XarrayDataCube = GeopysparkDataCube._numpy_to_xarraydatacube(
                multidim_array,
                extent=extent,
                band_coordinates=openeo_metadata.band_dimension.band_names if openeo_metadata.has_band_dimension() else None,
                time_coordinates=pd.DatetimeIndex(dates)
            )

            data = UdfData(proj={"EPSG": 900913}, datacube_list=[datacube], user_context=context)
            result_data = run_udf_code(code=function, data=data)
            cubes = result_data.get_datacube_list()
            if len(cubes) != 1:
                raise ValueError("The provided UDF should return one datacube, but got: " + str(cubes))
            result_array: xr.DataArray = cubes[0].array
            if 't' in result_array.dims:
                return [(SpaceTimeKey(col=tiles[0].col, row=tiles[0].row, instant=pd.Timestamp(timestamp)),
                         Tile(array_slice.values, CellType.FLOAT32, tile_list[0][1].no_data_value))
                        for timestamp, array_slice in result_array.groupby('t')]
            else:
                return [(SpaceTimeKey(col=tiles[0].col, row=tiles[0].row, instant=datetime.fromisoformat('2020-01-01T00:00:00')),
                         Tile(result_array.values, CellType.FLOAT32, tile_list[0][1].no_data_value))]

        def rdd_function(openeo_metadata: GeopysparkCubeMetadata, rdd: TiledRasterLayer) -> TiledRasterLayer:
            float_rdd = rdd.convert_data_type(CellType.FLOAT32).to_numpy_rdd()

            def to_spatial_key(tile: Tuple[SpaceTimeKey, Tile]):
                key: SpatialKey = gps.SpatialKey(tile[0].col, tile[0].row)
                value: Tuple[SpaceTimeKey, Tile] = (tile[0], tile[1])
                return (key, value)

            # Group all tiles by SpatialKey. Save the SpaceTimeKey in the value with the Tile.
            spatially_grouped = float_rdd.map(lambda tile: to_spatial_key(tile)).groupByKey()

            # Apply the tile_function to all tiles with the same spatial key.
            numpy_rdd = spatially_grouped.flatMap(
                log_memory(partial(tile_function, rdd.layer_metadata, openeo_metadata))
            )

            # Convert the result back to a TiledRasterLayer.
            metadata = GeopysparkDataCube._transform_metadata(rdd.layer_metadata, cellType=CellType.FLOAT32)
            _log.info(f"apply_neighborhood created datacube {metadata}")
            return gps.TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPACETIME, numpy_rdd, metadata)

        return self.apply_to_levels(partial(rdd_function, self.metadata))

    def chunk_polygon(
            self, reducer: Union[ProcessGraphVisitor, Dict], chunks: MultiPolygon, mask_value: float,
            env: EvalEnv, context: dict = None,
    ) -> 'GeopysparkDataCube':
        from openeogeotrellis.backend import SingleNodeUDFProcessGraphVisitor, GeoPySparkBackendImplementation
        if isinstance(reducer, dict):
            reducer = GeoPySparkBackendImplementation.accept_process_graph(reducer)
        chunks: List[Polygon] = chunks.geoms
        jvm = self._get_jvm()

        result_collection = None
        if isinstance(reducer, SingleNodeUDFProcessGraphVisitor):
            from openeo_driver.ProcessGraphDeserializer import convert_node
            udf = reducer.udf_args.get('udf', None)
            context: dict = reducer.udf_args.get('context', {})
            context: dict = convert_node(context, env=env) # Resolve "from_parameter" references in context object
            if not isinstance(udf, str):
                raise ValueError("The 'run_udf' process requires at least a 'udf' string argument, but got: '%s'." % udf)
            # Polygons should use the same projection as the rdd.
            reprojected_polygons: jvm.org.openeo.geotrellis.ProjectedPolygons \
                = to_projected_polygons(jvm, GeometryCollection(chunks))
            band_names = self.metadata.band_dimension.band_names

            def rdd_function(rdd, _zoom):
                return jvm.org.openeo.geotrellis.udf.Udf.runChunkPolygonUserCode(
                    udf, rdd, reprojected_polygons, band_names, context, mask_value
                )

            # All JEP implementation work with float cell types.
            float_cube = self.apply_to_levels(lambda layer: self._convert_celltype(layer, "float32"))
            result_collection = float_cube._apply_to_levels_geotrellis_rdd(
                rdd_function, self.metadata, gps.LayerType.SPACETIME
            )
        else:
            # Use OpenEOProcessScriptBuilder.
            raise NotImplementedError()
        return result_collection

    def reduce_dimension(
            self, reducer: Union[ProcessGraphVisitor, Dict], dimension: str, env: EvalEnv,
            binary=False, context=None,
    ) -> 'GeopysparkDataCube':
        from openeogeotrellis.backend import SingleNodeUDFProcessGraphVisitor,GeoPySparkBackendImplementation
        if isinstance(reducer,dict):
            reducer = GeoPySparkBackendImplementation.accept_process_graph(reducer)

        result_collection = None
        if isinstance(reducer,SingleNodeUDFProcessGraphVisitor):
            udf = reducer.udf_args.get('udf',None)
            context = reducer.udf_args.get('context', {})
            result_collection = self._run_udf_dimension(udf, context, dimension, env)
        elif self.metadata.has_band_dimension() and dimension == self.metadata.band_dimension.name:
            result_collection = self._apply_bands_dimension(reducer)
        elif hasattr(reducer,'processes') and isinstance(reducer.processes,dict) and len(reducer.processes) == 1:
            result_collection = self.reduce(reducer.processes.popitem()[0],dimension)
        else:
            raise FeatureUnsupportedException(
                "Unsupported combination of reducer %s and dimension %s." % (reducer, dimension))
        if result_collection is not None:
            result_collection.metadata = result_collection.metadata.reduce_dimension(dimension)
            if self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name and self.pyramid.layer_type != gps.LayerType.SPATIAL:
                result_collection = result_collection.apply_to_levels(lambda rdd:  rdd.to_spatial_layer() if rdd.layer_type != gps.LayerType.SPATIAL else rdd)
        return result_collection

    def _run_udf_dimension(self, udf, context, dimension, env):
        # TODO Putting this import at toplevel breaks things at the moment (circular import issues)
        from openeo_driver.ProcessGraphDeserializer import convert_node
        # Resolve "from_parameter" references in context object
        context = convert_node(context, env=env)
        if not isinstance(udf, str):
            raise ValueError("The 'run_udf' process requires at least a 'udf' string argument, but got: '%s'." % udf)
        if self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name:
            # EP-2760 a special case of reduce where only a single udf based callback is provided. The more generic case is not yet supported.
            return self.apply_tiles_spatiotemporal(udf, context)
        elif self.metadata.has_band_dimension() and dimension == self.metadata.band_dimension.name:
            return self.apply_tiles(udf, context)
        else:
            raise FeatureUnsupportedException(f"reduce_dimension with UDF along dimension {dimension} is not supported")

    def apply_tiles(self, function, context={}, runtime="python") -> 'GeopysparkDataCube':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)
        if runtime == 'Python-Jep':
            band_names = self.metadata.band_dimension.band_names

            def rdd_function(rdd, _zoom):
                jvm = gps.get_spark_context()._jvm
                udf = jvm.org.openeo.geotrellis.udf.Udf
                return udf.runUserCode(function, rdd, band_names, context)

            # All JEP implementation work with the float datatype.
            float_cube = self.apply_to_levels(lambda layer: self._convert_celltype(layer, "float32"))
            return float_cube._apply_to_levels_geotrellis_rdd(rdd_function, self.metadata, gps.LayerType.SPACETIME)
        else:
            def rdd_function(openeo_metadata: GeopysparkCubeMetadata, rdd: TiledRasterLayer):
                """
                Apply a user defined function to every tile in a TiledRasterLayer
                and return the transformed TiledRasterLayer.
                """
                def tile_function(metadata: Metadata,
                                  openeo_metadata: GeopysparkCubeMetadata,
                                  geotrellis_tile: Tuple[SpaceTimeKey, Tile]
                                  ) -> 'Tuple[SpaceTimeKey, Tile]':
                    """
                    Apply a user defined function to a geopyspark.geotrellis.Tile and return the transformed tile.
                    """

                    # Setup the UDF input data.
                    key = geotrellis_tile[0]
                    extent = GeopysparkDataCube._mapTransform(metadata.layout_definition, key)
                    datacube: XarrayDataCube = GeopysparkDataCube._numpy_to_xarraydatacube(
                        geotrellis_tile[1].cells,
                        extent=extent,
                        band_coordinates=openeo_metadata.band_dimension.band_names
                    )
                    data = UdfData(proj={"EPSG": 900913}, datacube_list=[datacube], user_context=context)

                    # Run UDF.
                    result_data = run_udf_code(code=function, data=data)

                    # Handle the resulting xarray datacube.
                    cubes: List[XarrayDataCube] = result_data.get_datacube_list()
                    if len(cubes) != 1:
                        raise ValueError("The provided UDF should return one datacube, but got: " + str(cubes))
                    result_array: xr.DataArray = cubes[0].array
                    _log.info(f"apply_tiles tilefunction result dims: {result_array.dims}")
                    result_tile = Tile(result_array.values,
                                       geotrellis_tile[1].cell_type,
                                       geotrellis_tile[1].no_data_value)
                    return (key, result_tile)

                # Convert TiledRasterLayer to PySpark RDD to access the Scala RDD cell values in Python.
                numpy_rdd = rdd.convert_data_type(CellType.FLOAT32).to_numpy_rdd()

                # Apply the UDF to every tile in the RDD.
                # Note rdd_metadata variable:
                # if rdd.layer_metadata is passed directly in lambda function it will try to serialize the entire rdd!
                rdd_metadata = rdd.layer_metadata
                numpy_rdd = numpy_rdd.map(
                    log_memory(partial(tile_function, rdd_metadata, openeo_metadata)),
                    preservesPartitioning=True
                )

                # Return the result back as a TiledRasterLayer.
                metadata = GeopysparkDataCube._transform_metadata(rdd.layer_metadata, cellType=CellType.FLOAT32)
                return gps.TiledRasterLayer.from_numpy_rdd(rdd.layer_type, numpy_rdd, metadata)

        # Apply the UDF to every tile for every zoom level of the pyramid.
        return self.apply_to_levels(partial(rdd_function, self.metadata))

    def aggregate_time(self, temporal_window, aggregationfunction) -> Series :
        #group keys
        #reduce
        pass

    def aggregate_temporal(
            self, intervals: List, labels: List, reducer, dimension: str = None, context:dict = None
    ) -> 'GeopysparkDataCube':
        """ Computes a temporal aggregation based on an array of date and/or time intervals.

            Calendar hierarchies such as year, month, week etc. must be transformed into specific intervals by the clients. For each interval, all data along the dimension will be passed through the reducer. The computed values will be projected to the labels, so the number of labels and the number of intervals need to be equal.

            If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :param intervals: Left-closed temporal intervals, which are allowed to overlap. Each temporal interval in the array has exactly two elements:
                                The first element is the start of the temporal interval. The specified instance in time is included in the interval.
                                The second element is the end of the temporal interval. The specified instance in time is excluded from the interval.
            :param labels: Labels for the intervals. The number of labels and the number of groups need to be equal.
            :param reducer: A reducer to be applied on all values along the specified dimension. The reducer must be a callable process (or a set processes) that accepts an array and computes a single return value of the same type as the input values, for example median.
            :param dimension: The temporal dimension for aggregation. All data along the dimension will be passed through the specified reducer. If the dimension is not set, the data cube is expected to only have one temporal dimension.

            :return: A data cube containing  a result for each time window
        """
        reformat_date = lambda d : pd.to_datetime(d).strftime('%Y-%m-%dT%H:%M:%SZ')
        date_list = []
        for interval in intervals:
            if isinstance(interval,str):
                date_list.append(interval)
            else:
                for date in interval:
                    date_list.append(date)
        intervals_iso = [ reformat_date(date) for date in date_list  ]
        if(labels is not None):
            labels_iso = list(map(lambda l:pd.to_datetime(l).strftime('%Y-%m-%dT%H:%M:%SZ'), labels))
        else:
            labels_iso = [ reformat_date(i[0]) for i in intervals]


        pysc = gps.get_spark_context()
        from openeogeotrellis.backend import SingleNodeUDFProcessGraphVisitor, GeoPySparkBackendImplementation
        if isinstance(reducer, dict):
            reducer = GeoPySparkBackendImplementation.accept_process_graph(reducer)

        if isinstance(reducer, str):
            #deprecated codepath: only single process reduces
            pysc = gps.get_spark_context()
            mapped_keys = self._apply_to_levels_geotrellis_rdd(
                lambda rdd,level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mapInstantToInterval(rdd,intervals_iso,labels_iso))
            reducer = self._normalize_temporal_reducer(dimension, reducer)
            return mapped_keys.apply_to_levels(lambda rdd: rdd.aggregate_by_cell(reducer))
        elif isinstance(reducer, GeotrellisTileProcessGraphVisitor):
            return self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().aggregateTemporal(rdd,
                                                                                                          intervals_iso,
                                                                                                          labels_iso,reducer.builder,context if isinstance(context,dict) else {}))
        else:
            raise FeatureUnsupportedException("Unsupported type of reducer in aggregate_temporal: " + str(reducer))


    @classmethod
    def _transform_metadata(cls, layer_or_metadata, cellType = None):
        layer = None
        if hasattr(layer_or_metadata,'layer_metadata'):
            layer=layer_or_metadata
            metadata=layer_or_metadata.layer_metadata
        else:
            metadata=layer_or_metadata

        output_metadata_dict = metadata.to_dict()
        if cellType != None:
            output_metadata_dict['cellType'] = CellType.FLOAT32
        metadata= Metadata.from_dict(output_metadata_dict)
        if layer is not None:
            layer.layer_metadata = metadata
            return layer
        else:
            return metadata

    def _aggregate_over_time_numpy(self, reducer: Callable[[Iterable[Tile]], Tile]) -> 'GeopysparkDataCube':
        """
        Aggregate over time.
        :param reducer: a function that reduces n Tiles to a single Tile
        :return:
        """
        def aggregate_temporally(layer):
            grouped_numpy_rdd = layer.to_spatial_layer().convert_data_type(CellType.FLOAT32).to_numpy_rdd().groupByKey()
            composite = grouped_numpy_rdd.mapValues(reducer)
            metadata = GeopysparkDataCube._transform_metadata(layer.layer_metadata, cellType=CellType.FLOAT32)
            return TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, composite, metadata)

        return self.apply_to_levels(aggregate_temporally)


    @classmethod
    def __reproject_polygon(cls, polygon: Union[Polygon, MultiPolygon], srs, dest_srs):
        from shapely.ops import transform
        from pyproj import Transformer
        return transform(Transformer.from_crs(srs, dest_srs, always_xy=True).transform, polygon)  # apply projection


    def merge_cubes(self, other: 'GeopysparkDataCube', overlaps_resolver:str=None):
        #we may need to align datacubes automatically?
        #other_pyramid_levels = {k: l.tile_to_layout(layout=self.pyramid.levels[k]) for k, l in other.pyramid.levels.items()}
        pysc = gps.get_spark_context()

        leftBandNames = []
        rightBandNames = []
        if self.metadata.has_band_dimension():
            leftBandNames = self.metadata.band_names
        else:
            leftBandNames = [ "left_band_unnamed"]

        if other.metadata.has_band_dimension():
            rightBandNames = other.metadata.band_names
        else:
            rightBandNames = [ "right_band_unnamed"]


        if other.pyramid.levels.keys() != self.pyramid.levels.keys():
            raise OpenEOApiException(message="Trying to merge two cubes with different levels, perhaps you had to use 'resample_cube_spatial'? Levels of this cube: " + str(self.pyramid.levels.keys()) +
                                             " are merged with %s" % str(other.pyramid.levels.keys()))

        # TODO properly combine bbox and temporal extents in metadata?
        pr = pysc._jvm.org.openeo.geotrellis.OpenEOProcesses()
        if self._is_spatial() and other._is_spatial():
            def merge(rdd,other,level):
                left = pr.wrapCube(rdd)
                left.openEOMetadata().setBandNames(leftBandNames)
                right = pr.wrapCube(other.pyramid.levels[level].srdd.rdd())
                right.openEOMetadata().setBandNames(rightBandNames)
                return pr.mergeSpatialCubes(
                    left,
                    right,
                    overlaps_resolver
                )
            merged_data = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level:merge(rdd,other,level)

            )
        elif self._is_spatial():
            merged_data = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level:
                pr.mergeCubes_SpaceTime_Spatial(
                    other.pyramid.levels[level].srdd.rdd(),
                    rdd,
                    overlaps_resolver,
                    True  # swapOperands
                )
            )
        elif other._is_spatial():
            merged_data = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level:
                pr.mergeCubes_SpaceTime_Spatial(
                    rdd,
                    other.pyramid.levels[level].srdd.rdd(),
                    overlaps_resolver,
                    False  # swapOperands
                )
            )
        else:
            def merge(rdd,other,level):
                left = pr.wrapCube(rdd)
                left.openEOMetadata().setBandNames(leftBandNames)
                right = pr.wrapCube(other.pyramid.levels[level].srdd.rdd())
                right.openEOMetadata().setBandNames(rightBandNames)
                return pr.mergeCubes(
                    left,
                    right,
                    overlaps_resolver
                )

            merged_data=self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level:merge(rdd,other,level)
            )

        if self.metadata.has_band_dimension() and other.metadata.has_band_dimension():
            for iband in other.metadata.bands:
                if iband.name not in merged_data.metadata.band_names:
                    merged_data.metadata=merged_data.metadata.append_band(iband)
        
        return merged_data

    # TODO legacy alias to be removed
    merge = merge_cubes

    def mask_polygon(self, mask: Union[Polygon, MultiPolygon], srs="EPSG:4326",
                     replacement=None, inside=False) -> 'GeopysparkDataCube':
        max_level = self.get_max_level()
        layer_crs = max_level.layer_metadata.crs
        reprojected_polygon = self.__reproject_polygon(mask, "+init=" + srs, layer_crs)
        # TODO should we warn when masking generates an empty collection?
        # TODO: use `replacement` and `inside`
        return self.apply_to_levels(lambda rdd: rdd.mask(
            reprojected_polygon,
            partition_strategy=None,
            options=gps.RasterizerOptions()
        ))

    def mask(self, mask: 'GeopysparkDataCube',
             replacement=None) -> 'GeopysparkDataCube':
        # mask needs to be the same layout as this layer
        mask_pyramid_levels = {
            k: l.tile_to_layout(layout=self.pyramid.levels[k])
            for k, l in mask.pyramid.levels.items()
        }
        replacement = float(replacement) if replacement is not None else None
        if self._is_spatial() and mask._is_spatial():
            rasterMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask_spatial_spatial
        elif mask._is_spatial():
            rasterMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask_spacetime_spatial
        else:
            rasterMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask

        return self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: rasterMask(rdd, mask_pyramid_levels[level].srdd.rdd(), replacement)
        )

    def apply_kernel(self, kernel: np.ndarray, factor=1, border=0, replace_invalid=0):
        # TODO: support border options and replace_invalid
        if border != 0:
            raise ProcessParameterInvalidException(
                parameter='border', process='apply_kernel',
                reason=f'This backend only supports border value 0, not {border!r}.'
            )
        if replace_invalid != 0:
            raise ProcessParameterInvalidException(
                parameter='replace_invalid', process='apply_kernel',
                reason=f'This backend only supports replace_invalid value 0 not {replace_invalid!r}.'
            )

        pysc = gps.get_spark_context()

        #converting a numpy array into a geotrellis tile seems non-trivial :-)
        kernel = factor * kernel.astype(np.float64)
        kernel_tile = Tile.from_numpy_array(kernel, no_data_value=None)
        rdd = pysc.parallelize([(gps.SpatialKey(0,0), kernel_tile)])
        metadata = {'cellType': str(kernel.dtype),
                    'extent': {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0},
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0},
                        'maxKey': {'col': 0, 'row': 0}},
                    'layoutDefinition': {
                        'extent': {'xmin': 0.0, 'ymin': 0.0, 'xmax': 1.0, 'ymax': 1.0},
                        'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 1, 'layoutRows': 1}}}
        geopyspark_layer = TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, rdd, metadata)
        geotrellis_tile = geopyspark_layer.srdd.rdd().collect()[0]._2().band(0)

        if self.pyramid.layer_type == gps.LayerType.SPACETIME:
            result_collection = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().apply_kernel_spacetime(rdd, geotrellis_tile))
        else:
            result_collection = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().apply_kernel_spatial(rdd,geotrellis_tile))
        return result_collection

    def apply_neighborhood(self, process: Dict, size: List, overlap: List, env: EvalEnv) -> 'GeopysparkDataCube':

        spatial_dims = self.metadata.spatial_dimensions
        if len(spatial_dims) != 2:
            raise OpenEOApiException(message="Unexpected spatial dimensions in apply_neighborhood,"
                                             " expecting exactly 2 spatial dimensions: %s" % str(spatial_dims))
        x = spatial_dims[0]
        y = spatial_dims[1]
        size_dict = {e['dimension']:e for e in size}
        overlap_dict = {e['dimension']: e for e in overlap}
        if size_dict.get(x.name, {}).get('unit', None) != 'px' or size_dict.get(y.name, {}).get('unit', None) != 'px':
            raise OpenEOApiException(message="apply_neighborhood: window sizes for the spatial dimensions"
                                             " of this datacube should be specified in pixels."
                                             " This was provided: %s" % str(size))
        sizeX = int(size_dict[x.name]['value'])
        sizeY = int(size_dict[y.name]['value'])
        if sizeX < 32 or sizeY < 32:
            raise OpenEOApiException(message="apply_neighborhood: window sizes smaller then 32 are not yet supported.")
        overlap_x = overlap_dict.get(x.name,{'value': 0, 'unit': 'px'})
        overlap_y = overlap_dict.get(y.name,{'value': 0, 'unit': 'px'})
        if overlap_x.get('unit', None) != 'px' or overlap_y.get('unit', None) != 'px':
            raise OpenEOApiException(message="apply_neighborhood: overlap sizes for the spatial dimensions"
                                             " of this datacube should be specified, in pixels."
                                             " This was provided: %s" % str(overlap))
        jvm = self._get_jvm()
        overlap_x_value = int(overlap_x['value'])
        overlap_y_value = int(overlap_y['value'])
        retiled_collection = self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: jvm.org.openeo.geotrellis.OpenEOProcesses().retile(rdd, sizeX, sizeY, overlap_x_value, overlap_y_value))

        from openeogeotrellis.backend import SingleNodeUDFProcessGraphVisitor, GeoPySparkBackendImplementation

        process = GeoPySparkBackendImplementation.accept_process_graph(process)
        temporal_size = temporal_overlap = None
        if self.metadata.has_temporal_dimension():
            temporal_size = size_dict.get(self.metadata.temporal_dimension.name,None)
            temporal_overlap = overlap_dict.get(self.metadata.temporal_dimension.name, None)

        result_collection = None
        if isinstance(process, SingleNodeUDFProcessGraphVisitor):
            udf = process.udf_args.get('udf', None)
            context = process.udf_args.get('context', {})
            # TODO Putting this import at toplevel breaks things at the moment (circular import issues)
            from openeo_driver.ProcessGraphDeserializer import convert_node
            # Resolve "from_parameter" references in context object
            context = convert_node(context, env=env)
            if not isinstance(udf, str):
                raise ValueError(
                    "The 'run_udf' process requires at least a 'udf' string argument, but got: '%s'." % udf)
            if temporal_size is None or temporal_size.get('value',None) is None:
                #full time dimension has to be provided
                if not self.metadata.has_temporal_dimension():
                    raise OpenEOApiException(
                        message="apply_neighborhood: datacubes without a time dimension are not yet supported for this case")
                result_collection = retiled_collection.apply_tiles_spatiotemporal(udf,context=context)
            elif temporal_size.get('value',None) == 'P1D' and temporal_overlap is None:
                result_collection = retiled_collection.apply_tiles(udf,context=context)
            else:
                raise OpenEOApiException(
                    message="apply_neighborhood: for temporal dimension,"
                            " either process all values, or only single date is supported for now!")

        elif isinstance(process, GeotrellisTileProcessGraphVisitor):
            if temporal_size is None or temporal_size.get('value', None) is None:
                raise OpenEOApiException(message="apply_neighborhood: only supporting complex callbacks on bands")
            elif temporal_size.get('value', None) == 'P1D' and temporal_overlap is None:
                result_collection = self._apply_bands_dimension(process)
            else:
                raise OpenEOApiException(message="apply_neighborhood: only supporting complex callbacks on bands")
        else:
            raise OpenEOApiException(message="apply_neighborhood: only supporting callbacks with a single UDF.")

        if overlap_x_value > 0 or overlap_y_value > 0:

            result_collection = result_collection._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: jvm.org.openeo.geotrellis.OpenEOProcesses().remove_overlap(rdd, sizeX, sizeY,
                                                                                      overlap_x_value,
                                                                                      overlap_y_value))

        return result_collection

    def resample_cube_spatial(self, target: 'GeopysparkDataCube', method: str = 'near') -> 'GeopysparkDataCube':
        """
        Resamples the spatial dimensions (x,y) of this data cube to a target data cube and return the results as a new data cube.

        https://processes.openeo.org/#resample_cube_spatial

        :param target: An data cube that specifies the target
        :param method: The resampling method.
        :return: A raster data cube with values warped onto the new projection.

        """
        resample_method = ResampleMethod(self._get_resample_method(method))
        if len(self.pyramid.levels)!=1 or len(target.pyramid.levels)!=1:
            raise FeatureUnsupportedException(message='This backend does not support resampling between full '
                                                      'pyramids, for instance used by viewing services. Batch jobs '
                                                      'should work.')
        max_level:TiledRasterLayer = self.get_max_level()
        target_max_level:TiledRasterLayer = target.pyramid.levels[target.pyramid.max_zoom]
        if self.pyramid.layer_type == gps.LayerType.SPACETIME and target.pyramid.layer_type == gps.LayerType.SPACETIME:
            level_rdd_tuple = self._get_jvm().org.openeo.geotrellis.OpenEOProcesses().resampleCubeSpatial(max_level.srdd.rdd(),target_max_level.srdd.rdd(),resample_method)
        elif self.pyramid.layer_type == gps.LayerType.SPATIAL:
            partitioner = target_max_level.srdd.rdd().partitioner()
            if target.pyramid.layer_type == gps.LayerType.SPACETIME or partitioner.isEmpty():
                partitioner = max_level.srdd.rdd().partitioner()
            if(partitioner.isEmpty()):
                partitioner=None
            else:
                partitioner=partitioner.get()
            layout = target_max_level.srdd.rdd().metadata().layout()
            crs = target_max_level.srdd.rdd().metadata().crs()
            level_rdd_tuple = self._get_jvm().org.openeo.geotrellis.OpenEOProcesses().resampleCubeSpatial_spatial(
                max_level.srdd.rdd(), crs, layout, resample_method, partitioner)
        else:
            raise FeatureUnsupportedException(message='resample_cube_spatial - Unsupported combination of two cubes of type: ' + str(self.pyramid.layer_type) + ' and ' + str(target.pyramid.layer_type))

        layer = self._create_tilelayer(level_rdd_tuple._2(),max_level.layer_type,target.pyramid.max_zoom)
        pyramid = Pyramid({target.pyramid.max_zoom:layer})
        return GeopysparkDataCube(pyramid=pyramid, metadata=self.metadata)




    def resample_spatial(
            self,
            resolution: Union[float, Tuple[float, float]],
            projection: Union[int, str] = None,
            method: str = 'near',
            align: str = 'upper-left'
    ):
        """
        https://open-eo.github.io/openeo-api/v/0.4.0/processreference/#resample_spatial
        :param resolution:
        :param projection:
        :param method:
        :param align:
        :return:
        """

        # TODO: use align

        resample_method = self._get_resample_method(method)
        max_level = self.get_max_level()
        current_crs_proj4 = max_level.layer_metadata.crs
        logging.info(f"Reprojecting datacube with crs {current_crs_proj4} and layout {max_level.layer_metadata.layout_definition} to {projection} and {resolution}")
        if projection is not None and crs_to_proj4(projection) == current_crs_proj4:
            projection = None

        #IF projection is defined, we need to warp
        if projection is not None and resolution==0.0:
            reprojected = self.apply_to_levels(lambda layer: gps.TiledRasterLayer(
                layer.layer_type, layer.srdd.reproject(str(projection), resample_method, None)
            ))
            return reprojected
        elif resolution != 0.0:

            extent = max_level.layer_metadata.layout_definition.extent
            currentTileLayout: gps.TileLayout = max_level.layer_metadata.tile_layout
            currentTileCols = currentTileLayout.tileCols
            currentTileRows = currentTileLayout.tileRows

            if projection is not None:
                extent = self._reproject_extent(
                    max_level.layer_metadata.crs, projection, extent.xmin, extent.ymin, extent.xmax, extent.ymax
                )

            width = extent.xmax - extent.xmin
            height = extent.ymax - extent.ymin

            currentResolutionX = width / (currentTileCols * currentTileLayout.layoutCols)
            currentResolutionY = width / (currentTileRows * currentTileLayout.layoutRows)
            if projection == None and currentResolutionX == resolution:
                return self


            nbTilesX = width / (currentTileCols * resolution)
            nbTilesY = height / (currentTileRows * resolution)

            exactTileSizeX = width/(resolution * math.ceil(nbTilesX))
            exactNbTilesX = width/(resolution * exactTileSizeX)

            exactTileSizeY = height / (resolution * math.ceil(nbTilesY))
            exactNbTilesY = height / (resolution * exactTileSizeY)


            newLayout = gps.LayoutDefinition(extent=extent,tileLayout=gps.TileLayout(int(exactNbTilesX),int(exactNbTilesY),int(exactTileSizeX),int(exactTileSizeY)))

            if(projection is not None):
                resampled = max_level.tile_to_layout(newLayout,target_crs=projection, resample_method=resample_method)
            else:
                resampled = max_level.tile_to_layout(newLayout,resample_method=resample_method)

            pyramid = Pyramid({0: resampled})
            return GeopysparkDataCube(pyramid=pyramid, metadata=self.metadata)
            #return self.apply_to_levels(lambda layer: layer.tile_to_layout(projection, resample_method))
        return self

    def _get_resample_method(self, method):
        resample_method = {
            'bilinear': gps.ResampleMethod.BILINEAR,
            'average': gps.ResampleMethod.AVERAGE,
            'cubic': gps.ResampleMethod.CUBIC_CONVOLUTION,
            'cubicspline': gps.ResampleMethod.CUBIC_SPLINE,
            'lanczos': gps.ResampleMethod.LANCZOS,
            'mode': gps.ResampleMethod.MODE,
            'max': gps.ResampleMethod.MAX,
            'min': gps.ResampleMethod.MIN,
            'med': gps.ResampleMethod.MEDIAN,
        }.get(method, gps.ResampleMethod.NEAREST_NEIGHBOR)
        return resample_method

    def linear_scale_range(self, input_min, input_max, output_min, output_max) -> 'GeopysparkDataCube':
        """ Color stretching
            :param input_min: Minimum input value
            :param input_max: Maximum input value
            :param output_min: Minimum output value
            :param output_max: Maximum output value
            :return A data cube
        """
        rescaled = self.apply_to_levels(lambda layer: layer.normalize(output_min, output_max, input_min, input_max))
        output_range = output_max - output_min
        if output_range >1 and type(output_min) == int and type(output_max) == int:
            if output_range < 254 and output_min >= 0:
                rescaled = rescaled.apply_to_levels(lambda layer: layer.convert_data_type(gps.CellType.UINT8,255))
            elif output_range < 65535 and output_min >= 0:
                rescaled = rescaled.apply_to_levels(lambda layer: layer.convert_data_type(gps.CellType.UINT16))
        return rescaled

    def timeseries(self, x, y, srs="EPSG:4326") -> Dict:
        max_level = self.get_max_level()
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

    def raster_to_vector(self):
        """
        Outputs polygons, where polygons are formed from homogeneous zones of four-connected neighbors
        @return:
        """
        max_level = self.get_max_level()
        with tempfile.NamedTemporaryFile(suffix=".json.tmp",delete=False) as temp_file:
            gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().vectorize(max_level.srdd.rdd(),temp_file.name)
            #postpone turning into an actual collection upon usage
            return DelayedVector(temp_file.name)

    def get_max_level(self):
        return self.pyramid.levels[self.pyramid.max_zoom]

    def aggregate_spatial(self, geometries: Union[str, BaseGeometry], reducer,
                          target_dimension: str = "result") -> Union[AggregatePolygonResult,
                                                                     AggregateSpatialVectorCube]:

        if isinstance(reducer, dict):
            if len(reducer) == 1:
                single_process = next(iter(reducer.values())).get('process_id')
                return self.zonal_statistics(geometries,single_process)
            else:
                visitor = GeotrellisTileProcessGraphVisitor(_builder=self._get_jvm().org.openeo.geotrellis.aggregate_polygon.SparkAggregateScriptBuilder()).accept_process_graph(reducer)
                return self.zonal_statistics(geometries, visitor.builder)

        raise OpenEOApiException(
                message=f"Reducer {reducer} is not supported in aggregate_spatial",
                code="ReducerUnsupported", status_code=400
            )

    def zonal_statistics(self, regions: Union[str, BaseGeometry], func) -> Union[AggregatePolygonResult,
                                                                                 AggregateSpatialVectorCube]:
        # TODO: rename to aggregate_spatial?
        # TODO eliminate code duplication
        _log.info("zonal_statistics with {f!r}, {r}".format(f=func, r=type(regions)))

        def insert_timezone(instant):
            return instant.replace(tzinfo=pytz.UTC) if instant.tzinfo is None else instant

        def csv_dir() -> str:
            clusterDir = pathlib.Path("/data/projects/OpenEO/timeseries")

            if (not clusterDir.exists()):
                clusterDir = pathlib.Path(".").resolve()
            temp_output = tempfile.mkdtemp(prefix="timeseries_", suffix="_csv", dir=clusterDir)
            os.chmod(temp_output, 0o777)

            return temp_output

        if isinstance(regions, (Polygon, MultiPolygon)):
            regions = GeometryCollection([regions])

        polygons = (None if isinstance(regions, Point) or
                            (isinstance(regions, GeometryCollection) and
                             any(isinstance(geom, Point) for geom in regions.geoms))
                    else to_projected_polygons(self._get_jvm(), regions))

        highest_level = self.get_max_level()
        scala_data_cube = highest_level.srdd.rdd()
        layer_metadata = highest_level.layer_metadata

        bandNames = ["band_unnamed"]
        if self.metadata.has_band_dimension():
            bandNames = self.metadata.band_names

        wrapped = self._get_jvm().org.openeo.geotrellis.OpenEOProcesses().wrapCube(scala_data_cube)
        wrapped.openEOMetadata().setBandNames(bandNames)

        if self._is_spatial():
            geometry_wkts = [str(regions)] if isinstance(regions, Point) else [str(geom) for geom in regions.geoms]
            geometries_srs = "EPSG:4326"

            temp_dir = csv_dir()

            self._compute_stats_geotrellis().compute_generic_timeseries_from_spatial_datacube(
                func,
                wrapped,
                geometry_wkts,
                geometries_srs,
                temp_dir
            )

            return AggregateSpatialVectorCube(temp_dir, regions=regions, metadata=self.metadata)
        else:
            from_date = insert_timezone(layer_metadata.bounds.minKey.instant)
            to_date = insert_timezone(layer_metadata.bounds.maxKey.instant)

            if polygons:
                # TODO also add dumping results first to temp json file like with "mean"
                if func == 'histogram':
                    stats = self._compute_stats_geotrellis().compute_histograms_time_series_from_datacube(
                        scala_data_cube, polygons, from_date.isoformat(), to_date.isoformat(), 0
                    )
                    timeseries = self._as_python(stats)
                elif func ==  "mean":
                    with tempfile.NamedTemporaryFile(suffix=".json.tmp") as temp_file:
                        self._compute_stats_geotrellis().compute_average_timeseries_from_datacube(
                            scala_data_cube,
                            polygons,
                            from_date.isoformat(),
                            to_date.isoformat(),
                            0,
                            temp_file.name
                        )
                        with open(temp_file.name, encoding='utf-8') as f:
                            timeseries = json.load(f)
                else:
                    temp_output = csv_dir()

                    self._compute_stats_geotrellis().compute_generic_timeseries_from_datacube(
                        func,
                        wrapped,
                        polygons,
                        temp_output
                    )
                    return AggregatePolygonResultCSV(temp_output, regions=regions, metadata=self.metadata)
            else:
                geometry_wkts = [str(regions)] if isinstance(regions, Point) else [str(geom) for geom in regions.geoms]
                geometries_srs = "EPSG:4326"

                temp_dir = csv_dir()

                self._compute_stats_geotrellis().compute_generic_timeseries_from_datacube(
                    func,
                    wrapped,
                    geometry_wkts,
                    geometries_srs,
                    temp_dir
                )

                return AggregatePolygonResultCSV(temp_dir, regions=regions, metadata=self.metadata)

            return AggregatePolygonResult(
                timeseries=timeseries,
                # TODO: regions can also be a string (path to vector file) instead of geometry object
                regions=regions,
                metadata=self.metadata
            )

    def _compute_stats_geotrellis(self):
        accumulo_instance_name = 'hdp-accumulo-instance'
        return self._get_jvm().org.openeo.geotrellis.ComputeStatsGeotrellisAdapter(self._zookeepers(), accumulo_instance_name)

    def _zookeepers(self):
        return ','.join(ConfigParams().zookeepernodes)

    # FIXME: define this somewhere else?
    def _as_python(self, java_object):
        """
        Converts Java collection objects retrieved from Py4J to their Python counterparts, recursively.
        :param java_object: a JavaList or JavaMap
        :return: a Python list or dictionary, respectively
        """

        from py4j.java_collections import JavaList, JavaMap

        if isinstance(java_object, JavaList):
            return [self._as_python(elem) for elem in list(java_object)]

        if isinstance(java_object, JavaMap):
            return {self._as_python(key): self._as_python(value) for key, value in dict(java_object).items()}

        return java_object

    def _to_xarray(self):
        spatial_rdd = self.get_max_level()
        return self._collect_as_xarray(spatial_rdd)

    def save_result(self, filename: Union[str, pathlib.Path], format: str, format_options: dict = None) -> str:
        result = self.write_assets(filename, format, format_options)
        return result.popitem()[1]['href']

    def write_assets(self, filename: Union[str, pathlib.Path], format: str, format_options: dict = None) -> Dict:
        """
        Save cube to disk

        Supported formats:
        * GeoTIFF: raster with the limitation that it only export bands at a single (random) date
        * PNG: 8-bit grayscale or RGB raster with the limitation that it only export bands at a single (random) date
        * NetCDF: raster, currently using h5NetCDF
        * JSON: the json serialization of the underlying xarray, with extra attributes such as value/coord dtypes, crs, nodata value

        :return: STAC assets dictionary: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#assets
        """
        filename = str(filename)
        directory = str(pathlib.Path(filename).parent)
        s3_filename = "s3://OpenEO-data{}".format(filename)
        s3_directory = "s3://OpenEO-data{}".format(directory)
        format = format.upper()
        format_options = format_options or {}
        _log.info("save_result format {f} with options {o}".format(f=format, o=format_options))
        strict_cropping = format_options.get("strict_cropping", True)
        #geotiffs = self.rdd.merge().to_geotiff_rdd(compression=gps.Compression.DEFLATE_COMPRESSION).collect()

        # get the data at highest resolution
        max_level = self.get_max_level()

        def to_latlng_bbox(bbox: 'Extent') -> Tuple[float, float, float, float]:
            latlng_extent = self._reproject_extent(src_crs=max_level.layer_metadata.crs,
                                                   dst_crs="+init=EPSG:4326",
                                                   xmin=bbox.xmin(), ymin=bbox.ymin(),
                                                   xmax=bbox.xmax(), ymax=bbox.ymax())

            return latlng_extent.xmin, latlng_extent.ymin, latlng_extent.xmax, latlng_extent.ymax

        if self.metadata.spatial_extent and strict_cropping:
            bbox = self.metadata.spatial_extent
            crs = bbox.get("crs") or "EPSG:4326"
            if isinstance(crs, int):
                crs = "EPSG:%d" % crs
            crop_bounds = self._reproject_extent(
                src_crs="+init=" + crs, dst_crs=max_level.layer_metadata.crs,
                xmin=bbox["west"], ymin=bbox["south"], xmax=bbox["east"], ymax=bbox["north"]
            )
        else:
            crop_bounds = None

        if self.metadata.temporal_extent:
            date_from, date_to = self.metadata.temporal_extent
            crop_dates = (pd.Timestamp(date_from), pd.Timestamp(date_to))
        else:
            crop_dates = None

        tiled = format_options.get("tiled", False)
        stitch = format_options.get("stitch", False)
        catalog = format_options.get("parameters", {}).get("catalog", False)
        tile_grid = format_options.get("tile_grid", "")
        sample_by_feature = format_options.get("sample_by_feature", False)
        feature_id_property = format_options.get("feature_id_property", False)
        batch_mode = format_options.get("batch_mode", False)
        overviews = format_options.get("overviews", "OFF")
        colormap = format_options.get("colormap", None)

        save_filename = s3_filename if batch_mode and ConfigParams().is_kube_deploy else filename
        save_directory = s3_directory if batch_mode and ConfigParams().is_kube_deploy else directory

        if format in ["GTIFF", "PNG"]:
            if max_level.layer_type != gps.LayerType.SPATIAL and (not batch_mode or catalog or stitch or format=="PNG") :
                max_level = max_level.to_spatial_layer()

            if format == "GTIFF":
                zlevel = format_options.get("ZLEVEL",6)
                if catalog:
                    _log.info("save_result (catalog) save_on_executors")
                    self._save_on_executors(max_level, filename)
                elif stitch:
                    if tile_grid:
                        _log.info("save_result save_stitched_tile_grid")
                        filenames = self._save_stitched_tile_grid(max_level, save_filename, tile_grid, crop_bounds, zlevel=zlevel)
                        return {str(pathlib.Path(filename).name): {"href": filename} for filename in filenames}
                    else:
                        _log.info("save_result save_stitched")
                        bbox = self._save_stitched(max_level, save_filename, crop_bounds, zlevel=zlevel)
                        return {str(pathlib.Path(filename).name): {
                            "href": save_filename,
                            "bbox": to_latlng_bbox(bbox)
                        }}
                else:
                    _log.info("save_result: saveRDD")
                    gtiff_options = self._get_jvm().org.openeo.geotrellis.geotiff.GTiffOptions()
                    gtiff_options.addHeadTag("PROCESSING_SOFTWARE",softwareversion)
                    getattr(gtiff_options, "overviews_$eq")(overviews)
                    if( colormap is not None):
                        def color_to_int(color):
                            if isinstance(color,int):
                                return color
                            elif isinstance(color,list):
                                import struct
                                import builtins

                                color_as_int = struct.unpack('>L',
                                              bytes(map(lambda x: builtins.int(x * 255), color)))[0]
                                return color_as_int

                        converted_colors = { float(k):color_to_int(v) for k,v in colormap.items()}
                        gpsColormap = gps.ColorMap.build(breaks=converted_colors)
                        gtiff_options.setColorMap(gpsColormap.cmap)
                    band_count = -1
                    if self.metadata.has_band_dimension():
                        band_count = len(self.metadata.band_dimension.band_names)
                        for index, band_name in enumerate(self.metadata.band_dimension.band_names):
                            gtiff_options.addBandTag(index, "DESCRIPTION", band_name)
                    if crop_bounds:
                        crop_extent = self._get_jvm().geotrellis.vector.Extent(crop_bounds.xmin,crop_bounds.ymin,crop_bounds.xmax,crop_bounds.ymax)
                    else:
                        crop_extent = None
                    bands = []
                    if self.metadata.has_band_dimension():
                        bands = [b._asdict() for b in self.metadata.bands]
                    nodata = max_level.layer_metadata.no_data_value

                    if batch_mode and max_level.layer_type != gps.LayerType.SPATIAL:
                        compression = self._get_jvm().geotrellis.raster.io.geotiff.compression.DeflateCompression(
                            zlevel)

                        if tile_grid:
                            timestamped_paths = (self._get_jvm()
                                .org.openeo.geotrellis.geotiff.package.saveStitchedTileGridTemporal(
                                max_level.srdd.rdd(), save_directory, tile_grid, compression))
                        elif sample_by_feature:
                            #EP-3874 user requests to output data by polygon
                            _log.info("Output one tiff file per feature and timestamp.")
                            geometries = format_options['geometries']
                            if isinstance(geometries, MultiPolygon):
                                geometries = GeometryCollection(geometries.geoms)
                            projected_polygons = to_projected_polygons(self._get_jvm(), geometries)
                            labels = self.get_labels(geometries)
                            timestamped_paths = self._get_jvm().org.openeo.geotrellis.geotiff.package.saveSamples(
                                max_level.srdd.rdd(), save_directory, projected_polygons, labels, compression)
                        else:
                            timestamped_paths = self._get_jvm().org.openeo.geotrellis.geotiff.package.saveRDDTemporal(
                                max_level.srdd.rdd(), save_directory, zlevel, self._get_jvm().scala.Option.apply(crop_extent),
                                gtiff_options)

                        assets = {}

                        # noinspection PyProtectedMember
                        timestamped_paths = [(pathlib.Path(timestamped_path._1()), timestamped_path._2(), timestamped_path._3())
                                             for timestamped_path in timestamped_paths]

                        for path, timestamp, bbox in timestamped_paths:
                            assets[path.name] = {
                                "href": str(path),
                                "type": "image/tiff; application=geotiff",
                                "roles": ["data"],
                                'bands': bands,
                                'nodata': nodata,
                                'datetime': timestamp,
                                'bbox': to_latlng_bbox(bbox)
                            }
                        return assets
                    else:
                        if tile_grid:
                            filenames = self._save_stitched_tile_grid(max_level, filename, tile_grid, crop_bounds,
                                                                      zlevel=zlevel)
                            return {str(pathlib.Path(filename).name): {"href": filename,
                                                                       "type": "image/tiff; application=geotiff",
                                                                       "roles": ["data"]} for filename in filenames}
                        else:
                            originalName = pathlib.Path(filename)

                            filePath = originalName.parent / ("openEO.tif" if originalName.name == "out" else originalName.name)
                            self._get_jvm().org.openeo.geotrellis.geotiff.package.saveRDD(max_level.srdd.rdd(),band_count,str(filePath),zlevel,self._get_jvm().scala.Option.apply(crop_extent),gtiff_options)
                            return {
                                str(filePath.name): {
                                    "href": str(filePath),
                                    "type": "image/tiff; application=geotiff",
                                    "roles": ["data"],
                                    'bands': bands,
                                    'nodata': nodata,
                                    'datetime': None
                                }
                            }
            else:
                if not filename.endswith(".png"):
                    filename = filename + ".png"
                if crop_bounds:
                    crop_extent = self._get_jvm().geotrellis.vector.Extent(crop_bounds.xmin, crop_bounds.ymin, crop_bounds.xmax, crop_bounds.ymax)
                    self._get_jvm().org.openeo.geotrellis.png.package.saveStitched(max_level.srdd.rdd(), save_filename, crop_extent)
                else:
                    self._get_jvm().org.openeo.geotrellis.png.package.saveStitched(max_level.srdd.rdd(), save_filename)
                return {
                    str(pathlib.Path(filename).name): {
                        "href": filename,
                        "type": "image/png",
                        "roles": ["data"]
                    }
                }

        elif format == "NETCDF":
            band_names = ["var"]
            bands = None
            dim_names = {}
            if self.metadata.has_band_dimension():
                bands = [dict_no_none(**b._asdict()) for b in self.metadata.bands]
                band_names = self.metadata.band_names
                dim_names['bands'] = self.metadata.band_dimension.name
            if self.metadata.has_temporal_dimension():
                dim_names['t'] = self.metadata.temporal_dimension.name
            nodata = max_level.layer_metadata.no_data_value
            global_metadata = format_options.get("file_metadata",{})
            zlevel = format_options.get("ZLEVEL", 6)

            if batch_mode and sample_by_feature:
                _log.info("Output one netCDF file per feature.")
                geometries = format_options['geometries']
                if isinstance(geometries, MultiPolygon):
                    geometries = GeometryCollection(geometries.geoms)
                projected_polygons = to_projected_polygons(self._get_jvm(), geometries)
                labels = self.get_labels(geometries)
                if(max_level.layer_type != gps.LayerType.SPATIAL):
                    print(f"projected_polygons carries {len(projected_polygons.polygons())} polygons")
                    asset_paths = self._get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSamples(
                        max_level.srdd.rdd(),
                        save_directory,
                        projected_polygons,
                        labels,
                        band_names,
                        dim_names,
                        global_metadata)
                else:
                    asset_paths = self._get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSamplesSpatial(
                        max_level.srdd.rdd(),
                        save_directory,
                        projected_polygons,
                        labels,
                        band_names,
                        dim_names,
                        global_metadata)

                return self.return_netcdf_assets(asset_paths, bands, nodata)
            else:
                if not stitch:
                    originalName = pathlib.Path(filename)
                    filename = save_directory + "/" + ("openEO.nc" if originalName.name == "out" else originalName.name)
                    if(max_level.layer_type != gps.LayerType.SPATIAL):
                        asset_paths = self._get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSingleNetCDF(max_level.srdd.rdd(),
                            filename,
                            band_names,
                            dim_names,global_metadata,zlevel
                        )
                    else:
                        asset_paths = self._get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSingleNetCDFSpatial(
                            max_level.srdd.rdd(),
                            filename,
                            band_names,
                            dim_names, global_metadata, zlevel
                            )
                    return self.return_netcdf_assets(asset_paths, bands, nodata)

                else:
                    if not tiled:
                        result=self._collect_as_xarray(max_level, crop_bounds, crop_dates)
                    else:
                        result=self._collect_as_xarray(max_level)


                    if batch_mode:
                        filename = directory +  "/openEO.nc"
                    XarrayIO.to_netcdf_file(array=result, path=filename)
                    if batch_mode:
                        asset = {
                            "href": filename,
                            "roles": ["data"],
                            "type": "application/x-netcdf"
                        }
                        if bands is not None:
                            asset["bands"] = bands
                        return {"openEO.nc": asset}

        elif format == "JSON":
            # saving to json, this is potentially big in memory
            # get result as xarray
            if not tiled:
                result=self._collect_as_xarray(max_level, crop_bounds, crop_dates)
            else:
                result=self._collect_as_xarray(max_level)
                
            XarrayIO.to_json_file(array=result, path=filename)

        else:
            raise OpenEOApiException(
                message="Format {f!r} is not supported".format(f=format),
                code="FormatUnsupported", status_code=400
            )
        return {str(pathlib.Path(filename).name):{"href":filename}}

    def return_netcdf_assets(self, asset_paths, bands, nodata):
        asset_paths = [pathlib.Path(asset_paths.get(i)) for i in range(len(asset_paths))]
        assets = {}
        for p in asset_paths:
            assets[p.name] = {
                "href": str(p),
                "type": "application/x-netcdf",
                "roles": ["data"],
                "nodata": nodata,
            }
            if bands is not None:
                assets[p.name]["bands"] = bands
        return assets

    def get_labels(self, geometries):
        if isinstance(geometries,DelayedVector):
            geometries = list(geometries.geometries)

        if isinstance(geometries, collections.abc.Sized):
            return [str(x) for x in range(len(geometries))]
        else:
            return ["0"]


    def _collect_as_xarray(self, rdd, crop_bounds=None, crop_dates=None):
            
        # windows/dims are tuples of (xmin/mincol,ymin/minrow,width/cols,height/rows)
        layout_pix=rdd.layer_metadata.layout_definition.tileLayout
        layout_win=(0, 0, layout_pix.layoutCols*layout_pix.tileCols, layout_pix.layoutRows*layout_pix.tileRows)
        layout_extent=rdd.layer_metadata.layout_definition.extent
        layout_dim=(layout_extent.xmin, layout_extent.ymin, layout_extent.xmax-layout_extent.xmin, layout_extent.ymax-layout_extent.ymin)
        xres=layout_dim[2]/layout_win[2]
        yres=layout_dim[3]/layout_win[3]
        if crop_bounds:
            xmin=math.floor((crop_bounds.xmin-layout_extent.xmin)/xres)
            ymin=math.floor((crop_bounds.ymin-layout_extent.ymin)/yres)
            xmax= math.ceil((crop_bounds.xmax-layout_extent.xmin)/xres)
            ymax= math.ceil((crop_bounds.ymax-layout_extent.ymin)/yres)
            crop_win=(xmin, ymin, xmax-xmin, ymax-ymin)
        else:
            xmin=rdd.layer_metadata.bounds.minKey.col
            xmax=rdd.layer_metadata.bounds.maxKey.col+1
            ymin=rdd.layer_metadata.bounds.minKey.row
            ymax=rdd.layer_metadata.bounds.maxKey.row+1
            crop_win=(xmin*layout_pix.tileCols, ymin*layout_pix.tileRows, (xmax-xmin)*layout_pix.tileCols, (ymax-ymin)*layout_pix.tileRows)
        crop_dim=(layout_dim[0]+crop_win[0]*xres, layout_dim[1]+crop_win[1]*yres, crop_win[2]*xres, crop_win[3]*yres)
            

        # build metadata for the xarrays
        # coordinates are in the order of t,bands,x,y
        dims=[]
        coords={}
        has_time=self.metadata.has_temporal_dimension()
        if has_time:
            dims.append('t')
        has_bands=self.metadata.has_band_dimension()
        if has_bands:
            dims.append('bands')
            coords['bands']=self.metadata.band_names
        dims.append('x')
        coords['x']=np.linspace(crop_dim[0]+0.5*xres, crop_dim[0]+crop_dim[2]-0.5*xres, crop_win[2])
        dims.append('y')
        coords['y']=np.linspace(crop_dim[1]+0.5*yres, crop_dim[1]+crop_dim[3]-0.5*yres, crop_win[3])
        
        def stitch_at_time(crop_win, layout_win, tiles):
            
            # value expected to be another tuple with the original spacetime key and the array
            subarrs=list(tiles)
                        
            # get block sizes
            bw,bh=subarrs[0][1].cells.shape[-2:]
            bbands=sum(subarrs[0][1].cells.shape[:-2]) if len(subarrs[0][1].cells.shape)>2 else 1
            wbind=np.arange(0,bbands)
            dtype=subarrs[0][1].cells.dtype
            nodata=subarrs[0][1].no_data_value
            
            # allocate collector ndarray
            if nodata:
                window=np.full((bbands,crop_win[2],crop_win[3]), nodata, dtype)
            else:
                window=np.empty((bbands,crop_win[2],crop_win[3]), dtype)
            wxind=np.arange(crop_win[0],crop_win[0]+crop_win[2])
            wyind=np.arange(crop_win[1],crop_win[1]+crop_win[3])

            # override classic bottom-left corner coord system to top-left
            # note that possible key types are SpatialKey and SpaceTimeKey, but since at this level only col/row is used, casting down to SpatialKey
            switch_topleft=True
            tp=(0,1,2)
            if switch_topleft:
                nyblk=int(layout_win[3]/bh)-1
                subarrs=list(map(
                    lambda t: ( SpatialKey(t[0].col,nyblk-t[0].row), t[1] ),
                    subarrs
                ))
                tp=(0,2,1)
            
            # loop over blocks and merge into
            for iblk in subarrs:
                iwin=(iblk[0].col*bw, iblk[0].row*bh, bw, bh)
                iarr=iblk[1].cells
                iarr=iarr.reshape((-1,bh,bw)).transpose(tp)
                ixind=np.arange(iwin[0],iwin[0]+iwin[2])
                iyind=np.arange(iwin[1],iwin[1]+iwin[3])
                if switch_topleft:
                    iyind=iyind[::-1]
                xoverlap= np.intersect1d(wxind,ixind,True,True)
                yoverlap= np.intersect1d(wyind,iyind,True,True)
                if len(xoverlap[1])>0 and len(yoverlap[1]>0):
                    window[np.ix_(wbind,xoverlap[1],yoverlap[1])]=iarr[np.ix_(wbind,xoverlap[2],yoverlap[2])]
                    
            # return date (or None) - window tuple
            return window

        # at every date stitch together the layer, still on the workers   
        #mapped=list(map(lambda t: (t[0].row,t[0].col),rdd.to_numpy_rdd().collect())); min(mapped); max(mapped)
        collection=rdd\
            .to_numpy_rdd()\
            .map(lambda t: (t[0].instant if has_time else None, (t[0], t[1])))\
            .groupByKey()\
            .mapValues(partial(stitch_at_time, crop_win, layout_win))\
            .collect()
            
# only for debugging on driver, do not use in production
#         collection=rdd\
#             .to_numpy_rdd()\
#             .filter(lambda t: (t[0].instant>=crop_dates[0] and t[0].instant<=crop_dates[1]) if has_time else True)\
#             .map(lambda t: (t[0].instant if has_time else None, (t[0], t[1])))\
#             .groupByKey()\
#             .collect()
#         collection=list(map(partial(stitch_at_time, crop_win, layout_win),collection))
        
        if len(collection)==0:
            return xr.DataArray(np.full([0]*len(dims),0),dims=dims,coords=dict(map(lambda k: (k[0],[]),coords.items())))
        
        if len(collection)>1:
            collection.sort(key= lambda i: i[0])
                        
        if not has_bands:
            #no bands defined, so we need to force the data to only have 2 dimensions
            collection=list(map(lambda i: (i[0],i[1][0] if len(i[1].shape)>2 else i[1]), collection))

        # collect to an xarray
        if has_time:
            collection=list(zip(*collection))
            coords['t']=list(map(lambda i: np.datetime64(i),collection[0]))
            npresult=np.stack(collection[1])
            # TODO: this is a workaround if metadata goes out of sync, fix upstream process nodes to update metdata
            if has_bands and len(coords['bands'])!=npresult.shape[-3]:
                coords['bands']=[ 'band_'+str(i) for i in range(npresult.shape[-3])]
            result=xr.DataArray(npresult,dims=dims,coords=coords)
        else:
            # TODO error if len > 1
            result=xr.DataArray(collection[0][1],dims=dims,coords=coords)
            
        # add some metadata
        result=result.assign_attrs(dict(
            # TODO: layer_metadata is always 255, regardless of dtype, only correct inside the rdd-s
            nodata=rdd.layer_metadata.no_data_value,
            # TODO: crs seems to be recognized when saving to netcdf and loading with gdalinfo/qgis, but yet projection is incorrect https://github.com/pydata/xarray/issues/2288
            crs=rdd.layer_metadata.crs
        ))

        projCRS = pyproj.CRS.from_proj4(rdd.layer_metadata.crs)
        # Some things we need to do to make GDAL
        # and other software recognize the CRS
        # cfr: https://github.com/pydata/xarray/issues/2288
        result.coords['spatial_ref'] = 0
        result.coords['spatial_ref'].attrs['spatial_ref'] = projCRS.to_wkt()
        result.coords['spatial_ref'].attrs['crs_wkt'] = projCRS.to_wkt()
        result.attrs['grid_mapping'] = 'spatial_ref'
        
        return result

        
    @classmethod
    def _reproject_extent(cls, src_crs, dst_crs, xmin, ymin, xmax, ymax):
        src_proj = pyproj.Proj(src_crs)
        dst_proj = pyproj.Proj(dst_crs)

        def reproject_point(x, y):
            return pyproj.transform(
                src_proj,
                dst_proj,
                x, y,
                always_xy=True
            )

        reprojected_xmin1, reprojected_ymin1 = reproject_point(xmin, ymin)
        reprojected_xmax1, reprojected_ymax1 = reproject_point(xmax, ymax)
        reprojected_xmin2, reprojected_ymax2 = reproject_point(xmin, ymax)
        reprojected_xmax2, reprojected_ymin2 = reproject_point(xmax, ymin)
        crop_bounds = \
            Extent(xmin=min(reprojected_xmin1,reprojected_xmin2), ymin=min(reprojected_ymin1,reprojected_ymin2),
                   xmax=max(reprojected_xmax1,reprojected_xmax2), ymax=max(reprojected_ymax1,reprojected_ymax2))
        return crop_bounds

    def _save_on_executors(self, spatial_rdd: gps.TiledRasterLayer, path,zlevel=6):
        geotiff_rdd = spatial_rdd.to_geotiff_rdd(
            storage_method=gps.StorageMethod.TILED,
            compression=gps.Compression.DEFLATE_COMPRESSION
        )

        basedir = pathlib.Path(str(path) + '.catalogresult')
        basedir.mkdir(parents=True, exist_ok=True)

        def write_tiff(item):
            key, data = item
            path = basedir / '{c}-{r}.tiff'.format(c=key.col, r=key.row)
            with path.open('wb') as f:
                f.write(data)

        geotiff_rdd.foreach(write_tiff)
        tiffs = [str(path.absolute()) for path in basedir.glob('*.tiff')]

        _log.info("Merging results {t!r}".format(t=tiffs))
        merge_args = [ "-o", path, "-of", "GTiff", "-co", "COMPRESS=DEFLATE", "-co", "TILED=TRUE","-co","ZLEVEL=%s"%zlevel]
        merge_args += tiffs
        _log.info("Executing: {a!r}".format(a=merge_args))
        #xargs avoids issues with too many args
        subprocess.run(['xargs', '-0', 'gdal_merge.py'], input='\0'.join(merge_args), universal_newlines=True)


    def _save_stitched(self, spatial_rdd, path, crop_bounds=None,zlevel=6) -> 'Extent':
        jvm = self._get_jvm()

        max_compression = jvm.geotrellis.raster.io.geotiff.compression.DeflateCompression(zlevel)

        if crop_bounds:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitched(spatial_rdd.srdd.rdd(), path, crop_bounds._asdict(),
                                                                   max_compression)
        else:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitched(spatial_rdd.srdd.rdd(), path, max_compression)

    def _save_stitched_tile_grid(self, spatial_rdd, path, tile_grid, crop_bounds=None, zlevel=6):
        jvm = self._get_jvm()

        max_compression = jvm.geotrellis.raster.io.geotiff.compression.DeflateCompression(zlevel)

        if crop_bounds:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitchedTileGrid(spatial_rdd.srdd.rdd(), path,
                                                                                  tile_grid, crop_bounds._asdict(),
                                                                                  max_compression)
        else:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitchedTileGrid(spatial_rdd.srdd.rdd(), path,
                                                                                  tile_grid, max_compression)

    def _save_stitched_tiled(self, spatial_rdd, filename):
        import rasterio as rstr
        from affine import Affine
        import rasterio._warp as rwarp

        max_level = self.get_max_level()

        spatial_rdd = spatial_rdd.persist()

        sorted_keys = sorted(spatial_rdd.collect_keys())

        upper_left_coords = GeopysparkDataCube._mapTransform(max_level.layer_metadata.layout_definition, sorted_keys[0])
        lower_right_coords = GeopysparkDataCube._mapTransform(max_level.layer_metadata.layout_definition, sorted_keys[-1])

        data = spatial_rdd.stitch()

        bands, w, h = data.cells.shape
        nodata = max_level.layer_metadata.no_data_value
        dtype = data.cells.dtype
        ex = Extent(xmin=upper_left_coords.left, ymin=lower_right_coords.bottom, xmax=lower_right_coords.right, ymax=upper_left_coords.top)
        cw, ch = (ex.xmax - ex.xmin) / w, (ex.ymax - ex.ymin) / h
        overview_level = int(math.log(w) / math.log(2) - 8)

        with rstr.io.MemoryFile() as memfile, open(filename, 'wb') as f:
            with memfile.open(driver='GTiff',
                              count=bands,
                              width=w,
                              height=h,
                              transform=Affine(cw, 0.0, ex.xmin,
                                               0.0, -ch, ex.ymax),
                              crs=rstr.crs.CRS.from_proj4(spatial_rdd.layer_metadata.crs),
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

    def ndvi(self, **kwargs) -> 'GeopysparkDataCube':
        return self._ndvi_v10(**kwargs)

    def _ndvi_v10(self, nir: str = None, red: str = None, target_band: str = None) -> 'GeopysparkDataCube':
        """1.0-style of ndvi process"""
        if not self.metadata.has_band_dimension():
            raise OpenEOApiException(
                status_code=400,
                code="DimensionAmbiguous",
                message="dimension of type `bands` is not available or is ambiguous.",
            )

        if target_band and target_band in self.metadata.band_names:
            raise OpenEOApiException(
                status_code=400,
                code="BandExists",
                message="A band with the specified target name exists.",
            )

        def first_index_if(coll, pred, *fallbacks):
            try:
                return next((i for i, elem in enumerate(coll) if pred(elem)))
            except StopIteration:
                if fallbacks:
                    head, *tail = fallbacks
                    return first_index_if(coll, head, *tail)
                else:
                    return None

        if not red:
            red_index = first_index_if(self.metadata.bands, lambda b: b.common_name == 'red')
        else:
            red_index = first_index_if(self.metadata.bands, lambda b: b.name == red, lambda b: b.common_name == red)

        if not nir:
            nir_index = first_index_if(self.metadata.bands, lambda b: b.common_name == 'nir')
        else:
            nir_index = first_index_if(self.metadata.bands, lambda b: b.name == nir, lambda b: b.common_name == nir)

        if red_index is None:
            raise OpenEOApiException(
                status_code=400,
                code="RedBandAmbiguous",
                message="The red band can't be resolved, please specify a band name.",
            )
        if nir_index is None:
            raise OpenEOApiException(
                status_code=400,
                code="NirBandAmbiguous",
                message="The NIR band can't be resolved, please specify a band name.",
            )

        ndvi_collection = self._ndvi_collection(red_index, nir_index,append=bool(target_band))

        if target_band:  # append a new band named $target_band
            result_metadata = self.metadata.append_band(Band(name=target_band, common_name=target_band, wavelength_um=None))
        else:  # drop all bands
            result_metadata = self.metadata.reduce_dimension("bands")

        return GeopysparkDataCube(pyramid=ndvi_collection.pyramid, metadata=result_metadata)

    def _ndvi_collection(self, red_index: int, nir_index: int, append=False) -> 'GeopysparkDataCube':
        reduce_graph = {
            "red": {
                "process_id": "array_element",
                "arguments": {"data": {"from_parameter": "data"}, "index": red_index}
            },
            "nir": {
                "process_id": "array_element",
                "arguments": {"data": {"from_parameter": "data"}, "index": nir_index}
            },
            "nirminusred": {
                "process_id": "subtract",
                "arguments": {
                    "x": {"from_node": "nir"},
                    "y": {"from_node": "red"},
                }
            },
            "nirplusred": {
                "process_id": "add",
                "arguments": {
                    "x": {"from_node": "nir"},
                    "y": {"from_node": "red"},
                }
            },
            "ndvi": {
                "process_id": "divide",
                "arguments": {
                    "x": {"from_node": "nirminusred"},
                    "y": {"from_node": "nirplusred"},
                },
                "result": not append,
            },
            "array_append": {
                "process_id": "array_append",
                "arguments": {
                    "data": {"from_parameter": "data"},
                    "value": {"from_node": "ndvi"},
                },
                "result": append,
            },
        }

        from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
        visitor = GeotrellisTileProcessGraphVisitor()

        return self.reduce_bands(visitor.accept_process_graph(reduce_graph))

    def apply_atmospheric_correction(self, missionID: str, sza: float, vza: float, raa: float, gnd: float, aot: float, cwv: float, appendDebugBands: bool) -> 'GeopysparkDataCube':
        return self.atmospheric_correction(missionID     , sza       , vza       , raa       , gnd       , aot       , cwv       , appendDebugBands      )

    def atmospheric_correction(self,method:str,elevation_model:str, missionID: str, sza: float, vza: float, raa: float, gnd: float, aot: float,
                               cwv: float, appendDebugBands: bool) -> 'GeopysparkDataCube':
        if missionID is None: missionID = "SENTINEL2"
        if method is None: method = "ICOR"
        if elevation_model is None: elevation_model = "DEM"
        if sza is None: sza = np.NaN
        if vza is None: vza = np.NaN
        if raa is None: raa = np.NaN
        if gnd is None: gnd = np.NaN
        if aot is None: aot = np.NaN
        if cwv is None: cwv = np.NaN
        if appendDebugBands is not None:
            if appendDebugBands == 1:
                appendDebugBands = True
            else:
                appendDebugBands = False
        else:
            appendDebugBands = False
        bandIds = self.metadata.band_names
        _log.info("Bandids: " + str(bandIds))
        atmo_corrected = self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: gps.get_spark_context()._jvm.org.openeo.geotrellis.icor.AtmosphericCorrection().correct(
                # ICOR or SMAC
                method,
                gps.get_spark_context()._jsc,
                rdd,
                bandIds,
                # [29.0, 5.0, 130.0, nodefault, nodefault, nodefault, 0.33],
                [sza, vza, raa, gnd, aot, cwv, 0.33],
                # DEM or SRTM
                elevation_model,
                # SENTINEL2 or LANDSAT8 for now
                missionID,
                appendDebugBands
            )
        )
        return atmo_corrected

    def sar_backscatter(self, args: SarBackscatterArgs) -> 'GeopysparkDataCube':
        # Nothing to do: the actual SAR backscatter processing already happened in `load_collection`
        return self

    def resolution_merge(self, args: ResolutionMergeArgs) -> 'GeopysparkDataCube':
        high_band_indices = [self.metadata.get_band_index(b) for b in args.high_resolution_bands]
        low_band_indices = [self.metadata.get_band_index(b) for b in args.low_resolution_bands]
        #TODO only works for Sentinel-2, throw error otherwise
        merged = self._apply_to_levels_geotrellis_rdd(
            lambda rdd,
                   level: gps.get_spark_context()._jvm.org.openeo.geotrellis.ard.Pansharpening().pansharpen_sentinel2(
                rdd,
                high_band_indices,
                low_band_indices

            )
        )
        return merged
