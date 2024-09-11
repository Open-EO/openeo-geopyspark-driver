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
from typing import Dict, List, Union, Tuple, Iterable, Callable, Optional

import geopyspark as gps
import numpy as np
import pandas as pd
import pyproj
import pytz
import xarray as xr
from geopyspark import TiledRasterLayer, Pyramid, Tile, SpaceTimeKey, SpatialKey, Metadata
from geopyspark.geotrellis import Extent, ResampleMethod
from geopyspark.geotrellis.constants import CellType
from pandas import Series
from pyproj import CRS
from shapely.geometry import mapping, Point, Polygon, MultiPolygon, GeometryCollection, box
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.metadata import Band
from openeo.udf import UdfData
from openeo.udf.xarraydatacube import XarrayDataCube, XarrayIO
from openeo.util import dict_no_none, str_truncate
from openeo_driver.datacube import DriverDataCube, DriverVectorCube
from openeo_driver.datastructs import ResolutionMergeArgs
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.errors import FeatureUnsupportedException, OpenEOApiException, \
    ProcessParameterInvalidException
from openeo_driver.ProcessGraphDeserializer import convert_node, _period_to_intervals
from openeo_driver.save_result import AggregatePolygonResult
from openeo_driver.utils import EvalEnv
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata
from openeogeotrellis.ml.geopysparkmlmodel import GeopysparkMlModel
from openeogeotrellis.processgraphvisiting import GeotrellisTileProcessGraphVisitor, SingleNodeUDFProcessGraphVisitor
from openeogeotrellis.ml.aggregatespatialvectorcube import AggregateSpatialVectorCube
from openeogeotrellis.utils import (
    to_projected_polygons,
    log_memory,
    ensure_executor_logging,
    get_jvm,
    temp_csv_dir, reproject_cellsize,
)
from openeogeotrellis.udf import run_udf_code
from openeogeotrellis._version import __version__ as softwareversion
from openeogeotrellis.vectorcube import AggregateSpatialResultCSV


_log = logging.getLogger(__name__)

SpatialExtent = collections.namedtuple("SpatialExtent", ["top", "bottom", "right", "left", "height", "width"])

def callsite(func):
    def try_str(f):
        try:
            return str(f)
        except Exception as e:
            return repr(e)

    def run(*args, **kwargs):
        name = func.__name__
        name += ','.join(map(try_str,args))
        name += ','.join(map(try_str, kwargs.values()))
        gps.get_spark_context().setLocalProperty("callSite.short",name)
        try:
            return func(*args, **kwargs)
        finally:
            gps.get_spark_context().setLocalProperty("callSite.short", None)

    return run


class GeopysparkDataCube(DriverDataCube):

    metadata: GeopysparkCubeMetadata = None

    def __init__(
            self, pyramid: Pyramid,
            metadata: GeopysparkCubeMetadata = None
    ):
        super().__init__(metadata=metadata or GeopysparkCubeMetadata({}))
        self.pyramid = pyramid

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
        jvm = get_jvm()
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

    @callsite
    def filter_temporal(self, start: str, end: str) -> 'GeopysparkDataCube':
        # TODO: is this necessary? Temporal range is handled already at load_collection time
        return self.apply_to_levels(
            lambda rdd: rdd.filter_by_times([pd.to_datetime(start), pd.to_datetime(end)]),
            metadata=self.metadata.filter_temporal(start, end)
        )

    @callsite
    def filter_bbox(self, west, east, north, south, crs=None, base=None, height=None) -> 'GeopysparkDataCube':
        return self.filter_spatial(geometries=box(west,south,east,north),geometry_crs=crs,mask=False)

    @callsite
    def filter_spatial(
        self, geometries: Union[Polygon, MultiPolygon, DriverVectorCube], geometry_crs="EPSG:4326", mask=True
    ) -> "GeopysparkDataCube":
        # TODO: support more geometry types but geopyspark.geotrellis.layer.TiledRasterLayer.mask doesn't seem to work
        #  with e.g. GeometryCollection

        max_level = self.get_max_level()
        layer_crs = max_level.layer_metadata.crs

        if isinstance(geometries, DriverVectorCube):
            geometry_crs = geometries.get_crs()
            geometries = geometries.to_multipolygon()

        reprojected_polygon = self.__reproject_polygon(polygon=geometries, srs=geometry_crs, dest_srs=layer_crs)

        if mask:
            masked = self.mask_polygon(reprojected_polygon,srs=layer_crs)
        else:
            masked = self
        xmin, ymin, xmax, ymax = reprojected_polygon.bounds

        crop_extent = get_jvm().geotrellis.vector.Extent(xmin, ymin, xmax, ymax)
        crop = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().crop_metadata

        return masked._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: crop(rdd,crop_extent),
            metadata=self.metadata.filter_bbox(west=xmin, south=ymin, east=xmax, north=ymax, crs=layer_crs)
        )

    @callsite
    def filter_bands(self, bands) -> 'GeopysparkDataCube':
        band_indices = [self.metadata.get_band_index(b) for b in bands]
        _log.info("filter_bands({b!r}) -> indices {i!r}".format(b=bands, i=band_indices))
        return self.apply_to_levels(lambda rdd: rdd.bands(band_indices), metadata=self.metadata.filter_bands(bands))

    @callsite
    def filter_labels(self, condition: dict, dimension: str, context: Optional[dict] = None,
                      env: EvalEnv = None) -> 'DriverDataCube':
        #TODO this is provided by FileLayerProvider, but also need this here
        return self

    @callsite
    def rename_dimension(self, source: str, target: str) -> 'GeopysparkDataCube':
        return GeopysparkDataCube(pyramid=self.pyramid, metadata=self.metadata.rename_dimension(source, target))

    @callsite
    def apply(self, process: dict, *, context: Optional[dict] = None, env: EvalEnv) -> "GeopysparkDataCube":
        from openeogeotrellis.backend import GeoPySparkBackendImplementation

        if isinstance(process, dict):
            datatype = self.get_max_level().layer_metadata.cell_type
            process = GeoPySparkBackendImplementation.accept_process_graph(process, default_input_parameter="data",
                                                                           default_input_datatype=datatype)

        if isinstance(process, GeotrellisTileProcessGraphVisitor):
            #apply should leave metadata intact, so can do a simple call?
            # Note: It's not obvious from its name, but `reduce_bands` not only supports reduce operations,
            # also `apply` style local unary mapping operations.
            return  self._apply_bands_dimension(process,context = context)
        if isinstance(process, SingleNodeUDFProcessGraphVisitor):
            udf, udf_context = self._extract_udf_code_and_context(process=process, context=context, env=env)
            runtime = process.udf_args.get("runtime", "Python")
            return self.apply_tiles(udf_code=udf, context=udf_context, runtime=runtime)
        else:
            raise FeatureUnsupportedException(f"Unsupported: apply with {process}")

    def _extract_udf_code_and_context(
        self,
        process: SingleNodeUDFProcessGraphVisitor,
        context: dict,
        env: Optional[EvalEnv] = None,
    ) -> Tuple[str, dict]:
        """Extract UDF code and UDF context from given visitor and parent's context"""
        udf = process.udf_args.get("udf")
        if not isinstance(udf, str):
            raise ValueError(f"The 'run_udf' process requires at least a 'udf' string argument, but got: {udf!r}.")

        udf_context = process.udf_args.get("context", {})
        # Resolve "from_parameter" references
        udf_context = convert_node(udf_context, env=(env or EvalEnv()).push_parameters({"context": context}))

        return udf, udf_context

    @callsite
    def apply_dimension(
        self,
        process: Union[dict, GeotrellisTileProcessGraphVisitor],
        *,
        dimension: str,
        target_dimension: Optional[str] = None,
        context: Optional[dict] = None,
        env: EvalEnv,
    ) -> "DriverDataCube":
        from openeogeotrellis.backend import GeoPySparkBackendImplementation

        apply_bands = self.metadata.has_band_dimension() and dimension == self.metadata.band_dimension.name
        datatype = "float32" if apply_bands else self.get_max_level().layer_metadata.cell_type

        if isinstance(process, dict):
            process = GeoPySparkBackendImplementation.accept_process_graph(process,default_input_parameter="data",default_input_datatype=datatype)
        if isinstance(process, GeotrellisTileProcessGraphVisitor):

            if self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name:
                context = convert_node(context, env=env)
                pysc = gps.get_spark_context()
                if self.metadata.has_band_dimension() and target_dimension == self.metadata.band_dimension.name:
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
            elif apply_bands:
                return self._apply_bands_dimension(process)
            else:
                raise FeatureUnsupportedException(f"apply_dimension along dimension {dimension} is not supported. These dimensions are available: " + str(self.metadata.dimension_names()))
        if isinstance(process, SingleNodeUDFProcessGraphVisitor):
            udf, udf_context = self._extract_udf_code_and_context(process=process, context=context, env=env)
            runtime = process.udf_args.get("runtime", "Python")
            return self._run_udf_dimension(udf=udf, udf_context=udf_context, dimension=dimension, runtime=runtime)

        raise FeatureUnsupportedException(f"Unsupported: apply_dimension with {process}")

    @callsite
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

    def _apply_bands_dimension(self, pgVisitor: GeotrellisTileProcessGraphVisitor, context=None) -> 'GeopysparkDataCube':
        """
        Apply a process graph to every tile, with tile.bands (List[Tile]) as process input.
        """
        # All processing is done with float32 as cell type.
        float_datacube = self.apply_to_levels(lambda layer: layer.convert_data_type("float32"))

        # Apply process to every tile, with tile.bands (List[Tile]) as process input.
        # This is done for the entire pyramid.
        pysc = gps.get_spark_context()
        if isinstance(context, GeopysparkMlModel):
            context = context.get_java_object()
        if context is None:
            context = {}
        elif not isinstance(context, dict):
            context = {"context": context}

        if self.metadata.has_band_dimension():
            context["array_labels"] = self.metadata.band_names
        else:
            _log.warning(f"Applying callback to the bands, but no band labels available on this datacube.")

        result_cube: GeopysparkDataCube = float_datacube._apply_to_levels_geotrellis_rdd(
            lambda rdd, level:
                pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mapBands(
                    rdd, pgVisitor.builder, context
                )
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

    @callsite
    def add_dimension(self, name: str, label: str, type: str = None):
        return GeopysparkDataCube(
            pyramid=self.pyramid,
            metadata=self.metadata.add_dimension(name=name, label=label, type=type)
        )

    @callsite
    def drop_dimension(self, name: str):
        if name not in self.metadata.dimension_names():
            raise OpenEOApiException(status_code=400, code="DimensionNotAvailable", message=
                """Dimension with name '{}' does not exist""".format(name))

        if self.metadata.has_temporal_dimension() and self.metadata.temporal_dimension.name == name:
            return self.apply_to_levels(lambda l:l.to_spatial_layer(),metadata=self.metadata.drop_dimension(name=name))
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

    @callsite
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

    @callsite
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
                raise OpenEOApiException(
                    status_code=400,
                    message="""In run_udf, the data has {b} bands, while the 'bands' dimension has {len_dim} labels.
                These labels were set on the dimension: {labels}. Please investigate if dimensions and labels are correct. The mismatch occured for {extent} and {time}.""".format(
                        b=band_count, len_dim=len(band_coordinates), labels=str(band_coordinates), extent = str(extent), time = time_coordinates
                    ),
                )

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
            gridx = (extent.right - extent.left) / extent.width
            gridy = (extent.top - extent.bottom) / extent.height
            xdelta = gridx * 0.5 * (bands_numpy.shape[-1] - extent.width)
            ydelta = gridy * 0.5 * (bands_numpy.shape[-2] - extent.height)
            xmin = extent.left - xdelta
            xmax = extent.right + xdelta
            ymin = extent.bottom - ydelta
            ymax = extent.top + ydelta
            coords["x"] = np.linspace(xmin + 0.5 * gridx, xmax - 0.5 * gridx, bands_numpy.shape[-1], dtype=np.float32)
            coords["y"] = np.linspace(ymax - 0.5 * gridy, ymin + 0.5 * gridy, bands_numpy.shape[-2], dtype=np.float32)

        the_array = xr.DataArray(bands_numpy, coords=coords,dims=dims,name="openEODataChunk")
        return XarrayDataCube(the_array)

    @callsite
    def apply_tiles_spatiotemporal(self, udf_code: str, udf_context: Optional[dict] = None, runtime: str = "Python", overlap_x: int = 0, overlap_y: int = 0) -> "GeopysparkDataCube":
        """
        Group tiles by SpatialKey, then apply a Python function to every group of tiles.
        :param udf_code: A string containing a Python function that handles groups of tiles, each labeled by date.
        :return: The original data cube with its tiles transformed by the function.
        """

        # Early compile to detect syntax errors
        _log.info(f"[apply_tiles_spatiotemporal] Setting up for running UDF {str_truncate(udf_code, width=1000)!r}")
        _ = compile(source=udf_code, filename='UDF.py', mode='exec')

        if runtime == "Python-Jep":
            band_names = []
            if self.metadata.has_band_dimension():
                band_names = self.metadata.band_dimension.band_names
            new_bands: Optional[str] = None

            def rdd_function(rdd, _zoom):
                nonlocal new_bands  # TODO: Get rid of nonlocal usage
                jvm = gps.get_spark_context()._jvm
                udf = jvm.org.openeo.geotrellis.udf.Udf
                tup = udf.runUserCodeSpatioTemporalWithBands(udf_code, rdd, band_names, udf_context, overlap_x, overlap_y)
                if new_bands:
                    assert new_bands == list(tup._2())
                new_bands = list(tup._2())
                return tup._1()

            float_cube = self.apply_to_levels(lambda layer: self._convert_celltype(layer, "float32"))
            ret = float_cube._apply_to_levels_geotrellis_rdd(rdd_function, self.metadata, gps.LayerType.SPACETIME)
            if new_bands:
                self.metadata.band_dimension.bands = [Band(b) for b in new_bands]
            return ret

        @ensure_executor_logging
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

            data = UdfData(proj={"EPSG": CRS.from_user_input(metadata.crs).to_epsg()}, datacube_list=[datacube], user_context=udf_context)
            _log.debug(f"[apply_tiles_spatiotemporal] running UDF {str_truncate(udf_code, width=1000)!r} on {datacube!r} with context {udf_context}")
            result_data = run_udf_code(code=udf_code, data=data)
            cubes = result_data.get_datacube_list()
            if len(cubes) != 1:
                raise ValueError(f"The provided UDF should return one datacube, but got: {result_data}")
            result_array: xr.DataArray = cubes[0].array
            _log.debug(f"[apply_tiles_spatiotemporal] UDF resulted in {result_array}!r")
            if 't' in result_array.dims:
                result_array = result_array.transpose(*('t' ,'bands','y', 'x'))
                return [(SpaceTimeKey(col=tiles[0].col, row=tiles[0].row, instant=pd.Timestamp(timestamp)),
                         Tile(array_slice.values, CellType.FLOAT32, tile_list[0][1].no_data_value))
                        for timestamp, array_slice in result_array.groupby('t')]
            else:
                result_array = result_array.transpose(*( 'bands', 'y', 'x'))
                return [(SpaceTimeKey(col=tiles[0].col, row=tiles[0].row, instant=datetime.fromisoformat('2020-01-01T00:00:00')),
                         Tile(result_array.values, CellType.FLOAT32, tile_list[0][1].no_data_value))]

        def rdd_function(openeo_metadata: GeopysparkCubeMetadata, rdd: TiledRasterLayer) -> TiledRasterLayer:
            converted = rdd.convert_data_type(CellType.FLOAT32)
            float_rdd = converted.to_numpy_rdd()

            def to_spatial_key(tile: Tuple[SpaceTimeKey, Tile]):
                key: SpatialKey = gps.SpatialKey(tile[0].col, tile[0].row)
                value: Tuple[SpaceTimeKey, Tile] = (tile[0], tile[1])
                return (key, value)

            b = rdd.layer_metadata.bounds
            rows = b.maxKey.row - b.minKey.row + 1
            partitions = (b.maxKey.col - b.minKey.col + 1) * (rows)

            def partitionByKey(spatialkey):
                """
                Try having one partition per timeseries to bring memory to a minimum
                """
                try:
                    return rows * (spatialkey.col-b.minKey.col) + (spatialkey.row-b.minKey.row)
                except Exception as e:
                    import pyspark
                    hashPartitioner = pyspark.rdd.portable_hash
                    return hashPartitioner(tuple)
            # Group all tiles by SpatialKey. Save the SpaceTimeKey in the value with the Tile.
            spatially_grouped = float_rdd.map(lambda tile: to_spatial_key(tile)).groupByKey(numPartitions=partitions,partitionFunc=partitionByKey)

            # Apply the tile_function to all tiles with the same spatial key.
            numpy_rdd = spatially_grouped.flatMap(
                log_memory(partial(tile_function, rdd.layer_metadata, openeo_metadata))
            )

            # Convert the result back to a TiledRasterLayer.
            metadata = GeopysparkDataCube._transform_metadata(rdd.layer_metadata, cellType=CellType.FLOAT32)
            _log.info(f"apply_neighborhood created datacube {metadata}")
            return gps.TiledRasterLayer.from_numpy_rdd(gps.LayerType.SPACETIME, numpy_rdd, metadata)

        return self.apply_to_levels(partial(rdd_function, self.metadata))

    @callsite
    def chunk_polygon(
        self,
        reducer: Union[ProcessGraphVisitor, Dict],
        # TODO: it's wrong to use MultiPolygon as a collection of polygons. MultiPolygons should be handled as single, atomic "features"
        #       also see https://github.com/Open-EO/openeo-python-driver/issues/288
        chunks: MultiPolygon,
        mask_value: float,
        env: EvalEnv,
        context: Optional[dict] = None,
    ) -> "GeopysparkDataCube":
        # TODO: rename this to `apply_polygon`
        from openeogeotrellis.backend import GeoPySparkBackendImplementation

        if isinstance(reducer, dict):
            reducer = GeoPySparkBackendImplementation.accept_process_graph(reducer)

        if isinstance(chunks, Polygon):
            chunks = [chunks]
        elif isinstance(chunks, MultiPolygon):
            chunks: List[Polygon] = chunks.geoms
        else:
            raise ValueError(f"Invalid type for `chunks`: {type(chunks)}")

        jvm = get_jvm()

        result_collection = None
        if isinstance(reducer, SingleNodeUDFProcessGraphVisitor):
            udf, udf_context = self._extract_udf_code_and_context(process=reducer, context=context, env=env)
            # Polygons should use the same projection as the rdd.
            # TODO Usage of GeometryCollection should be avoided. It's abused here like a FeatureCollection,
            #       but a GeometryCollections is conceptually just single "feature".
            #       What you want here is proper support for FeatureCollections or at least a list of individual geometries.
            #       also see https://github.com/Open-EO/openeo-python-driver/issues/71, https://github.com/Open-EO/openeo-python-driver/issues/288
            reprojected_polygons: jvm.org.openeo.geotrellis.ProjectedPolygons \
                = to_projected_polygons(jvm, GeometryCollection(chunks))
            band_names = self.metadata.band_dimension.band_names

            def rdd_function(rdd, _zoom):
                return jvm.org.openeo.geotrellis.udf.Udf.runChunkPolygonUserCode(
                    udf, rdd, reprojected_polygons, band_names, udf_context, mask_value
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

    @callsite
    def reduce_dimension(
        self,
        reducer: Union[ProcessGraphVisitor, Dict],
        *,
        dimension: str,
        context: Optional[dict] = None,
        env: EvalEnv,
        binary=False,
    ) -> "GeopysparkDataCube":
        from openeogeotrellis.backend import GeoPySparkBackendImplementation

        if isinstance(reducer, dict):
            datatype = self.get_max_level().layer_metadata.cell_type
            reducer = GeoPySparkBackendImplementation.accept_process_graph(reducer, default_input_parameter="data",
                                                                           default_input_datatype=datatype)

        if isinstance(reducer, SingleNodeUDFProcessGraphVisitor):
            udf, udf_context = self._extract_udf_code_and_context(process=reducer, context=context, env=env)
            runtime = reducer.udf_args.get("runtime", "Python")
            result_collection = self._run_udf_dimension(udf=udf, udf_context=udf_context, dimension=dimension, runtime=runtime)
        elif self.metadata.has_band_dimension() and dimension == self.metadata.band_dimension.name:
            result_collection = self._apply_bands_dimension(reducer, context)
        elif self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name:
            pysc = gps.get_spark_context()
            result_collection = self._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().applyTimeDimension(
                    rdd, reducer.builder, context if isinstance(context, dict) else {}
                )
            )
        else:
            raise FeatureUnsupportedException(
                "Unsupported combination of reducer %s and dimension %s." % (reducer, dimension))
        if result_collection is not None:
            result_collection.metadata = result_collection.metadata.reduce_dimension(dimension)
            if self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name and self.pyramid.layer_type != gps.LayerType.SPATIAL:
                result_collection = result_collection.apply_to_levels(lambda rdd:  rdd.to_spatial_layer() if rdd.layer_type != gps.LayerType.SPATIAL else rdd)
        return result_collection

    def _run_udf_dimension(self, udf: str, udf_context: dict, dimension: str, runtime: str = "Python"):
        if not isinstance(udf, str):
            raise ValueError("The 'run_udf' process requires at least a 'udf' string argument, but got: '%s'." % udf)
        if self.metadata.has_temporal_dimension() and dimension == self.metadata.temporal_dimension.name:
            # EP-2760 a special case of reduce where only a single udf based callback is provided. The more generic case is not yet supported.
            return self.apply_tiles_spatiotemporal(udf_code=udf, udf_context=udf_context, runtime=runtime)
        elif self.metadata.has_band_dimension() and dimension == self.metadata.band_dimension.name:
            return self.apply_tiles(udf_code=udf, context=udf_context, runtime=runtime)
        else:
            raise FeatureUnsupportedException(f"reduce_dimension with UDF along dimension {dimension} is not supported")

    @callsite
    def apply_tiles(self, udf_code: str, context={}, runtime="python", overlap_x: int = 0, overlap_y: int = 0) -> 'GeopysparkDataCube':
        """Apply a function to the given set of bands in this image collection."""
        #TODO apply .bands(bands)

        # Early compile to detect syntax errors
        _log.info(f"[apply_tiles] setting up for running UDF {str_truncate(udf_code, width=1000)!r}")
        _ = compile(source=udf_code, filename='UDF.py', mode='exec')

        if runtime == 'Python-Jep':
            band_names = self.metadata.band_dimension.band_names
            new_bands: Optional[str] = None

            def rdd_function(rdd, _zoom):
                nonlocal new_bands  # TODO: Get rid of nonlocal usage
                jvm = gps.get_spark_context()._jvm
                udf = jvm.org.openeo.geotrellis.udf.Udf
                tup = udf.runUserCodeWithBands(udf_code, rdd, band_names, context, overlap_x, overlap_y)
                if new_bands:
                    assert new_bands == list(tup._2())
                new_bands = list(tup._2())
                return tup._1()

            # All JEP implementation work with the float datatype.
            float_cube = self.apply_to_levels(lambda layer: self._convert_celltype(layer, "float32"))
            ret = float_cube._apply_to_levels_geotrellis_rdd(rdd_function, self.metadata, gps.LayerType.SPACETIME)
            if new_bands:
                self.metadata.band_dimension.bands = [Band(b) for b in new_bands]

            return ret
        else:
            def rdd_function(openeo_metadata: GeopysparkCubeMetadata, rdd: TiledRasterLayer):
                """
                Apply a user defined function to every tile in a TiledRasterLayer
                and return the transformed TiledRasterLayer.
                """

                @ensure_executor_logging
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
                    data = UdfData(proj={"EPSG": CRS.from_user_input(metadata.crs).to_epsg()}, datacube_list=[datacube], user_context=context)

                    # Run UDF.
                    _log.debug(f"[apply_tiles] running UDF {str_truncate(udf_code, width=1000)!r} on {data}!r")
                    result_data = run_udf_code(code=udf_code, data=data)
                    _log.debug(f"[apply_tiles] UDF resulted in {result_data}!r")

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

    @callsite
    def aggregate_temporal(
        self, intervals: List, labels: List, reducer, dimension: str = None, context: Optional[dict] = None, reduce = True
    ) -> "GeopysparkDataCube":
        """Computes a temporal aggregation based on an array of date and/or time intervals.

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
        from openeogeotrellis.backend import GeoPySparkBackendImplementation
        if isinstance(reducer, dict):
            datatype = self.get_max_level().layer_metadata.cell_type
            reducer = GeoPySparkBackendImplementation.accept_process_graph(reducer, default_input_parameter="data",
                                                                           default_input_datatype=datatype)

        if isinstance(reducer, str):
            #deprecated codepath: only single process reduces
            pysc = gps.get_spark_context()
            mapped_keys = self._apply_to_levels_geotrellis_rdd(
                lambda rdd,level: pysc._jvm.org.openeo.geotrellis.OpenEOProcesses().mapInstantToInterval(rdd,intervals_iso,labels_iso))
            reducer = self._normalize_temporal_reducer(dimension, reducer)
            return mapped_keys.apply_to_levels(lambda rdd: rdd.aggregate_by_cell(reducer))
        elif isinstance(reducer, GeotrellisTileProcessGraphVisitor):
            def aggregate(rdd,level):
                pr = pysc._jvm.org.openeo.geotrellis.OpenEOProcesses()
                band_names = []
                if self.metadata.has_band_dimension():
                    band_names = self.metadata.band_names
                else:
                    band_names = ["band_unnamed"]
                wrapped = pr.wrapCube(rdd)
                wrapped.openEOMetadata().setBandNames(band_names)
                return pr.aggregateTemporal(wrapped, intervals_iso, labels_iso, reducer.builder, context if isinstance(context, dict) else {}, reduce)

            return self._apply_to_levels_geotrellis_rdd(aggregate)
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

    @callsite
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

        if overlaps_resolver is None:
            # TODO: checking for overlap should also consider spatial extent and temporal extent, not only bands #479
            intersection = [value for value in leftBandNames if value in rightBandNames]
            if len(intersection) > 0:
                # Spec: https://github.com/Open-EO/openeo-processes/blob/0dd3ab0d81f67506547136532af39b5c9a16771e/merge_cubes.json#L83-L87
                raise OpenEOApiException(
                    status_code=400,
                    code="OverlapResolverMissing",
                    message=f"merge_cubes: Overlapping data cubes, but no overlap resolver has been specified."
                    + f" Either set an overlaps_resolver or rename the bands."
                    + f" Left names: {leftBandNames}, right names: {rightBandNames}.",
                )

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
                    merged_data.metadata = merged_data.metadata.append_band(iband)

        return merged_data

    # TODO legacy alias to be removed
    merge = merge_cubes

    @callsite
    def mask_polygon(self, mask: Union[Polygon, MultiPolygon], srs="EPSG:4326",
                     replacement=None, inside=False) -> 'GeopysparkDataCube':
        max_level = self.get_max_level()
        layer_crs = max_level.layer_metadata.crs
        reprojected_polygon = self.__reproject_polygon(mask,CRS.from_user_input(srs), layer_crs)
        # TODO should we warn when masking generates an empty collection?
        # TODO: use `replacement` and `inside`
        return self.apply_to_levels(lambda rdd: rdd.mask(
            reprojected_polygon,
            partition_strategy=None,
            options=gps.RasterizerOptions()
        ))

    @callsite
    def mask(self, mask: 'GeopysparkDataCube',
             replacement=None) -> 'GeopysparkDataCube':

        replacement = float(replacement) if replacement is not None else None
        if self._is_spatial() and mask._is_spatial():
            rasterMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask_spatial_spatial
        elif mask._is_spatial():
            rasterMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask_spacetime_spatial
        else:
            rasterMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().rasterMask

        return self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: rasterMask(rdd, mask.pyramid.levels[level].srdd.rdd(), replacement)
        )

    def to_scl_dilation_mask(
        self,
        erosion_kernel_size: int,
        mask1_values: List[int],
        mask2_values: List[int],
        kernel1_size: int,
        kernel2_size: int,
    ) -> "GeopysparkDataCube":
        toMask = gps.get_spark_context()._jvm.org.openeo.geotrellis.OpenEOProcesses().toSclDilationMask
        return self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: toMask(
                rdd,
                erosion_kernel_size,
                mask1_values,
                mask2_values,
                kernel1_size,
                kernel2_size,
            )
        )

    @callsite
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

    @callsite
    def apply_neighborhood(
        self, process: dict, *, size: List[dict], overlap: List[dict], context: Optional[dict] = None, env: EvalEnv
    ) -> "GeopysparkDataCube":
        spatial_dims = self.metadata.spatial_dimensions
        if len(spatial_dims) != 2:
            raise OpenEOApiException(message="Unexpected spatial dimensions in apply_neighborhood,"
                                             " expecting exactly 2 spatial dimensions: %s" % str(spatial_dims))
        x = spatial_dims[0]
        y = spatial_dims[1]
        size_dict = {e['dimension']:e for e in size}
        overlap_dict = {e['dimension']: e for e in overlap} if overlap is not None else {}
        if size_dict.get(x.name, {}).get('unit', None) != 'px' or size_dict.get(y.name, {}).get('unit', None) != 'px':
            raise ProcessParameterInvalidException(
                parameter="size",
                process="apply_neighborhood",
                reason=f"only 'px' is currently supported for spatial window size (got {size!r})",
            )
        sizeX = int(size_dict[x.name]['value'])
        sizeY = int(size_dict[y.name]['value'])

        overlap_x_dict = overlap_dict.get(x.name,{'value': 0, 'unit': 'px'})
        overlap_y_dict = overlap_dict.get(y.name,{'value': 0, 'unit': 'px'})
        if overlap_x_dict.get('unit', None) != 'px' or overlap_y_dict.get('unit', None) != 'px':
            raise ProcessParameterInvalidException(
                parameter="overlap",
                process="apply_neighborhood",
                reason=f"only 'px' is currently supported for spatial overlap (got {overlap!r})",
            )
        jvm = get_jvm()
        overlap_x = int(overlap_x_dict['value'])
        overlap_y = int(overlap_y_dict['value'])
        # TODO: Retile only accepts SpaceTimeKeys
        retiled_collection = self._apply_to_levels_geotrellis_rdd(
            lambda rdd, level: jvm.org.openeo.geotrellis.OpenEOProcesses().retile(rdd, sizeX, sizeY, overlap_x, overlap_y))

        retiled_metadata: Metadata = retiled_collection.pyramid.levels[retiled_collection.pyramid.max_zoom].layer_metadata
        retiled_tile_layout = retiled_metadata.tile_layout

        from openeogeotrellis.backend import GeoPySparkBackendImplementation

        process = GeoPySparkBackendImplementation.accept_process_graph(process)
        temporal_size = temporal_overlap = None
        has_time_dim = self.metadata.has_temporal_dimension()
        if has_time_dim:
            temporal_size = size_dict.get(self.metadata.temporal_dimension.name,None)
            temporal_overlap = overlap_dict.get(self.metadata.temporal_dimension.name, None)

        result_collection = None
        if isinstance(process, SingleNodeUDFProcessGraphVisitor):
            runtime = process.udf_args.get('runtime', 'Python')
            udf, udf_context = self._extract_udf_code_and_context(process=process, context=context, env=env)

            if sizeX < 32 or sizeY < 32:
                raise ProcessParameterInvalidException(
                    parameter="size",
                    process="apply_neighborhood",
                    reason=f"window sizes smaller then 32 are not yet supported for UDFs (got {size!r}).",
                )

            if has_time_dim and (temporal_size is None or temporal_size.get('value',None) is None):
                #full time dimension has to be provided
                result_collection = retiled_collection.apply_tiles_spatiotemporal(udf_code = udf,
                    udf_context = udf_context, runtime = runtime, overlap_x = overlap_x, overlap_y = overlap_y)
            elif not has_time_dim or (temporal_size.get('value',None) == 'P1D' and temporal_overlap is None):
                result_collection = retiled_collection.apply_tiles(udf_code = udf, context = udf_context,
                    runtime = runtime, overlap_x = overlap_x, overlap_y = overlap_y)
            else:
                raise ProcessParameterInvalidException(
                    parameter="size",
                    process="apply_neighborhood",
                    reason=f"for temporal dimension, either process all values, or 'P1D' for single date is currently supported."
                    + f" Overlap should not be set for time dimension."
                    + f" (Got {temporal_size=}, {temporal_overlap=})",
                )
            if overlap_x > 0 or overlap_y > 0:
                # Check if the resolution of result_collection changed (UDF feature).
                result_metadata: Metadata = result_collection.pyramid.levels[
                    result_collection.pyramid.max_zoom].layer_metadata
                result_tile_layout = result_metadata.tile_layout
                # Change size and overlap if #pixels per tile changed.
                if result_tile_layout.tileCols != retiled_tile_layout.tileCols or result_tile_layout.tileRows != retiled_tile_layout.tileRows:
                    ratio_x = result_tile_layout.tileCols / retiled_tile_layout.tileCols
                    ratio_y = result_tile_layout.tileRows / retiled_tile_layout.tileRows
                    sizeX = int(sizeX * ratio_x)
                    sizeY = int(sizeY * ratio_y)
                    overlap_x = int(overlap_x * ratio_x)
                    overlap_y = int(overlap_y * ratio_y)

        elif isinstance(process, GeotrellisTileProcessGraphVisitor):
            if temporal_size is None or temporal_size.get('value', None) is None:
                raise OpenEOApiException(message="apply_neighborhood: only supporting complex callbacks on bands")
            elif temporal_size.get('value', None) == 'P1D' and temporal_overlap is None:
                result_collection = self._apply_bands_dimension(process)
            elif temporal_size.get('value', None) != None and temporal_overlap is None and sizeX==1 and sizeY==1 and overlap_x==0 and overlap_y==0:
                if(not self.metadata.has_temporal_dimension()):
                    raise OpenEOApiException(message=f"apply_neighborhood: no time dimension on cube, cannot chunk on time with value {temporal_size}")
                temporal_extent = self.metadata.temporal_dimension.extent
                start = temporal_extent[0]
                end = temporal_extent[1]
                intervals = _period_to_intervals(start,end,temporal_size.get('value', None))
                result_collection = self.aggregate_temporal(intervals, None, process, None, context,reduce=False)
            else:
                raise OpenEOApiException(message="apply_neighborhood: only supporting complex callbacks on bands")
        else:
            raise OpenEOApiException(message="apply_neighborhood: only supporting callbacks with a single UDF.")

        if overlap_x > 0 or overlap_y > 0:
            result_collection = result_collection._apply_to_levels_geotrellis_rdd(
                lambda rdd, level: jvm.org.openeo.geotrellis.OpenEOProcesses().remove_overlap(rdd, sizeX, sizeY,
                                                                                      overlap_x,
                                                                                      overlap_y))

        return result_collection

    @callsite
    def resample_cube_spatial(self, target: 'GeopysparkDataCube', method: str = 'near') -> 'GeopysparkDataCube':
        """
        Resamples the spatial dimensions (x,y) of this data cube to a target data cube and return the results as a new data cube.

        https://processes.openeo.org/#resample_cube_spatial

        :param target: An data cube that specifies the target
        :param method: The resampling method.
        :return: A raster data cube with values warped onto the new projection.

        """
        resample_method = ResampleMethod(self._get_resample_method(method))

        max_level:TiledRasterLayer = self.get_max_level()
        target_max_level:TiledRasterLayer = target.pyramid.levels[target.pyramid.max_zoom]
        if self.pyramid.layer_type == gps.LayerType.SPACETIME and target.pyramid.layer_type == gps.LayerType.SPACETIME:
            level_rdd_tuple = get_jvm().org.openeo.geotrellis.OpenEOProcesses().resampleCubeSpatial(max_level.srdd.rdd(),target_max_level.srdd.rdd(),resample_method)
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
            level_rdd_tuple = get_jvm().org.openeo.geotrellis.OpenEOProcesses().resampleCubeSpatial_spatial(
                max_level.srdd.rdd(), crs, layout, resample_method, partitioner)
        else:
            raise FeatureUnsupportedException(message='resample_cube_spatial - Unsupported combination of two cubes of type: ' + str(self.pyramid.layer_type) + ' and ' + str(target.pyramid.layer_type))

        layer = self._create_tilelayer(level_rdd_tuple._2(),max_level.layer_type,target.pyramid.max_zoom)
        pyramid = Pyramid({target.pyramid.max_zoom:layer})
        return GeopysparkDataCube(pyramid=pyramid, metadata=self.metadata)

    @callsite
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
        cube_crs = max_level.layer_metadata.crs
        current_crs_proj4 = cube_crs
        _log.info(
            f"Reprojecting datacube with crs {current_crs_proj4} and layout {max_level.layer_metadata.layout_definition} to {projection} and {resolution}"
        )
        if projection is not None and CRS.from_user_input(projection).equals(CRS.from_user_input(current_crs_proj4)):
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

            if projection is not None:
                extent = GeopysparkDataCube._reproject_extent(
                    cube_crs, projection, extent.xmin, extent.ymin, extent.xmax, extent.ymax
                )

            newLayout = GeopysparkDataCube._layout_for_resolution(extent,currentTileLayout, projection,resolution)
            if newLayout == None:
                return self

            cellsize_before = self.get_cellsize()
            if not isinstance(resolution, tuple):
                resolution = (resolution, resolution)

            if projection is not None:
                # reproject to target CRS to make meaningful comparisons
                e = max_level.layer_metadata.layout_definition.extent
                e = GeopysparkDataCube._reproject_extent(
                    cube_crs, "EPSG:4326", e.xmin, e.ymin, e.xmax, e.ymax
                )
                extent_dict = {"west": e.xmin, "east": e.xmax,
                               "south": e.ymin, "north": e.ymax,
                               "crs": "EPSG:4326"}

                cellsize_before = reproject_cellsize(extent_dict, cellsize_before, cube_crs, projection)

            estimated_size_in_pixels_tup = self.calculate_layer_size_in_pixels()

            resolution_factor = (
                cellsize_before[0] / resolution[0],
                cellsize_before[1] / resolution[1]
            )

            proposed_partition_count_tup = (
                estimated_size_in_pixels_tup[0] * resolution_factor[0] / currentTileLayout.tileCols,
                estimated_size_in_pixels_tup[1] * resolution_factor[1] / currentTileLayout.tileRows
            )
            # Could also use "currentTileLayout.layoutCols * resolution_factor[0]", but that would be less accurate.
            # The repartitioning only considers the resolution change. It considers that the partitioning took
            # already into account band count and the pixel type, sparse/not, ...
            proposed_partition_count = int(proposed_partition_count_tup[0] * proposed_partition_count_tup[1])
            if (  # Only repartition when there would be significantly more
                    max_level.getNumPartitions() * 2 <= proposed_partition_count
                    and max_level.layer_type == gps.LayerType.SPACETIME):
                if proposed_partition_count < 10000:
                    _log.info(
                        f"Repartitioning datacube with {max_level.getNumPartitions()} partitions to {proposed_partition_count} before resample_spatial."
                    )
                    max_level = max_level.repartition(int(proposed_partition_count))
                else:
                    _log.warning(
                        f"resample_spatial proposed new partition count {proposed_partition_count} is too high, not repartitioning."
                    )

            if(projection is not None):
                resampled = max_level.tile_to_layout(newLayout,target_crs=projection, resample_method=resample_method)
            else:
                resampled = max_level.tile_to_layout(newLayout,resample_method=resample_method)

            pyramid = Pyramid({0: resampled})
            return GeopysparkDataCube(pyramid=pyramid, metadata=self.metadata)
            #return self.apply_to_levels(lambda layer: layer.tile_to_layout(projection, resample_method))
        return self

    def get_cellsize(self):
        max_level = self.get_max_level()
        extent = max_level.layer_metadata.layout_definition.extent
        currentTileLayout: gps.TileLayout = max_level.layer_metadata.tile_layout
        return (
            (extent.xmax - extent.xmin) / (currentTileLayout.tileCols * currentTileLayout.layoutCols),
            (extent.ymax - extent.ymin) / (currentTileLayout.tileRows * currentTileLayout.layoutRows)
        )

    def calculate_layer_size_in_pixels(self) -> Tuple[float, float]:
        """
        The (width,height) of the layer in pixels. Does not take into account bands.
        Just a division, and is not rounded on an integer.
        """
        layout_cellsize = self.get_cellsize()
        max_level = self.get_max_level()
        layer_extent = max_level.layer_metadata.extent
        layer_metadata_size = (
            layer_extent.xmax - layer_extent.xmin,
            layer_extent.ymax - layer_extent.ymin
        )
        layer_size_in_pixels = (
            layer_metadata_size[0] / layout_cellsize[0],
            layer_metadata_size[1] / layout_cellsize[1]
        )
        return layer_size_in_pixels

    @staticmethod
    def _layout_for_resolution(extent, currentTileLayout, projection, target_resolution):
        currentTileCols = currentTileLayout.tileCols
        currentTileRows = currentTileLayout.tileRows

        width = extent.xmax - extent.xmin
        height = extent.ymax - extent.ymin

        currentResolutionX = width / (currentTileCols * currentTileLayout.layoutCols)
        currentResolutionY = width / (currentTileRows * currentTileLayout.layoutRows)
        if projection == None and abs(currentResolutionX - target_resolution) / target_resolution < 0.00001:
            _log.info(f"Resampling datacube not necessary, resolution already at {target_resolution}")
            return None

        newPixelCountX = math.ceil(width /target_resolution)
        newPixelCountY = math.ceil(height / target_resolution)

        #keep tile cols constant
        nbTilesX = math.ceil(newPixelCountX / currentTileCols)
        nbTilesY = math.ceil(newPixelCountY / currentTileRows)

        newWidth = nbTilesX*currentTileCols*target_resolution
        newHeight = nbTilesY * currentTileRows * target_resolution

        newExtent = Extent(extent.xmin, extent.ymin,extent.xmin+newWidth,extent.ymin+newHeight)

        return gps.LayoutDefinition(extent=newExtent, tileLayout=gps.TileLayout(int(nbTilesX), int(nbTilesY),
                                                                             int(currentTileCols), int(currentTileRows)))
    @staticmethod
    def _get_resample_method( method):
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
        # TODO #421 drop old unsued "point timeseries" feature
        max_level = self.get_max_level()
        transformer = pyproj.Transformer.from_crs(pyproj.crs.CRS(init=srs), max_level.layer_metadata.crs)
        (x_layer, y_layer) = transformer.transform(x, y)
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
                _log.error("unexpected value {v}")

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
            return DelayedVector(temp_file.name).to_driver_vector_cube()

    def get_max_level(self) -> TiledRasterLayer:
        return self.pyramid.levels[self.pyramid.max_zoom]

    @callsite
    def aggregate_spatial(self, geometries: Union[str, BaseGeometry, DriverVectorCube], reducer,
                          target_dimension: str = "result") -> Union[AggregatePolygonResult,
                                                                     AggregateSpatialVectorCube]:

        if isinstance(reducer, dict) and len(reducer) > 0:
            single_process = next(iter(reducer.values())).get('process_id')
            if len(reducer) == 1 and  single_process == 'histogram':
                #TODO: can be removed when histogram is deprecated?
                return self.zonal_statistics(geometries, single_process)
            else:
                visitor = GeotrellisTileProcessGraphVisitor(_builder=get_jvm().org.openeo.geotrellis.aggregate_polygon.SparkAggregateScriptBuilder()).accept_process_graph(reducer)
                return self.zonal_statistics(geometries, visitor.builder)

        raise OpenEOApiException(
                message=f"Reducer {reducer} is not supported in aggregate_spatial",
                code="ReducerUnsupported", status_code=400
            )

    def zonal_statistics(
        self, regions: Union[BaseGeometry, DriverVectorCube], func
    ) -> Union[
        # TODO simplify these return options https://github.com/Open-EO/openeo-python-driver/issues/149
        AggregatePolygonResult,
        AggregateSpatialVectorCube,
        AggregateSpatialResultCSV,
    ]:
        # TODO: rename to aggregate_spatial?
        # TODO eliminate code duplication
        _log.info("zonal_statistics with {f!r}, {r}".format(f=func, r=type(regions)))

        def insert_timezone(instant):
            return instant.replace(tzinfo=pytz.UTC) if instant.tzinfo is None else instant

        if isinstance(regions, (Polygon, MultiPolygon)):
            # TODO: GeometryCollection usage is deprecated
            regions = GeometryCollection([regions])
        projected_polygons = to_projected_polygons(get_jvm(), regions, none_for_points=True)

        def regions_to_wkt(regions: Union[BaseGeometry, DriverVectorCube]) -> List[str]:
            if isinstance(regions, BaseMultipartGeometry):
                return [str(g) for g in regions.geoms]
            elif isinstance(regions, BaseGeometry):
                return [str(regions)]
            elif isinstance(regions, DriverVectorCube):
                return regions.to_wkt()
            else:
                raise ValueError(regions)

        highest_level = self.get_max_level()
        scala_data_cube = highest_level.srdd.rdd()
        layer_metadata = highest_level.layer_metadata

        bandNames = ["band_unnamed"]
        if self.metadata.has_band_dimension():
            bandNames = self.metadata.band_names

        wrapped = get_jvm().org.openeo.geotrellis.OpenEOProcesses().wrapCube(scala_data_cube)
        wrapped.openEOMetadata().setBandNames(bandNames)

        if self._is_spatial():
            geometry_wkts = regions_to_wkt(regions)
            geometries_srs = "EPSG:4326"

            temp_dir = temp_csv_dir(message=f"{type(self).__name__}.zonal_statistics (spatial)")

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

            if projected_polygons:
                # TODO also add dumping results first to temp json file like with "mean"
                if func == 'histogram':
                    # TODO: This code path is likely never used.
                    stats = self._compute_stats_geotrellis().compute_histograms_time_series_from_datacube(
                        scala_data_cube, projected_polygons, from_date.isoformat(), to_date.isoformat(), 0
                    )
                    timeseries = self._as_python(stats)
                else:
                    temp_dir = temp_csv_dir(
                        message=f"{type(self).__name__}.zonal_statistics (projected_polygons)"
                    )
                    # TODO: We lose band names while these can be useful in for example "vector_to_raster".
                    self._compute_stats_geotrellis().compute_generic_timeseries_from_datacube(
                        func,
                        wrapped,
                        projected_polygons,
                        temp_dir,
                    )
                    return AggregateSpatialResultCSV(
                        temp_dir, regions=regions, metadata=self.metadata
                    )
            else:
                geometry_wkts = regions_to_wkt(regions)
                geometries_srs = "EPSG:4326"

                temp_dir = temp_csv_dir(
                    message=f"{type(self).__name__}.zonal_statistics (no projected_polygons)"
                )
                self._compute_stats_geotrellis().compute_generic_timeseries_from_datacube(
                    func,
                    wrapped,
                    geometry_wkts,
                    geometries_srs,
                    temp_dir,
                )

                return AggregateSpatialResultCSV(
                    temp_dir, regions=regions, metadata=self.metadata
                )

            return AggregatePolygonResult(
                timeseries=timeseries,
                # TODO: regions can also be a string (path to vector file) instead of geometry object
                regions=regions,
                metadata=self.metadata
            )

    def _compute_stats_geotrellis(self):
        accumulo_instance_name = 'hdp-accumulo-instance'
        return get_jvm().org.openeo.geotrellis.ComputeStatsGeotrellisAdapter(self._zookeepers(), accumulo_instance_name)

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

    @callsite
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
        bucket = str(get_backend_config().s3_bucket_name)
        filename = str(filename)
        directory = str(pathlib.Path(filename).parent)
        s3_filename = "s3://{b}{f}".format(b=bucket, f=filename)
        s3_directory = "s3://{b}{d}".format(b=bucket, d=directory)
        format = format.upper()
        format_options = format_options or {}
        strict_cropping = format_options.get("strict_cropping", True)
        #geotiffs = self.rdd.merge().to_geotiff_rdd(compression=gps.Compression.DEFLATE_COMPRESSION).collect()

        # get the data at highest resolution
        max_level = self.get_max_level()

        req_id_key = get_jvm().org.openeo.logging.JsonLayout.RequestId()
        req_id = gps.get_spark_context().getLocalProperty(req_id_key)
        if req_id is not None:
            gps.get_spark_context().setJobGroup(req_id,f"save_result {req_id}",interruptOnCancel=True)

        def to_latlng_bbox(bbox: "Extent") -> Tuple[float, float, float, float]:
            latlng_extent = self._reproject_extent(
                src_crs=max_level.layer_metadata.crs,
                dst_crs="EPSG:4326",
                xmin=bbox.xmin(),
                ymin=bbox.ymin(),
                xmax=bbox.xmax(),
                ymax=bbox.ymax(),
            )

            return latlng_extent.xmin, latlng_extent.ymin, latlng_extent.xmax, latlng_extent.ymax

        def return_netcdf_assets(asset_paths, bands, nodata):
            assets = {}
            for asset in asset_paths:
                if isinstance(asset, str):  # TODO: for backwards compatibility, remove eventually (#646)
                    path = asset
                    extent = None
                else:
                    path = asset._1()
                    extent = asset._2()
                name = os.path.basename(path)
                assets[name] = {
                    "href": str(path),
                    "type": "application/x-netcdf",
                    "roles": ["data"],
                    "bbox": to_latlng_bbox(extent) if extent else None,
                    "geometry": mapping(Polygon.from_bounds(*to_latlng_bbox(extent))) if extent else None,
                    "nodata": nodata,
                }
                if bands is not None:
                    assets[name]["bands"] = bands
            return assets

        if self.metadata.spatial_extent and strict_cropping:
            bbox = self.metadata.spatial_extent
            crs = bbox.get("crs") or "EPSG:4326"
            if isinstance(crs, int):
                crs = "EPSG:%d" % crs
            crop_bounds = self._reproject_extent(
                src_crs=CRS.from_user_input(crs), dst_crs=max_level.layer_metadata.crs,
                xmin=bbox["west"], ymin=bbox["south"], xmax=bbox["east"], ymax=bbox["north"]
            )
            crop_extent = get_jvm().geotrellis.vector.Extent(crop_bounds.xmin, crop_bounds.ymin,
                                                                   crop_bounds.xmax, crop_bounds.ymax)
        else:
            crop_bounds = None
            crop_extent = None


        _log.info(f"save_result format {format} with bounds {crop_bounds} and options {format_options}")
        if self.metadata.temporal_extent:
            date_from, date_to = self.metadata.temporal_extent
            crop_dates = (pd.Timestamp(date_from), pd.Timestamp(date_to))
        else:
            crop_dates = None

        tiled = format_options.get("tiled", False)
        stitch = format_options.get("stitch", False)
        catalog = format_options.get("parameters", {}).get("catalog", False)
        tile_grid = format_options.get("tile_grid", None)
        sample_by_feature = format_options.get("sample_by_feature", False)
        feature_id_property = format_options.get("feature_id_property", None)
        batch_mode = format_options.get("batch_mode", False)
        overviews = format_options.get("overviews", "AUTO")
        overview_resample = format_options.get("overview_method", "near")
        colormap = format_options.get("colormap", None)
        description = format_options.get("file_metadata",{}).get("description","")
        filename_prefix = get_jvm().scala.Option.apply(format_options.get("filename_prefix", None))
        separate_asset_per_band = get_jvm().scala.Option.apply(format_options.get("separate_asset_per_band", None))

        if separate_asset_per_band.isDefined() and format != "GTIFF":
            raise OpenEOApiException("separate_asset_per_band is only supported with format GTIFF")

        save_filename = s3_filename if batch_mode and ConfigParams().is_kube_deploy and not get_backend_config().fuse_mount_batchjob_s3_bucket else filename
        save_directory = s3_directory if batch_mode and ConfigParams().is_kube_deploy and not get_backend_config().fuse_mount_batchjob_s3_bucket else directory

        if format in ["GTIFF", "PNG"]:
            def get_color_cmap():
                if (colormap is not None):
                    def color_to_int(color):
                        if isinstance(color, int):
                            return color
                        elif isinstance(color, list):
                            # Convert e.g. [0.5,0.1,0.2,0.5] to 2132358015.
                            # By multiplying each with 255 and interpreting them together
                            # as a 32-bit unsigned long in big-endian.
                            import struct
                            import builtins
                            color_as_int = struct.unpack('>L',
                                                         bytes(map(lambda x: builtins.int(x * 255), color)))[0]
                            return color_as_int

                    converted_colors = {float(k): color_to_int(v) for k, v in colormap.items()}
                    gpsColormap = gps.ColorMap.build(breaks=converted_colors)
                    return gpsColormap.cmap
                return None

            if max_level.layer_type != gps.LayerType.SPATIAL and (not batch_mode or catalog or stitch or format=="PNG") :
                max_level = max_level.to_spatial_layer()

            if format == "GTIFF":
                zlevel = format_options.get("ZLEVEL",6)
                if catalog:
                    _log.info("save_result (catalog) save_on_executors")
                    self._save_on_executors(max_level, filename, filename_prefix=filename_prefix)
                elif stitch:
                    if tile_grid:
                        _log.info("save_result save_stitched_tile_grid")
                        tiles = self._save_stitched_tile_grid(max_level, save_filename, tile_grid, crop_bounds,
                                                              zlevel=zlevel, filename_prefix=filename_prefix)

                        # noinspection PyProtectedMember
                        return {str(pathlib.Path(tile._1()).name): {
                            "href": tile._1(),
                            "bbox": to_latlng_bbox(tile._2()),
                            "geometry": mapping(Polygon.from_bounds(*to_latlng_bbox(tile._2()))),
                            "type": "image/tiff; application=geotiff",
                            "roles": ["data"]
                        } for tile in tiles}
                    else:
                        _log.info("save_result save_stitched")
                        bbox = self._save_stitched(max_level, save_filename, crop_bounds, zlevel=zlevel)
                        return {str(pathlib.Path(filename).name): {
                            "href": save_filename,
                            "bbox": to_latlng_bbox(bbox),
                            "geometry": mapping(Polygon.from_bounds(*to_latlng_bbox(bbox))),
                            "type": "image/tiff; application=geotiff",
                            "roles": ["data"]
                        }}
                else:
                    _log.info("save_result: saveRDD")
                    gtiff_options = get_jvm().org.openeo.geotrellis.geotiff.GTiffOptions()
                    if filename_prefix.isDefined():
                        gtiff_options.setFilenamePrefix(filename_prefix.get())
                    if separate_asset_per_band.isDefined():
                        gtiff_options.setSeparateAssetPerBand(separate_asset_per_band.get())
                    gtiff_options.addHeadTag("PROCESSING_SOFTWARE",softwareversion)
                    if description != "":
                        gtiff_options.addHeadTag("ImageDescription", description)
                    gtiff_options.setResampleMethod(overview_resample)
                    getattr(gtiff_options, "overviews_$eq")(overviews)
                    color_cmap = get_color_cmap()
                    if color_cmap is not None:
                        gtiff_options.setColorMap(color_cmap)
                    band_count = -1
                    if self.metadata.has_band_dimension():
                        band_count = len(self.metadata.band_dimension.band_names)
                        for index, band_name in enumerate(self.metadata.band_dimension.band_names):
                            gtiff_options.addBandTag(index, "DESCRIPTION", str(band_name))

                    bands = []
                    if self.metadata.has_band_dimension():
                        bands = [b._asdict() for b in self.metadata.bands]
                    nodata = max_level.layer_metadata.no_data_value

                    max_level_rdd = max_level.srdd.rdd()

                    if tile_grid:
                        if separate_asset_per_band.isDefined():
                            raise OpenEOApiException(message="separate_asset_per_band is not supported with tile_grid")

                    if batch_mode and max_level.layer_type != gps.LayerType.SPATIAL:
                        compression = get_jvm().geotrellis.raster.io.geotiff.compression.DeflateCompression(
                            zlevel)

                        band_indices_per_file = None
                        if tile_grid:
                            timestamped_paths = (get_jvm()
                                .org.openeo.geotrellis.geotiff.package.saveStitchedTileGridTemporal(
                                max_level_rdd, save_directory, tile_grid, compression, filename_prefix))
                        elif sample_by_feature:
                            if separate_asset_per_band.isDefined():
                                raise OpenEOApiException(
                                    message="separate_asset_per_band is not supported with sample_by_feature"
                                )
                            # EP-3874 user requests to output data by polygon
                            _log.info("Output one tiff file per feature and timestamp.")
                            geometries = format_options['geometries']
                            if isinstance(geometries, MultiPolygon):
                                geometries = GeometryCollection(geometries.geoms)
                            projected_polygons = to_projected_polygons(get_jvm(), geometries)
                            labels = self.get_labels(geometries,feature_id_property)
                            timestamped_paths = get_jvm().org.openeo.geotrellis.geotiff.package.saveSamples(
                                max_level_rdd, save_directory, projected_polygons, labels, compression,
                                filename_prefix)
                        else:
                            timestamped_paths = (
                                get_jvm().org.openeo.geotrellis.geotiff.package.saveRDDTemporalAllowAssetPerBand(
                                    max_level_rdd,
                                    save_directory,
                                    zlevel,
                                    get_jvm().scala.Option.apply(crop_extent),
                                    gtiff_options,
                                )
                            )
                            band_indices_per_file = [tup._4() for tup in timestamped_paths]

                        assets = {}

                        # noinspection PyProtectedMember
                        # TODO: contains a bbox so rename
                        timestamped_paths = [(timestamped_path._1(), timestamped_path._2(), timestamped_path._3())
                                             for timestamped_path in timestamped_paths]
                        for index, tup in enumerate(timestamped_paths):
                            path, timestamp, bbox = tup
                            tmp_bands = bands
                            if band_indices_per_file:
                                band_indices = band_indices_per_file[index]
                                tmp_bands = [b for i, b in enumerate(bands) if i in band_indices]
                            assets[str(pathlib.Path(path).name)] = {
                                "href": str(path),
                                "type": "image/tiff; application=geotiff",
                                "roles": ["data"],
                                "bands": tmp_bands,
                                "nodata": nodata,
                                "datetime": timestamp,
                                "bbox": to_latlng_bbox(bbox),
                                "geometry": mapping(Polygon.from_bounds(*to_latlng_bbox(bbox))),
                            }
                        return assets
                    else:
                        if tile_grid:
                            tiles = self._save_stitched_tile_grid(max_level, str(save_filename), tile_grid, crop_bounds,
                                                                  zlevel=zlevel, filename_prefix=filename_prefix)

                            # noinspection PyProtectedMember
                            return {str(pathlib.Path(tile._1()).name): {
                                "href": tile._1(),
                                "bbox": to_latlng_bbox(tile._2()),
                                "geometry": mapping(Polygon.from_bounds(*to_latlng_bbox(tile._2()))),
                                "type": "image/tiff; application=geotiff",
                                "roles": ["data"]
                            } for tile in tiles}
                        else:
                            paths_tuples = get_jvm().org.openeo.geotrellis.geotiff.package.saveRDDAllowAssetPerBand(
                                max_level_rdd,
                                band_count,
                                str(save_filename),
                                zlevel,
                                get_jvm().scala.Option.apply(crop_extent),
                                gtiff_options)

                            paths_tuples = [
                                (timestamped_path._1(), timestamped_path._2()) for timestamped_path in paths_tuples
                            ]
                            assets = {}
                            for path, band_indices in paths_tuples:
                                file_name = pathlib.Path(path).name
                                tmp_bands = [b for i, b in enumerate(bands) if i in band_indices]
                                assets[file_name] = {
                                    "href": str(path),
                                    "type": "image/tiff; application=geotiff",
                                    "roles": ["data"],
                                    "bands": tmp_bands,
                                    "nodata": nodata,
                                }
                            return assets

            else:
                if not save_filename.endswith(".png"):
                    save_filename = save_filename + ".png"

                png_options = get_jvm().org.openeo.geotrellis.png.PngOptions()
                color_cmap = get_color_cmap()
                if color_cmap is not None:
                    png_options.setColorMap(color_cmap)
                if crop_bounds:
                    crop_extent = get_jvm().geotrellis.vector.Extent(crop_bounds.xmin, crop_bounds.ymin, crop_bounds.xmax, crop_bounds.ymax)
                    get_jvm().org.openeo.geotrellis.png.package.saveStitched(max_level.srdd.rdd(), save_filename, crop_extent, png_options)
                else:
                    get_jvm().org.openeo.geotrellis.png.package.saveStitched(max_level.srdd.rdd(), save_filename, png_options)
                return {
                    str(pathlib.Path(save_filename).name): {
                        "href": save_filename,
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
                projected_polygons = to_projected_polygons(get_jvm(), geometries)
                labels = self.get_labels(geometries,feature_id_property)
                if(max_level.layer_type != gps.LayerType.SPATIAL):
                    _log.debug(f"projected_polygons carries {len(projected_polygons.polygons())} polygons")
                    asset_paths = get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSamples(
                        max_level.srdd.rdd(),
                        save_directory,
                        projected_polygons,
                        labels,
                        band_names,
                        dim_names,
                        global_metadata,
                        filename_prefix,
                    )
                else:
                    asset_paths = get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSamplesSpatial(
                        max_level.srdd.rdd(),
                        save_directory,
                        projected_polygons,
                        labels,
                        band_names,
                        dim_names,
                        global_metadata,
                        filename_prefix,
                    )

                return return_netcdf_assets(asset_paths, bands, nodata)
            else:
                originalName = pathlib.Path(filename)
                filename_tmp = "openEO.nc" if originalName.name == "out" else originalName.name
                if not stitch:
                    filename = save_directory + "/" + filename_tmp
                    if strict_cropping:
                        options = get_jvm().org.openeo.geotrellis.netcdf.NetCDFOptions()
                        options.setBandNames(band_names)
                        options.setDimensionNames(dim_names)
                        options.setAttributes(global_metadata)
                        options.setZLevel(zlevel)
                        options.setCropBounds(crop_extent)
                        asset_paths = get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.writeRasters(
                            max_level.srdd.rdd(),
                            filename,options
                        )
                    else:
                        if(max_level.layer_type != gps.LayerType.SPATIAL):
                            asset_paths = get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSingleNetCDF(max_level.srdd.rdd(),
                                filename,
                                band_names,
                                dim_names,global_metadata,zlevel
                            )
                        else:
                            asset_paths = get_jvm().org.openeo.geotrellis.netcdf.NetCDFRDDWriter.saveSingleNetCDFSpatial(
                                max_level.srdd.rdd(),
                                filename,
                                band_names,
                                dim_names, global_metadata, zlevel
                                )
                    return return_netcdf_assets(asset_paths, bands, nodata)

                else:
                    if not tiled:
                        result=self._collect_as_xarray(max_level, crop_bounds, crop_dates)
                    else:
                        result=self._collect_as_xarray(max_level)


                    # if batch_mode:
                    #     filename = directory +  "/openEO.nc"
                    XarrayIO.to_netcdf_file(array=result, path=filename)
                    if batch_mode:
                        asset = {
                            "href": filename,
                            "roles": ["data"],
                            "type": "application/x-netcdf"
                        }
                        if bands is not None:
                            asset["bands"] = bands
                        return {filename_tmp: asset}

        elif format == "JSON":
            # saving to json, this is potentially big in memory
            # get result as xarray
            if not tiled:
                result=self._collect_as_xarray(max_level, crop_bounds, crop_dates)
            else:
                result = self._collect_as_xarray(max_level)

            XarrayIO.to_json_file(array=result, path=filename)

        else:
            raise OpenEOApiException(
                message="Format {f!r} is not supported".format(f=format),
                code="FormatUnsupported", status_code=400
            )
        return {str(os.path.basename(filename)):{"href":filename}}

    def get_labels(self, geometries, feature_id_property=None):
        # TODO: return more descriptive labels/ids than these autoincrement strings (when possible)?
        if isinstance(geometries, DelayedVector):
            return [str(i) for i, _ in enumerate(geometries.geometries)]
        elif isinstance(geometries, DriverVectorCube):
            if feature_id_property is not None:
                values = geometries.get_band_values(feature_id_property)
                if values is not None:
                    return [str(v) for v in values]
                else:
                    _log.warning(f"save_result: a feature_id_property '{feature_id_property}' was specified, but could not find  labels in the vector cube: {geometries}.")

            return [str(i) for i in range(geometries.geometry_count())]
        elif isinstance(geometries, collections.abc.Sized):
            return [str(x) for x in range(len(geometries))]
        else:
            _log.warning(f"get_labels: unhandled geometries type {type(geometries)}")
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
            xmin = rdd.layer_metadata.bounds.minKey.col
            xmax = rdd.layer_metadata.bounds.maxKey.col + 1
            ymin = rdd.layer_metadata.bounds.minKey.row
            ymax = rdd.layer_metadata.bounds.maxKey.row + 1
            crop_win = (
                xmin * layout_pix.tileCols,
                ymin * layout_pix.tileRows,
                (xmax - xmin) * layout_pix.tileCols,
                (ymax - ymin) * layout_pix.tileRows,
            )
        crop_dim = (
            layout_dim[0] + crop_win[0] * xres,
            layout_dim[1] + crop_win[1] * yres,
            crop_win[2] * xres,
            crop_win[3] * yres,
        )

        # build metadata for the xarrays
        # coordinates are in the order of t,bands,x,y
        dims=[]
        coords={}
        has_time=self.metadata.has_temporal_dimension()
        if has_time:
            dims.append('t')
        has_bands=self.metadata.has_band_dimension()
        if has_bands:
            dims.append("bands")
            coords["bands"] = self.metadata.band_names
        dims.append("x")
        coords["x"] = np.linspace(crop_dim[0] + 0.5 * xres, crop_dim[0] + crop_dim[2] - 0.5 * xres, crop_win[2])
        dims.append("y")
        coords["y"] = np.linspace(crop_dim[1] + 0.5 * yres, crop_dim[1] + crop_dim[3] - 0.5 * yres, crop_win[3])

        def stitch_at_time(crop_win, layout_win, tiles):
            # value expected to be another tuple with the original spacetime key and the array
            subarrs = list(tiles)

            # get block sizes
            bw, bh = subarrs[0][1].cells.shape[-2:]
            bbands = sum(subarrs[0][1].cells.shape[:-2]) if len(subarrs[0][1].cells.shape) > 2 else 1
            wbind = np.arange(0, bbands)
            dtype = subarrs[0][1].cells.dtype
            nodata = subarrs[0][1].no_data_value

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
                nyblk = int(layout_win[3] / bh) - 1
                subarrs = list(map(lambda t: (SpatialKey(t[0].col, nyblk - t[0].row), t[1]), subarrs))
                tp = (0, 2, 1)

            # loop over blocks and merge into
            for iblk in subarrs:
                iwin=(iblk[0].col*bw, iblk[0].row*bh, bw, bh)
                iarr=iblk[1].cells
                iarr=iarr.reshape((-1,bh,bw)).transpose(tp)
                ixind=np.arange(iwin[0],iwin[0]+iwin[2])
                iyind=np.arange(iwin[1],iwin[1]+iwin[3])
                if switch_topleft:
                    iyind = iyind[::-1]
                xoverlap = np.intersect1d(wxind, ixind, True, True)
                yoverlap = np.intersect1d(wyind, iyind, True, True)
                if len(xoverlap[1]) > 0 and len(yoverlap[1] > 0):
                    window[np.ix_(wbind, xoverlap[1], yoverlap[1])] = iarr[np.ix_(wbind, xoverlap[2], yoverlap[2])]

            # return date (or None) - window tuple
            return window

        # at every date stitch together the layer, still on the workers
        # mapped=list(map(lambda t: (t[0].row,t[0].col),rdd.to_numpy_rdd().collect())); min(mapped); max(mapped)
        collection = (
            rdd.to_numpy_rdd()
            .map(lambda t: (t[0].instant if has_time else None, (t[0], t[1])))
            .groupByKey()
            .mapValues(partial(stitch_at_time, crop_win, layout_win))
            .collect()
        )

        # only for debugging on driver, do not use in production
        #         collection=rdd\
        #             .to_numpy_rdd()\
        #             .filter(lambda t: (t[0].instant>=crop_dates[0] and t[0].instant<=crop_dates[1]) if has_time else True)\
        #             .map(lambda t: (t[0].instant if has_time else None, (t[0], t[1])))\
        #             .groupByKey()\
        #             .collect()
        #         collection=list(map(partial(stitch_at_time, crop_win, layout_win),collection))

        if len(collection) == 0:
            return xr.DataArray(
                np.full([0] * len(dims), 0), dims=dims, coords=dict(map(lambda k: (k[0], []), coords.items()))
            )

        if len(collection) > 1:
            collection.sort(key=lambda i: i[0])

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
            result = xr.DataArray(collection[0][1], dims=dims, coords=coords)

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
        result.coords["spatial_ref"] = 0
        result.coords["spatial_ref"].attrs["spatial_ref"] = projCRS.to_wkt()
        result.coords["spatial_ref"].attrs["crs_wkt"] = projCRS.to_wkt()
        result.attrs["grid_mapping"] = "spatial_ref"

        return result

    @classmethod
    def _reproject_extent(cls, src_crs, dst_crs, xmin, ymin, xmax, ymax):
        src_proj = pyproj.Proj(src_crs)
        dst_proj = pyproj.Proj(dst_crs)

        def reproject_point(x, y):
            transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
            return transformer.transform(x, y)

        reprojected_xmin1, reprojected_ymin1 = reproject_point(xmin, ymin)
        reprojected_xmax1, reprojected_ymax1 = reproject_point(xmax, ymax)
        reprojected_xmin2, reprojected_ymax2 = reproject_point(xmin, ymax)
        reprojected_xmax2, reprojected_ymin2 = reproject_point(xmax, ymin)
        crop_bounds = \
            Extent(xmin=min(reprojected_xmin1,reprojected_xmin2), ymin=min(reprojected_ymin1,reprojected_ymin2),
                   xmax=max(reprojected_xmax1,reprojected_xmax2), ymax=max(reprojected_ymax1,reprojected_ymax2))
        return crop_bounds

    def _save_on_executors(self, spatial_rdd: gps.TiledRasterLayer, path, zlevel=6,
                           filename_prefix=None):
        geotiff_rdd = spatial_rdd.to_geotiff_rdd(
            storage_method=gps.StorageMethod.TILED,
            compression=gps.Compression.DEFLATE_COMPRESSION
        )

        basedir = pathlib.Path(str(path) + '.catalogresult')
        basedir.mkdir(parents=True, exist_ok=True)

        pre = ""
        if filename_prefix and filename_prefix.isDefined():
            pre = filename_prefix.get() + "_"

        def write_tiff(item):
            key, data = item
            tiffPath = basedir / (pre + '{c}-{r}.tiff'.format(c=key.col, r=key.row))
            with tiffPath.open('wb') as f:
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
        jvm = get_jvm()

        max_compression = jvm.geotrellis.raster.io.geotiff.compression.DeflateCompression(zlevel)

        if crop_bounds:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitched(spatial_rdd.srdd.rdd(), path, crop_bounds._asdict(),
                                                                   max_compression)
        else:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitched(spatial_rdd.srdd.rdd(), path, max_compression)

    def _save_stitched_tile_grid(self, spatial_rdd, path, tile_grid, crop_bounds=None, zlevel=6,
                                 filename_prefix=None):
        jvm = get_jvm()

        if filename_prefix and filename_prefix.isDefined():
            p = pathlib.Path(path)
            ext = p.name[p.name.index("."):]
            path = str(p.parent / (filename_prefix.get() + ext))

        max_compression = jvm.geotrellis.raster.io.geotiff.compression.DeflateCompression(zlevel)

        if crop_bounds:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitchedTileGrid(spatial_rdd.srdd.rdd(), path,
                                                                                  tile_grid, crop_bounds._asdict(),
                                                                                  max_compression)
        else:
            return jvm.org.openeo.geotrellis.geotiff.package.saveStitchedTileGrid(spatial_rdd.srdd.rdd(), path,
                                                                                  tile_grid, max_compression)

    @callsite
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

        visitor = GeotrellisTileProcessGraphVisitor()

        return self.reduce_bands(visitor.accept_process_graph(reduce_graph))

    @callsite
    def atmospheric_correction(
        self,
        method: Optional[str] = None,
        elevation_model: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> "GeopysparkDataCube":
        supported_methods = ["ICOR", "SMAC"]
        supported_missons = ["SENTINEL2", "LANDSAT8"]

        method = method or supported_methods[0]
        elevation_model = elevation_model or "DEM"
        options = options or {}
        # TODO: smarter mission_id fallback?
        mission_id = options.get("mission_id") or supported_missons[0]
        append_debug_bands = bool(options.get("append_debug_bands"))

        def get_float(d: dict, key: str, default: float = np.NaN) -> float:
            value = d.get(key)
            if value is None:
                value = default
            return value

        sza = get_float(options, "sza")
        vza = get_float(options, "vza")
        raa = get_float(options, "raa")
        gnd = get_float(options, "gnd")
        aot = get_float(options, "aot")
        cwv = get_float(options, "cwv")

        bandIds = self.metadata.band_names
        _log.info(f"atmospheric_correction: {method=} {mission_id=} {bandIds=}")
        if method.upper() not in supported_methods:
            raise ProcessParameterInvalidException(
                parameter="method",
                process="atmospheric_correction",
                reason=f"Unsupported method {method}, should be one of {supported_methods}",
            )
        if mission_id.upper() not in supported_missons:
            raise ProcessParameterInvalidException(
                parameter="missionId",
                process="atmospheric_correction",
                reason=f"Unsupported mission id {mission_id}, should be one of {supported_missons}",
            )
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
                mission_id,
                append_debug_bands,
            )
        )
        return atmo_corrected

    def sar_backscatter(self, args: SarBackscatterArgs) -> 'GeopysparkDataCube':
        # Nothing to do: the actual SAR backscatter processing already happened in `load_collection`
        return self

    @callsite
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
