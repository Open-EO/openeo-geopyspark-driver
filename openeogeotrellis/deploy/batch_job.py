from dataclasses import dataclass
from itertools import chain

import json
import logging
import os
import shutil
import stat
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse
from copy import deepcopy

import pyproj
from osgeo import gdal
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext, SparkConf
from pyspark.profiler import BasicProfiler
from shapely.geometry import mapping, Polygon
from shapely.geometry.base import BaseGeometry

from openeo.util import ensure_dir, Rfc3339, TimingLogger, dict_no_none
from openeo_driver import ProcessGraphDeserializer
from openeo_driver.datacube import DriverDataCube, DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import (
    ImageCollectionResult,
    JSONResult,
    SaveResult,
    NullResult,
    MlModelResult,
)
from openeo_driver.users import User
from openeo_driver.util.geometry import spatial_extent_union, reproject_bounding_box
from openeo_driver.util.logging import (
    BatchJobLoggingFilter,
    get_logging_config,
    setup_logging,
    LOGGING_CONTEXT_BATCH_JOB,
)
from openeo_driver.util.utm import area_in_square_meters
from openeo_driver.utils import EvalEnv, temporal_extent_union
from openeogeotrellis._version import __version__
from openeogeotrellis.backend import JOB_METADATA_FILENAME, GeoPySparkBackendImplementation
from openeogeotrellis.collect_unique_process_ids_visitor import CollectUniqueProcessIdsVisitor
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.deploy import load_custom_processes, build_gps_backend_deploy_metadata
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import (
    kerberos,
    describe_path,
    log_memory,
    get_jvm,
    add_permissions,
    to_s3_url,
    get_s3_binary_file_contents,
)

logger = logging.getLogger('openeogeotrellis.deploy.batch_job')
user_facing_logger = logging.getLogger('openeo-user-log')


OPENEO_LOGGING_THRESHOLD = os.environ.get("OPENEO_LOGGING_THRESHOLD", "INFO")
OPENEO_BATCH_JOB_ID = os.environ.get("OPENEO_BATCH_JOB_ID", "unknown-job")
# TODO: also trim batch_job id a bit before logging?
BatchJobLoggingFilter.set("job_id", OPENEO_BATCH_JOB_ID)


def _setup_user_logging(log_file: Path) -> None:
    file_handler = None
    if str(log_file.name) == "stdout":
        file_handler = logging.StreamHandler(sys.stdout)
    else:
        file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setLevel(logging.ERROR)

    user_facing_logger.addHandler(file_handler)

    add_permissions(log_file, stat.S_IWGRP)


def _create_job_dir(job_dir: Path):
    logger.info("creating job dir {j!r} (parent dir: {p}))".format(j=job_dir, p=describe_path(job_dir.parent)))
    ensure_dir(job_dir)
    if not ConfigParams().is_kube_deploy:
        try:
            shutil.chown(job_dir, user=None, group='eodata')
        except LookupError as e:
            logger.warning(f"Could not change group of {job_dir} to eodata.")

    add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group


def _parse(job_specification_file: str) -> Dict:
    with open(job_specification_file, 'rt', encoding='utf-8') as f:
        job_specification = json.load(f)

    return job_specification


def extract_result_metadata(tracer: DryRunDataTracer) -> dict:
    logger.info("Extracting result metadata from {t!r}".format(t=tracer))

    rfc3339 = Rfc3339(propagate_none=True)

    source_constraints = tracer.get_source_constraints()

    # Take union of extents
    temporal_extent = temporal_extent_union(*[
        sc["temporal_extent"] for _, sc in source_constraints if "temporal_extent" in sc
    ])
    extents = [sc["spatial_extent"] for _, sc in source_constraints if "spatial_extent" in sc]
    # In the result metadata we want the bbox to be in EPSG:4326 (lat-long).
    # Therefore, keep track of the bbox's CRS to convert it to EPSG:4326 at the end, if needed.
    bbox_crs = None
    bbox = None
    geometry = None
    area = None
    if(len(extents) > 0):
        spatial_extent = spatial_extent_union(*extents)
        bbox_crs = spatial_extent["crs"]
        temp_bbox = [spatial_extent[b] for b in ["west", "south", "east", "north"]]
        if all(b is not None for b in temp_bbox):
            bbox = temp_bbox  # Only set bbox once we are sure we have all the info
            polygon = Polygon.from_bounds(*bbox)
            geometry = mapping(polygon)
            area = area_in_square_meters(polygon, bbox_crs)

    start_date, end_date = [rfc3339.datetime(d) for d in temporal_extent]

    aggregate_spatial_geometries = tracer.get_geometries()  # TODO: consider "filter_spatial" geometries too?
    if aggregate_spatial_geometries:
        if len(aggregate_spatial_geometries) > 1:
            logger.warning("Multiple aggregate_spatial geometries: {c}".format(c=len(aggregate_spatial_geometries)))
        agg_geometry = aggregate_spatial_geometries[0]
        if isinstance(agg_geometry, BaseGeometry):
            # We only allow EPSG:4326 for the BaseGeometry case to keep things simple
            # and prevent complicated problems with CRS transformations.
            # The aggregation geometry comes from Shapely, but Shapely itself does not
            # support coordinate system transformations.
            # See also: https://shapely.readthedocs.io/en/stable/manual.html#coordinate-systems
            bbox_crs = "EPSG:4326"
            bbox = agg_geometry.bounds
            geometry = mapping(agg_geometry)
            area = area_in_square_meters(agg_geometry, bbox_crs)
        elif isinstance(agg_geometry, DelayedVector):
            bbox = agg_geometry.bounds
            bbox_crs = agg_geometry.crs
            # Intentionally don't return the complete vector file. https://github.com/Open-EO/openeo-api/issues/339
            geometry = mapping(Polygon.from_bounds(*bbox))
            area = DriverVectorCube.from_fiona([agg_geometry.path]).get_area()
        elif isinstance(agg_geometry, DriverVectorCube):
            bbox = agg_geometry.get_bounding_box()
            bbox_crs = agg_geometry.get_crs()
            geometry = agg_geometry.get_bounding_box_geojson()
            area = agg_geometry.get_area()
        else:
            logger.warning(f"Result metadata: no bbox/area support for {type(agg_geometry)}")

        # The aggregation geometries return tuples for their bounding box.
        # Keep the end result consistent and convert it to a list.
        if isinstance(bbox, tuple):
            bbox = list(bbox)

    links = tracer.get_metadata_links()
    links = [link for k, v in links.items() for link in v]

    # Convert bbox to lat-long, EPSG:4326 if it was any other CRS.
    bbox = convert_bbox_to_lat_long(bbox, bbox_crs)

    # TODO: dedicated type?
    # TODO: match STAC format?
    return {
        "geometry": geometry,
        "bbox": bbox,
        "area": {"value": area, "unit": "square meter"} if area else None,
        "start_datetime": start_date,
        "end_datetime": end_date,
        "links": links,
    }


def convert_bbox_to_lat_long(bbox: List[int], bbox_crs: Optional[Union[str, int, pyproj.CRS]] = None) -> List[int]:
    """Convert bounding box to lat-long, i.e. EPSG:4326, if it was not EPSG:4326 already.

    :param bbox: the bounding box
    :param bbox_crs: in which CRS bbox is currently expressed.
    :return: the bounding box expressed in EPSG:4326
    """
    # Convert bbox to lat-long, EPSG:4326 if it was any other CRS.
    if bbox and bbox_crs not in [4326, "EPSG:4326", "epsg:4326"]:
        # Note that if the bbox comes from the aggregate_spatial_geometries, then we may
        # get a pyproy CRS object instead of an EPSG code. In that case it is OK to
        # just do the reprojection, even if it is already EPSG:4326. That's just a no-op.
        # In constrast, handling all possible variants of pyproj CRS objects that are actually
        # all the exact same EPSG:4326 CRS, is complex and unnecessary.
        latlon_spatial_extent = {
            "west": bbox[0],
            "south": bbox[1],
            "east": bbox[2],
            "north": bbox[3],
            "crs": bbox_crs,
        }
        latlon_spatial_extent = reproject_bounding_box(
            latlon_spatial_extent, from_crs=None, to_crs="EPSG:4326"
        )
        return [latlon_spatial_extent[b] for b in ["west", "south", "east", "north"]]

    return bbox


def _export_result_metadata(tracer: DryRunDataTracer, result: SaveResult, output_file: Path, metadata_file: Path,
                            unique_process_ids: Set[str], asset_metadata: Dict = None,
                            ml_model_metadata: Dict = None) -> None:
    metadata = extract_result_metadata(tracer)

    def epsg_code(gps_crs) -> Optional[int]:
        crs = get_jvm().geopyspark.geotrellis.TileLayer.getCRS(gps_crs)
        return crs.get().epsgCode().getOrElse(None) if crs.isDefined() else None

    bands = []
    if isinstance(result, GeopysparkDataCube):
        if result.cube.metadata.has_band_dimension():
            bands = result.metadata.bands
        max_level = result.pyramid.levels[result.pyramid.max_zoom]
        nodata = max_level.layer_metadata.no_data_value
        epsg = epsg_code(max_level.layer_metadata.crs)
        instruments = result.metadata.get("summaries", "instruments", default=[])
    elif isinstance(result, ImageCollectionResult) and isinstance(result.cube, GeopysparkDataCube):
        if result.cube.metadata.has_band_dimension():
            bands = result.cube.metadata.bands
        max_level = result.cube.pyramid.levels[result.cube.pyramid.max_zoom]
        nodata = max_level.layer_metadata.no_data_value
        epsg = epsg_code(max_level.layer_metadata.crs)
        instruments = result.cube.metadata.get("summaries", "instruments", default=[])
    else:
        bands = []
        nodata = None
        epsg = None
        instruments = []

    if not isinstance(result, NullResult):
        if asset_metadata is None:
            # Old approach: need to construct metadata ourselves, from inspecting SaveResult
            metadata['assets'] = {
                output_file.name: {
                    'bands': bands,
                    'nodata': nodata,
                    'type': result.get_mimetype()
                }
            }
        else:
            # New approach: SaveResult has generated metadata already for us
            try:
                _extract_asset_metadata(
                    job_result_metadata=metadata, asset_metadata=asset_metadata, job_dir=output_file.parent, epsg=epsg
                )
            except Exception as e:
                error_summary = GeoPySparkBackendImplementation.summarize_exception_static(e)
                user_facing_logger.exception("Error while creating asset metadata: " + error_summary.summary)


    # _extract_asset_metadata may already fill in metadata["epsg"], but only
    # if the value of epsg was None. So we don't want to overwrite it with
    # None here.
    # TODO: would be better to eliminate this complication.
    if "epsg" not in metadata:
        metadata["epsg"] = epsg

    metadata["instruments"] = instruments
    metadata["processing:facility"] = "VITO - SPARK"  # TODO make configurable
    metadata["processing:software"] = "openeo-geotrellis-" + __version__
    metadata["unique_process_ids"] = list(unique_process_ids)
    global_metadata = result.options.get("file_metadata",{})
    metadata["providers"] = global_metadata.get("providers",[])
    metadata["processing:expression"] = global_metadata.get("processing:expression", {})
    metadata = {**metadata, **_get_tracker_metadata("")}

    if ml_model_metadata is not None:
        metadata['ml_model_metadata'] = ml_model_metadata

    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)

    add_permissions(metadata_file, stat.S_IWGRP)

    logger.info("wrote metadata to %s" % metadata_file)


GDALInfo = Dict[str, Any]
"""Output from GDAL.Info.

Type alias used as a helper type for the read projection metadata functions.
"""


ProjectionMetadata = Dict[str, Any]
"""Projection metadata retrieved about the raster file, compatible with STAC.

Type alias used as a helper type for the read projection metadata functions.
"""


@dataclass
class BandStatistics:
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    valid_percent: Optional[float] = None

    def to_dict(self):
        return {
            "minimum": self.minimum,
            "maximum": self.maximum,
            "mean": self.mean,
            "stddev": self.stddev,
            "valid_percent": self.valid_percent,
        }


RasterStatistics = Dict[str, BandStatistics]
"""
Maps a the bands in a raster to their band statistics, by the band name (str).

If the band name can not be found in the gdalinfo output, then the name will
be the band's number, as a string for consistency, and starting to count from 1,
so that would be: "1", "2", etc.
"""


@dataclass
class AssetRasterMetadata:
    """Metadata about the projection and raster statistics or a single raster asset.

    The raster is one file but that file may contain several bands.
    - The projection will be for the whole file.
    - The statistics will be per band.
    """

    gdal_info: GDALInfo
    projection: Optional[ProjectionMetadata] = None
    statistics: Optional[RasterStatistics] = None
    could_not_read_file: bool = False

    def to_dict(self) -> dict:
        if self.could_not_read_file:
            return {}

        result = dict_no_none(self.projection or {})

        if self.statistics is not None:
            raster_bands = []
            for band, stats in self.statistics.items():
                raster_bands.append({"name": band, "statistics": dict_no_none(stats.to_dict())})
            result["raster:bands"] = raster_bands

        return result


def _extract_asset_metadata(
    job_result_metadata: Dict[str, Any],
    asset_metadata: Dict[str, Any],
    job_dir: Path,
    epsg: int,
):
    """Extract the STAC metadata we need from a raster asset.

    :param job_result_metadata:
        The job result metadata that was already extracted and needs to be completed
    :param asset_metadata:
        The asset metadata extracted by `_extract_asset_raster_metadata`
        see: `_extract_asset_raster_metadata`
    :param job_dir:
        The path where the job's metadata and result metadata is saved.
    :param epsg:
        Used to detect if an epsg code was already extracted before because
        in that case we should not overwrite it.
        (This is only relevant if all assets have the same epsg and it would be save at
        the item level.)
    """
    raster_metadata, is_some_raster_md_missing = _extract_asset_raster_metadata(asset_metadata, job_dir)

    # Determine if projection metadata should be store at the item level,
    # because they are the same for all assets.
    epsgs = _make_set_for_key(raster_metadata, "proj:epsg")
    same_epsg_all_assets = len(epsgs) == 1

    bboxes = _make_set_for_key(raster_metadata, "proj:bbox", tuple)
    same_bbox_all_assets = len(bboxes) == 1

    shapes = _make_set_for_key(raster_metadata, "proj:shape", tuple)
    same_shapes_all_assets = len(shapes) == 1

    assets_have_same_proj_md = not is_some_raster_md_missing and all(
        [same_epsg_all_assets, same_bbox_all_assets, same_shapes_all_assets]
    )
    logger.debug(f"{assets_have_same_proj_md=}, based on: {is_some_raster_md_missing=}, {epsgs=}, {bboxes=}, {shapes=}")

    if assets_have_same_proj_md:
        # TODO: Should we overwrite or keep existing value for epsg?
        #   Seems best to keep existing values, but we should clarify which
        #   source gets priority for epsg.
        if not epsg and not job_result_metadata.get("epsg"):
            epsg = epsgs.pop()
            logger.debug(f"Projection metadata at top level: setting epsg to value from gdalinfo {epsg=}")
            job_result_metadata["epsg"] = epsg

        proj_bbox = list(bboxes.pop())
        job_result_metadata["proj:bbox"] = proj_bbox
        if not job_result_metadata.get("bbox"):
            # Convert bbox to lat-long / EPSG:4326, if it was any other CRS.
            bbox_lat_long = convert_bbox_to_lat_long(proj_bbox, epsg)
            job_result_metadata["bbox"] = bbox_lat_long
            logger.debug(
                f"Projection metadata at top level: setting bbox to value from gdalinfo: {job_result_metadata['bbox']}"
            )

        job_result_metadata["proj:shape"] = list(shapes.pop())
        logger.debug(
            "Projection metadata at top level: setting proj:shape "
            + f"to value from gdalinfo {job_result_metadata['proj:shape']=}"
        )
    else:
        # Each asset has its different projection metadata so set it per asset.
        for asset_path, raster_md in raster_metadata.items():
            proj_md = dict(**raster_md)
            if "raster:bands" in proj_md:
                del proj_md["raster:bands"]

            asset_metadata[asset_path].update(proj_md)
            logger.debug(
                f"Updated metadata for asset {asset_path} with projection metadata: "
                + f"{proj_md=}, {asset_metadata[asset_path]=}"
            )

    # Save raster statistics: always on the asset level.
    # There is no other place to store them really, and because stats are floats
    # they are very rarely going to be identical numbers.
    for asset_path, raster_md in raster_metadata.items():
        if "raster:bands" in raster_md:
            raster_bands = raster_md["raster:bands"]

            asset_metadata[asset_path]["raster:bands"] = raster_bands
            logger.debug(
                f"Updated metadata for asset {asset_path} with raster statistics: "
                + f"{raster_bands=}, {asset_metadata[asset_path]=}"
            )

    job_result_metadata["assets"] = asset_metadata


def _extract_asset_raster_metadata(
    asset_metadata: Dict[str, Any],
    job_dir: Path,
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """Extract STAC metadata about each raster asset, using gdalinfo.

    In particular we extract projection metadata and raster statistics.

    :param asset_metadata:
        the pre-existing metadata that should be completed with
        projection metadata and raster statistics.
    :param job_dir:
        the path where the job's metadata and result metadata is saved.
    :return:
        a dict that maps the asset's filename to a dict containing its metadata.
    """
    # TODO would be better if we could return just Dict[str, AssetRasterMetadata]
    #   or even CollectionRasterMetadata with CollectionRasterMetadata = Dict[str, AssetRasterMetadata]

    # Add the projection extension metadata.
    # When the projection metadata is the same for all assets, then set it at
    # the item level only. This makes it easier to read, so we see at a glance
    # that all bands have the same proj metadata.
    raster_metadata = {}

    # We also check whether any asset file was missing or its projection
    # metadata could not be read. In that case we never write the projection
    # metadata at the item level.
    is_some_raster_md_missing = False

    for asset_path, asset_md in asset_metadata.items():
        mime_type = asset_md.get("type")
        logger.debug(
            f"_export_result_metadata: {asset_path=}, "
            + f"file's MIME type: {mime_type}, "
            + f"job dir (based on output file): {job_dir=}"
        )
        logger.debug(f"{asset_path=}, {asset_md=}")

        #
        # TODO: Skip assets that aren't images
        #   For now I don't want to change the functionality,
        #   only adding logging to find why the gdalinfo receives a relative path.
        #   If the list of images formats is correct, then this code
        #   block should do the trick, bet test coverage should be added.
        #
        # Skip assets that aren't images
        # for example metadata with "type": "application/xml"
        # mime_type_images = ["image/tiff", "image/png", "application/x-netcdf"]
        # if mime_type and mime_type not in mime_type_images:
        #     logger.info(
        #         "_export_result_metadata: Asset file is not an image, "
        #         f"it has {mime_type=}: {asset_path=}"
        #     )
        #     continue

        # Won't assume the asset path is relative to the current working directory.
        # It should be relative to the job directory.
        abs_asset_path = get_abs_path_of_asset(asset_path, job_dir)
        logger.debug(f"{asset_path=} maps to absolute path: {abs_asset_path=} , " + f"{abs_asset_path.exists()=}")

        asset_href = asset_md.get("href", "")
        if not abs_asset_path.exists() and asset_href.startswith("s3://"):
            try:
                abs_asset_path.write_bytes(get_s3_binary_file_contents(asset_href))
            except Exception as exc:
                logger.error(
                    "Could not download asset from object storage: "
                    + f"asset={asset_path}, href={asset_href!r}, exception: {exc!r}"
                )

        asset_gdal_metadata = read_gdal_raster_metadata(abs_asset_path)
        logger.debug(f"{asset_path=}, {asset_gdal_metadata=}")
        # If gdal could not extract the projection metadata from the file
        # (The file is corrupt perhaps?).
        if asset_gdal_metadata.could_not_read_file:
            is_some_raster_md_missing = True
            logger.warning(
                "Could not get projection extension metadata for following asset:"
                + f" '{asset_path}', {abs_asset_path=}"
            )
        else:
            raster_metadata[asset_path] = asset_gdal_metadata.to_dict()
            # TODO: Would make it simpler if we could store the AssetRasterMetadata
            #   and convert it to dict at the end.
            # raster_metadata[asset_path] = asset_gdal_metadata

    logger.debug(f"{raster_metadata=}\n{is_some_raster_md_missing=}")
    return raster_metadata, is_some_raster_md_missing


def _make_set_for_key(
    data: Dict[str, Dict[str, Any]],
    key: str,
    func: callable = lambda x: x,
) -> set:
    """
    Create a set containing only the values for `key` from the dicts in data.values().

    Optionally apply func() to that value, for example to allow converting lists,
    which are not hashable and cannot be a set element, to tuples.
    """
    return {func(val.get(key)) for val in data.values() if key in val}


def get_abs_path_of_asset(asset_filename: str, job_dir: Union[str, Path]) -> Path:
    """Get a correct absolute path for the asset file.

    A simple `Path(mypath).resolve()` is not enough, because that is based on
    the current working directory and that is not guaranteed to be the
    job directory.

    Further, the job directory itself can also be a relative path, so we must
    also resolve job_dir as well.

    :param asset_filename:
        The filename or partial path to an asset file. This is the dictionary
        key for the asset name in the job's metadata

    :param job_dir:
        Path to the job directory.
        Can be either an absolute or a relative path.
        May come from user input on the command line, so it could be relative.

    :return: the absolute path to the asset file, inside job_dir.
    """
    logger.debug(
        f"{__name__}.get_abs_path_of_asset: {asset_filename=}, {job_dir=}, {Path.cwd()=}, "
        + f"{Path(job_dir).is_absolute()=}, {Path(job_dir).exists()=}, "
        + f"{Path(asset_filename).exists()=}"
    )

    abs_asset_path = Path(asset_filename)
    if not abs_asset_path.is_absolute():
        abs_asset_path = Path(job_dir).resolve() / asset_filename

    return abs_asset_path


def read_gdal_raster_metadata(asset_path: Union[str, Path]) -> Optional[AssetRasterMetadata]:
    """Get the projection metadata for the file in asset_path.

    :param asset_path: path to the asset file to read.

    :return:
        ProjectionMetadata, which is a dictionary containing the info for the
        STAC extension for projections.

        This dictionary contains the following fields, as described in stac-extensions,
        see: https://github.com/stac-extensions/projection

        - "proj:epsg"  The EPSG code of the CRS.
        - "proj:shape" The pixel size of the asset.
        - "proj:bbox"  The bounding box expressed in the asset CRS.

        When a field can not be found in the metadata that gdal.Info extracted,
        we leave out that field.

        Note that these dictionary keys in the return value *do* include the colon to be
        in line with the names in stac-extensions.

    TODO: upgrade GDAL to 3.6 and use the STAC dictionary that GDAL 3.6+ returns,
        instead of extracting it from the other output of ``gdal.Info()``.

    In a future version of the GeoPySpark driver we can upgrade to GDAL v3.6
    and in that version the gdal.Info function include these properties directly
    in the key "stac" of the dictionary it returns.
    """
    logger.debug(f"{__name__}.read_projection_extension_metadata: {asset_path=}")
    return parse_gdal_raster_metadata(read_gdal_info(str(asset_path)))


def read_gdal_info(asset_uri: str) -> GDALInfo:
    """Get the JSON output from gdal.Info for the file in asset_path

    This is equivalent to running the CLI tool called `gdalinfo`.

    :param asset_uri:
        Path to the asset file or URI to a subdataset in the file.

        If it is a netCDF file, we may get URIs of the format below, to access
        the subdatasets. See also: https://gdal.org/drivers/raster/netcdf.html

        NETCDF:"<regular path to netCDF file:>":<band name>

        For example:
        NETCDF:"/data/path/to/somefile.nc":B01

    :return:
        GDALInfo: which is a dictionary that contains the output from `gdal.Info()`.
    """
    logger.debug(f"{__name__}.read_gdal_info: {asset_uri=}")

    # By default, gdal does not raise exceptions but returns error codes and prints
    # error info on stdout. We don't want that. At the least it should go to the logs.
    # See https://gdal.org/api/python_gotchas.html
    gdal.UseExceptions()

    try:
        data_gdalinfo = gdal.Info(
            asset_uri,
            options=gdal.InfoOptions(format="json", stats=True),
        )
    except Exception as exc:
        # TODO: Specific exception type(s) would be better but Wasn't able to find what
        #   specific exceptions gdal.Info might raise.
        logger.warning(
            "Could not get projection extension metadata, "
            + f"gdal.Info failed for following asset: '{asset_uri}' . "
            + "Either file does not exist or else it is probably not a raster. "
            + f"Exception from GDAL: {exc}"
        )
        return {}
    else:
        logger.debug(f"{asset_uri=}, {data_gdalinfo=}")
        return data_gdalinfo


def parse_gdal_raster_metadata(gdal_info: GDALInfo) -> AssetRasterMetadata:
    """Parse the JSON output from gdal.Info.

    :param gdal_info: Dictionary that contains the output from `gdal.Info()`.
    :return:
        ProjectionMetadata, which is a dictionary containing the info for the
        STAC extension for projections.
    """

    # If there are subdatasets then the final answer comes from the subdatasets.
    # Otherwise, we get it from the file directly.
    if not gdal_info:
        return AssetRasterMetadata(could_not_read_file=True, gdal_info=gdal_info)

    raster_md = _process_gdalinfo_for_netcdf_subdatasets(gdal_info)
    if raster_md:
        return raster_md
    else:
        return AssetRasterMetadata(
            projection=_get_projection_extension_metadata(gdal_info),
            statistics=_get_raster_statistics(gdal_info),
            gdal_info=gdal_info,
        )


def _get_projection_extension_metadata(gdal_info: GDALInfo) -> ProjectionMetadata:
    """Helper function that parses gdal.Info output without processing subdatasets.

    :param gdal_info: Dictionary that contains the output from gdal.Info.

    :return:
        ProjectionMetadata, which is a dictionary containing the info for the
        STAC extension for projections.

        This dictionary contains the following fields, as described in stac-extensions,
        see: https://github.com/stac-extensions/projection

        - "proj:epsg"  The EPSG code of the CRS, if available.
        - "proj:shape" The pixel size of the asset, if available.
        - "proj:bbox"  The bounding box expressed in the asset CRS, if available.

        When a field can not be found in the metadata that gdal.Info extracted,
        we leave out that field.

        Note that these dictionary keys in the return value *do* include the colon to be
        in line with the names in stac-extensions.
    """
    proj_metadata = {}

    # Size of the pixels
    if shape := gdal_info.get("size"):
        proj_metadata["proj:shape"] = shape

    # Extract the EPSG code from the WKT string
    crs_as_wkt = gdal_info.get("coordinateSystem", {}).get("wkt")
    if crs_as_wkt:
        crs_id = pyproj.CRS.from_wkt(crs_as_wkt).to_epsg()
        if crs_id:
            proj_metadata["proj:epsg"] = crs_id

    # convert cornerCoordinates to proj:bbox format specified in
    # https://github.com/stac-extensions/projection
    # TODO: do we need to also handle 3D bboxes, i.e. the elevation bounds, if present?
    if "cornerCoordinates" in gdal_info:
        corner_coords: dict = gdal_info["cornerCoordinates"]
        # TODO: check if this way to combine the corners also handles 3D bounding boxes correctly.
        #   Need a correct example to test with.
        lole = corner_coords["lowerLeft"]
        upri = corner_coords["upperRight"]
        proj_metadata["proj:bbox"] = [*lole, *upri]

    return proj_metadata


def _get_raster_statistics(gdal_info: GDALInfo) -> RasterStatistics:
    """Helper function that parses gdal.Info output without processing subdatasets.

    :param gdal_info: Dictionary that contains the output from gdal.Info.

    :return: TODO
    """
    raster_stats = dict()
    for band in gdal_info.get("bands", []):
        band_num = band["band"]
        band_metadata = band.get("metadata", {})
        if not band_metadata:
            continue

        # Yes, the metadata from gdalinfo *does* contain a key that is
        # just the empty string.
        gdal_band_stats = band_metadata.get("", {})
        band_name = gdal_band_stats.get("long_name", str(band_num))

        def to_float_or_none(x):
            return None if x is None else float(x)

        band_stats = BandStatistics(
            minimum=to_float_or_none(gdal_band_stats.get("STATISTICS_MINIMUM")),
            maximum=to_float_or_none(gdal_band_stats.get("STATISTICS_MAXIMUM")),
            mean=to_float_or_none(gdal_band_stats.get("STATISTICS_MEAN")),
            stddev=to_float_or_none(gdal_band_stats.get("STATISTICS_STDDEV")),
            valid_percent=to_float_or_none(gdal_band_stats.get("STATISTICS_VALID_PERCENT")),
        )
        raster_stats[band_name] = band_stats

    return raster_stats


def _process_gdalinfo_for_netcdf_subdatasets(
    gdal_info: GDALInfo,
) -> Optional[AssetRasterMetadata]:
    """Read and process the gdal.Info for each subdataset, if subdatasets are present.

    This function only supports subdatasets in netCDF files.
    For other formats that may have subdatasets, such as HDF5, the subdatasets
    will not be processed.

    :param gdal_info: Dictionary that contains the output from gdal.Info.

    :return:
        ProjectionMetadata, which is a dictionary containing the info for the
        STAC extension for projections, the same type and information as what
        `_get_projection_extension_metadata` returns.

        Specifically:
        - "proj:epsg"  The EPSG code of the CRS.
        - "proj:shape" The pixel size of the asset.
        - "proj:bbox"  The bounding box expressed in the asset CRS.

        At present, when it is a netCDF file that does have subdatasets
        (bands, basically), then we only return the aforementioned fields from
        the subdatasets when all the subdatasets have the same value for that
        field. If the metadata differs between bands, then the field is left out.

        Storing the info of each individual band would be possible in the STAC
        standard, but we have not implemented at the moment in this function.
    """

    # TODO: might be better to separate out this check whether we need to process it.
    #   That would give cleaner logic, and no need to return None here.

    # NetCDF files list their bands under SUBDATASETS and more info can be
    # retrieved with a second gdal.Info() query.
    # This function only supports subdatasets in netCDF.
    # For other formats that have subdatasets, such as HDF5, the subdatasets
    # will not be processed.
    if gdal_info.get("driverShortName") != "netCDF":
        return None
    if "SUBDATASETS" not in gdal_info.get("metadata", {}):
        return None

    sub_datasets_proj = {}
    sub_datasets_stats = {}
    for key, sub_ds_uri in gdal_info["metadata"]["SUBDATASETS"].items():
        if key.endswith("_NAME"):
            sub_ds_gdal_info = read_gdal_info(sub_ds_uri)
            sub_ds_md = _get_projection_extension_metadata(sub_ds_gdal_info)
            sub_datasets_proj[sub_ds_uri] = sub_ds_md

            stats_info = _get_raster_statistics(sub_ds_gdal_info)
            logger.info(f"_process_gdalinfo_for_netcdf_subdatasets:: {stats_info=}")
            sub_datasets_stats[sub_ds_uri] = stats_info

    proj_info = {}
    shapes = _make_set_for_key(sub_datasets_proj, "proj:shape", tuple)
    if len(shapes) == 1:
        proj_info["proj:shape"] = list(shapes.pop())

    bboxes = _make_set_for_key(sub_datasets_proj, "proj:bbox", tuple)
    if len(bboxes) == 1:
        proj_info["proj:bbox"] = list(bboxes.pop())

    epsg_codes = _make_set_for_key(sub_datasets_proj, "proj:epsg")
    if len(epsg_codes) == 1:
        proj_info["proj:epsg"] = epsg_codes.pop()

    ds_band_names = [band for bands in sub_datasets_stats.values() for band in bands.keys()]
    logger.debug(f"{ds_band_names=}")

    all_raster_stats = None

    # We can only copy each band's stats if there are no duplicate bands across the subdatasets.
    if sorted(set(ds_band_names)) != sorted(ds_band_names):
        logger.warning(f"There are duplicate bands in {ds_band_names=}, Can not merge the bands' statistics.")
    else:
        logger.info(f"There are no duplicate bands in {ds_band_names=}, Will use all bands' statistics in result.")
        all_raster_stats = {}
        for bands in sub_datasets_stats.values():
            for band_name, stats in bands.items():
                all_raster_stats[band_name] = stats

    logger.debug(f"{all_raster_stats=}")

    result = AssetRasterMetadata(gdal_info=gdal_info, projection=proj_info, statistics=all_raster_stats)
    logger.debug(f"_process_gdalinfo_for_netcdf_subdatasets:: returning {result=}")

    return AssetRasterMetadata(gdal_info=gdal_info, projection=proj_info, statistics=all_raster_stats)


def _get_tracker(tracker_id=""):
    return get_jvm().org.openeo.geotrelliscommon.BatchJobMetadataTracker.tracker(tracker_id)


def _get_tracker_metadata(tracker_id="") -> dict:
    tracker = _get_tracker(tracker_id)
    t = tracker
    if(t is not None):
        tracker_results = t.asDict()
        pu = tracker_results.get("Sentinelhub_Processing_Units",None)
        usage = None
        if pu is not None:
            usage = {"sentinelhub":{"value":pu,"unit":"sentinelhub_processing_unit"}}

        pixels = tracker_results.get("InputPixels", None)
        if pixels is not None:
            usage = {"input_pixel":{"value":pixels/(1024*1024),"unit":"mega-pixel"}}

        links = tracker_results.get("links", None)
        all_links = None
        if links is not None:
            all_links = list(chain(*links.values()))
            all_links = [{"href": link.getSelfUrl(), "rel": "derived_from", "title": f"Derived from {link.getId()}"} for link in all_links]

        return dict_no_none(usage=usage,links=all_links)


def _deserialize_dependencies(arg: str) -> List[dict]:
    return json.loads(arg)


def _get_sentinel_hub_credentials_from_spark_conf(conf: SparkConf) -> Optional[(str, str)]:
    default_client_id = conf.get('spark.openeo.sentinelhub.client.id.default')
    default_client_secret = conf.get('spark.openeo.sentinelhub.client.secret.default')

    return (default_client_id, default_client_secret) if default_client_id and default_client_secret else None


def _get_vault_token(conf: SparkConf) -> Optional[str]:
    return conf.get('spark.openeo.vault.token')


def _get_access_token(conf: SparkConf) -> Optional[str]:
    return conf.get('spark.openeo.access_token')


def main(argv: List[str]) -> None:
    logger.info("batch_job.py argv: {a!r}".format(a=argv))
    logger.info("batch_job.py pid {p}; ppid {pp}; cwd {c}".format(p=os.getpid(), pp=os.getppid(), c=os.getcwd()))
    logger.warning("batch_job.py version info %r", build_gps_backend_deploy_metadata(
        packages=[
            "openeo",
            "openeo_driver",
            "openeo-geopyspark",
            # TODO list more packages like in openeo-deploy?
        ],
    ))

    if len(argv) < 10:
        raise Exception(
            f"usage: {argv[0]} "
            "<job specification input file> <job directory> <results output file name> <user log file name> "
            "<metadata file name> <api version> <dependencies> <user id> <max soft errors ratio> "
            "[Sentinel Hub client alias]"
        )

    job_specification_file = argv[1]
    job_dir = Path(argv[2])
    output_file = job_dir / argv[3]
    log_file = job_dir / argv[4]
    metadata_file = job_dir / argv[5]
    api_version = argv[6]
    dependencies = _deserialize_dependencies(argv[7])
    user_id = argv[8]
    BatchJobLoggingFilter.set("user_id", user_id)
    max_soft_errors_ratio = float(argv[9])
    sentinel_hub_client_alias = argv[10] if len(argv) >= 11 else None

    _create_job_dir(job_dir)

    _setup_user_logging(log_file)

    # Override default temp dir (under CWD). Original default temp dir `/tmp` might be cleaned up unexpectedly.
    temp_dir = Path(os.getcwd()) / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Using temp dir {t}".format(t=temp_dir))
    os.environ["TMPDIR"] = str(temp_dir)

    if ConfigParams().is_kube_deploy:
        from openeogeotrellis.utils import s3_client

        bucket = os.environ.get('SWIFT_BUCKET')
        s3_instance = s3_client()

        s3_instance.download_file(bucket, job_specification_file.strip("/"), job_specification_file )


    job_specification = _parse(job_specification_file)
    load_custom_processes()

    conf = (SparkConf()
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set(key='spark.kryo.registrator', value='geopyspark.geotools.kryo.ExpandedKryoRegistrator')
            .set("spark.kryo.classesToRegister", "org.openeo.geotrellisaccumulo.SerializableConfiguration,ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion"))

    with SparkContext(conf=conf) as sc:
        principal = sc.getConf().get("spark.yarn.principal")
        key_tab = sc.getConf().get("spark.yarn.keytab")

        default_sentinel_hub_credentials = _get_sentinel_hub_credentials_from_spark_conf(sc.getConf())
        vault_token = _get_vault_token(sc.getConf())
        access_token = _get_access_token(sc.getConf())

        kerberos(principal, key_tab)

        def run_driver():
            run_job(
                job_specification=job_specification, output_file=output_file, metadata_file=metadata_file,
                api_version=api_version, job_dir=job_dir, dependencies=dependencies, user_id=user_id,
                max_soft_errors_ratio=max_soft_errors_ratio,
                default_sentinel_hub_credentials=default_sentinel_hub_credentials,
                sentinel_hub_client_alias=sentinel_hub_client_alias, vault_token=vault_token, access_token=access_token,
            )

        if sc.getConf().get('spark.python.profile', 'false').lower() == 'true':
            # Including the driver in the profiling: a bit hacky solution but spark profiler api does not allow passing args&kwargs
            driver_profile = BasicProfiler(sc)
            driver_profile.profile(run_driver)
            # running the driver code and adding driver's profiling results as "RDD==-1"
            sc.profiler_collector.add_profiler(-1, driver_profile)
            # collect profiles into a zip file
            profile_dumps_dir = job_dir / 'profile_dumps'
            sc.dump_profiles(profile_dumps_dir)

            profile_zip = shutil.make_archive(base_name=str(profile_dumps_dir), format='gztar',
                                              root_dir=profile_dumps_dir)
            add_permissions(Path(profile_zip), stat.S_IWGRP)

            shutil.rmtree(profile_dumps_dir,
                          onerror=lambda func, path, exc_info:
                          logger.warning(f"could not recursively delete {profile_dumps_dir}: {func} {path} failed",
                                         exc_info=exc_info))

            logger.info("Saved profiling info to: " + profile_zip)
        else:
            run_driver()


@log_memory
def run_job(
    job_specification,
    output_file: Union[str, Path],
    metadata_file: Union[str, Path],
    api_version: str,
    job_dir: Union[str, Path],
    dependencies: List[dict],
    user_id: str = None,
    max_soft_errors_ratio: float = 0.0,
    default_sentinel_hub_credentials=None,
    sentinel_hub_client_alias="default",
    vault_token: str = None,
    access_token: str = None,
):
    # We actually expect type Path, but in reality paths as strings tend to
    # slip in anyway, so we better catch them and convert them.
    output_file = Path(output_file)
    metadata_file = Path(metadata_file)
    job_dir = Path(job_dir)

    logger.info(f"Job spec: {json.dumps(job_specification,indent=1)}")
    logger.info(f"{job_dir=}, {job_dir.resolve()=}, {output_file=}, {metadata_file=}")
    process_graph = job_specification['process_graph']
    job_options = job_specification.get("job_options", {})

    backend_implementation = GeoPySparkBackendImplementation(
        use_job_registry=False,
    )

    if default_sentinel_hub_credentials is not None:
        backend_implementation.set_default_sentinel_hub_credentials(*default_sentinel_hub_credentials)

    logger.info(f"Using backend implementation {backend_implementation}")
    correlation_id = OPENEO_BATCH_JOB_ID
    logger.info(f"Correlation id: {correlation_id}")
    env_values = {
        'version': api_version or "1.0.0",
        'pyramid_levels': 'highest',
        'user': User(user_id=user_id, internal_auth_data=dict_no_none(access_token=access_token)),
        'require_bounds': True,
        'correlation_id': correlation_id,
        'dependencies': dependencies.copy(),  # will be mutated (popped) during evaluation
        'backend_implementation': backend_implementation,
        'max_soft_errors_ratio': max_soft_errors_ratio,
        'sentinel_hub_client_alias': sentinel_hub_client_alias,
        'vault_token': vault_token
    }
    job_option_whitelist = [
        "data_mask_optimization",
        "node_caching"
    ]
    env_values.update({k: job_options[k] for k in job_option_whitelist if k in job_options})
    env = EvalEnv(env_values)
    tracer = DryRunDataTracer()
    logger.info("Starting process graph evaluation")
    pg_copy = deepcopy(process_graph)
    result = ProcessGraphDeserializer.evaluate(process_graph, env=env, do_dry_run=tracer)
    logger.info("Evaluated process graph, result (type {t}): {r!r}".format(t=type(result), r=result))

    if isinstance(result, DelayedVector):
        geojsons = (mapping(geometry) for geometry in result.geometries_wgs84)
        result = JSONResult(geojsons)

    if isinstance(result, DriverDataCube):
        format_options = job_specification.get('output', {})
        format_options["batch_mode"] = True
        result = ImageCollectionResult(cube=result, format='GTiff', options=format_options)

    if isinstance(result, DriverVectorCube):
        format_options = job_specification.get('output', {})
        format_options["batch_mode"] = True
        result = VectorCubeResult(cube=result, format='GTiff', options=format_options)

    if not isinstance(result, SaveResult) and not isinstance(result,List):  # Assume generic JSON result
        result = JSONResult(result)

    result_list = result
    if not isinstance(result,List):
        result_list = [result]

    global_metadata_attributes = {
        "title" : job_specification.get("title",""),
        "description": job_specification.get("description", ""),
        "institution": "openEO platform - Geotrellis backend: " + __version__
    }

    assets_metadata = {}
    for result in result_list:
        ml_model_metadata = None
        if('write_assets' in dir(result)):
            result.options["batch_mode"] = True
            result.options["file_metadata"] = global_metadata_attributes
            if( result.options.get("sample_by_feature")):
                geoms = tracer.get_last_geometry("filter_spatial")
                if geoms == None:
                    logger.warning("sample_by_feature enabled, but no geometries found. They can be specified using filter_spatial.")
                else:
                    result.options["geometries"] = geoms
                if(result.options["geometries"] == None):
                    logger.error("samply_by_feature was set, but no geometries provided through filter_spatial. Make sure to provide geometries.")
            the_assets_metadata = result.write_assets(str(output_file))
            if isinstance(result, MlModelResult):
                ml_model_metadata = result.get_model_metadata(str(output_file))
                logger.info("Extracted ml model metadata from %s" % output_file)
            for name,asset in the_assets_metadata.items():
                add_permissions(Path(asset["href"]), stat.S_IWGRP)
            logger.info(f"wrote {len(the_assets_metadata)} assets to {output_file}")
            assets_metadata = {**assets_metadata,**the_assets_metadata}
        elif isinstance(result, ImageCollectionResult):
            result.options["batch_mode"] = True
            result.save_result(filename=str(output_file))
            add_permissions(output_file, stat.S_IWGRP)
            logger.info("wrote image collection to %s" % output_file)
        elif isinstance(result, NullResult):
            logger.info("skipping output file %s" % output_file)
        else:
            raise NotImplementedError("unsupported result type {r}".format(r=type(result)))

    if any(dependency['card4l'] for dependency in dependencies):  # TODO: clean this up
        logger.debug("awaiting Sentinel Hub CARD4L data...")

        s3_service = get_jvm().org.openeo.geotrellissentinelhub.S3Service()

        poll_interval_secs = 10
        max_delay_secs = 600

        card4l_source_locations = [dependency['source_location'] for dependency in dependencies if dependency['card4l']]

        for source_location in set(card4l_source_locations):
            uri_parts = urlparse(source_location)
            bucket_name = uri_parts.hostname
            request_group_id = uri_parts.path[1:]

            try:
                # TODO: incorporate index to make sure the files don't clash
                s3_service.download_stac_data(bucket_name, request_group_id, str(job_dir), poll_interval_secs,
                                              max_delay_secs)
                logger.info("downloaded CARD4L data in {b}/{g} to {d}"
                            .format(b=bucket_name, g=request_group_id, d=job_dir))
            except Py4JJavaError as e:
                java_exception = e.java_exception

                if (java_exception.getClass().getName() ==
                        'org.openeo.geotrellissentinelhub.S3Service$StacMetadataUnavailableException'):
                    logger.warning("could not find CARD4L metadata to download from s3://{b}/{r} after {d}s"
                                   .format(b=bucket_name, r=request_group_id, d=max_delay_secs))
                else:
                    raise e

        _transform_stac_metadata(job_dir)

    unique_process_ids = CollectUniqueProcessIdsVisitor().accept_process_graph(process_graph).process_ids



    if "file_metadata" in result.options:
        result.options["file_metadata"]["providers"] = [
            {
                "name": "VITO",
                "description": "This data was processed on an openEO backend maintained by VITO.",
                "roles": [
                    "processor"
                ],
                "processing:facility": "openEO Geotrellis backend",
                "processing:software": {
                    "Geotrellis backend": __version__
                }
            }],
        result.options["file_metadata"]["processing:expression"] = [
            {
                "format": "openeo",
                "expression": pg_copy
            }]

    _export_result_metadata(tracer=tracer, result=result, output_file=output_file, metadata_file=metadata_file,
                            unique_process_ids=unique_process_ids, asset_metadata=assets_metadata,
                            ml_model_metadata=ml_model_metadata)

    if ConfigParams().is_kube_deploy:
        from openeogeotrellis.utils import s3_client

        _convert_job_metadatafile_outputs_to_s3_urls(metadata_file)

        bucket = os.environ.get('SWIFT_BUCKET')
        s3_instance = s3_client()

        logger.info("Writing results to object storage")
        for file in os.listdir(job_dir):
            full_path = str(job_dir) + "/" + file
            s3_instance.upload_file(full_path, bucket, full_path.strip("/"))


def _convert_job_metadatafile_outputs_to_s3_urls(metadata_file: Path):
    """Convert each asset's output_dir value to a URL on S3, in the job metadata file."""
    with open(metadata_file, "rt") as mdf:
        metadata_to_update = json.load(mdf)
    with open(metadata_file, "wt") as mdf:
        _convert_asset_outputs_to_s3_urls(metadata_to_update)
        json.dump(metadata_to_update, mdf)


def _convert_asset_outputs_to_s3_urls(job_metadata: dict):
    """Convert each asset's output_dir value to a URL on S3 in the metadata dictionary."""
    out_assets = job_metadata.get("assets", {})
    for asset in out_assets.values():
        if "href" in asset and not asset["href"].startswith("s3://"):
            asset["href"] = to_s3_url(asset["href"])


def _transform_stac_metadata(job_dir: Path):
    def relativize(assets: dict) -> dict:
        def relativize_href(asset: dict) -> dict:
            absolute_href = asset['href']
            relative_path = urlparse(absolute_href).path.split("/")[-1]
            return dict(asset, href=relative_path)

        return {asset_name: relativize_href(asset) for asset_name, asset in assets.items()}

    def drop_links(metadata: dict) -> dict:
        result = metadata.copy()
        result.pop('links', None)
        return result

    stac_metadata_files = [job_dir / file_name for file_name in os.listdir(job_dir) if
                           file_name.endswith("_metadata.json") and file_name != JOB_METADATA_FILENAME]

    for stac_metadata_file in stac_metadata_files:
        with open(stac_metadata_file, 'rt', encoding='utf-8') as f:
            stac_metadata = json.load(f)

        relative_assets = relativize(stac_metadata.get('assets', {}))
        transformed = dict(drop_links(stac_metadata), assets=relative_assets)

        with open(stac_metadata_file, 'wt', encoding='utf-8') as f:
            json.dump(transformed, f, indent=2)


if __name__ == '__main__':
    setup_logging(get_logging_config(
        root_handlers=["stderr_json" if ConfigParams().is_kube_deploy else "file_json"],
        context=LOGGING_CONTEXT_BATCH_JOB,
        root_level=OPENEO_LOGGING_THRESHOLD),
        capture_unhandled_exceptions=False,  # not needed anymore, as we have a try catch around everything
    )

    try:
        with TimingLogger("batch_job.py main", logger=logger):
            main(sys.argv)
    except Exception as e:
        error_summary = GeoPySparkBackendImplementation.summarize_exception_static(e)
        user_facing_logger.exception("OpenEO batch job failed: " + error_summary.summary)
        raise
