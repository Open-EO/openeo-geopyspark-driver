import multiprocessing
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import time
from typing import Dict, Optional, Union, Any, Tuple

import pyproj
from math import isfinite
from openeo.util import dict_no_none
from osgeo import gdal

from openeogeotrellis.utils import stream_s3_binary_file_contents, _make_set_for_key


def poorly_log(message: str, level=logging.INFO):
    # TODO: fix logging in combination with multiprocessing (#906)
    log_entry = dict_no_none(
        levelname=logging.getLevelName(level),
        message=message,
        created=time.time(),
        filename=Path(__file__).name,
        user_id=os.environ.get("OPENEO_USER_ID"),
        job_id=os.environ.get("OPENEO_BATCH_JOB_ID"),
    )

    print(json.dumps(log_entry))


"""Output from GDAL.Info.

Type alias used as a helper type for the read projection metadata functions.
"""
GDALInfo = Dict[str, Any]
"""Projection metadata retrieved about the raster file, compatible with STAC.

Type alias used as a helper type for the read projection metadata functions.
"""
ProjectionMetadata = Dict[str, Any]


@dataclass
class BandStatistics:
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    valid_percent: Optional[float] = None

    @staticmethod
    def _to_jsonable_float(x: Optional[float]) -> Union[float, str, None]:
        if x is None:
            return None

        return x if isfinite(x) else str(x)

    def to_dict(self):
        return {
            "minimum": self._to_jsonable_float(self.minimum),
            "maximum": self._to_jsonable_float(self.maximum),
            "mean": self._to_jsonable_float(self.mean),
            "stddev": self._to_jsonable_float(self.stddev),
            "valid_percent": self._to_jsonable_float(self.valid_percent),
        }


"""
Maps a the bands in a raster to their band statistics, by the band name (str).

If the band name can not be found in the gdalinfo output, then the name will
be the band's number, as a string for consistency, and starting to count from 1,
so that would be: "1", "2", etc.
"""
RasterStatistics = Dict[str, BandStatistics]


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


def _extract_gdal_asset_raster_metadata(
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


    def error_handler(e):
        poorly_log(f"Error while looking up result metadata, may be incomplete. {str(e)}", level=logging.WARNING)

    pool_size = min(10,max(1,int(len(asset_metadata)//3)))

    pool = multiprocessing.Pool(pool_size)
    job = [pool.apply_async(_get_metadata_callback, (asset_path, asset_md,job_dir,), error_callback=error_handler) for asset_path, asset_md in asset_metadata.items()]
    pool.close()
    pool.join()


    # Add the projection extension metadata.
    # When the projection metadata is the same for all assets, then set it at
    # the item level only. This makes it easier to read, so we see at a glance
    # that all bands have the same proj metadata.
    raster_metadata = {}

    # We also check whether any asset file was missing or its projection
    # metadata could not be read. In that case we never write the projection
    # metadata at the item level.
    is_some_raster_md_missing = False

    for j in job:
        try:
            result = j.get()
            if result is not None:
                raster_metadata[result[0]] = result[1]
            else:
                is_some_raster_md_missing = True

        except Exception as e:
            is_some_raster_md_missing = True

    #
    return raster_metadata, is_some_raster_md_missing


def _get_metadata_callback(asset_path: str, asset_md: Dict[str, str], job_dir: Path):

    mime_type: str = asset_md.get("type", "")

    # Skip assets that are clearly not images.
    if asset_path.endswith(".json"):
        return None

    # The asset path should be relative to the job directory.
    abs_asset_path: Path = get_abs_path_of_asset(asset_path, job_dir)

    asset_href: str = asset_md.get("href", "")
    if not abs_asset_path.exists() and asset_href.startswith("s3://"):
        try:
            with open(abs_asset_path, "wb") as f:
                for chunk in stream_s3_binary_file_contents(asset_href):
                    f.write(chunk)
        except Exception as exc:
            message = (
                "Could not download asset from object storage: "
                + f"asset={asset_path}, href={asset_href!r}, exception: {exc!r}"
            )
            poorly_log(message, level=logging.ERROR)

    asset_gdal_metadata: AssetRasterMetadata = read_gdal_raster_metadata(abs_asset_path)
    # If gdal could not extract the projection metadata from the file
    # (The file is corrupt perhaps?).
    if asset_gdal_metadata.could_not_read_file:
        return None
    else:
        return (asset_path, asset_gdal_metadata.to_dict())
        # TODO: Would make it simpler if we could store the AssetRasterMetadata
        #   and convert it to dict at the end.
        # raster_metadata[asset_path] = asset_gdal_metadata


def read_gdal_raster_metadata(asset_path: Union[str, Path]) -> AssetRasterMetadata:
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
    return parse_gdal_raster_metadata(read_gdal_info(str(asset_path)))


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
            band_name = sub_ds_uri.split(":")[-1]
            sub_ds_md = _get_projection_extension_metadata(sub_ds_gdal_info)
            sub_datasets_proj[sub_ds_uri] = sub_ds_md

            stats_info = _get_raster_statistics(sub_ds_gdal_info, band_name)
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

    all_raster_stats = {}

    # We can only copy each band's stats if there are no duplicate bands across
    # the subdatasets. If we find duplicate bands there is likely a bug.
    # Besides it is not obvious how we would need to merge statistics across
    # subdatasets, if the bands occur multiple times.
    for bands in sub_datasets_stats.values():
        for band_name, stats in bands.items():
            all_raster_stats[band_name] = stats

    result = AssetRasterMetadata(gdal_info=gdal_info, projection=proj_info, statistics=all_raster_stats)
    return AssetRasterMetadata(gdal_info=gdal_info, projection=proj_info, statistics=all_raster_stats)


def _get_projection_extension_metadata(gdal_info: GDALInfo) -> ProjectionMetadata:
    """Helper function that parses gdal.Info output without processing subdatasets.

    :param gdal_info: Dictionary that contains the output from gdal.Info.

    :return:
        ProjectionMetadata, which is a dictionary containing the info for the
        STAC extension for projections.

        This dictionary contains the following fields, as described in stac-extensions,
        see: https://github.com/stac-extensions/projection

        - "proj:epsg"  The EPSG code of the CRS, if available.
        - "proj:shape" The pixel size of the asset, if available, in Y,X order.
        - "proj:bbox"  The bounding box expressed in the asset CRS, if available.

        When a field can not be found in the metadata that gdal.Info extracted,
        we leave out that field.

        Note that these dictionary keys in the return value *do* include the colon to be
        in line with the names in stac-extensions.
    """
    proj_metadata = {}

    # Size of the pixels
    if shape := gdal_info.get("size"):
        proj_metadata["proj:shape"] = list(shape.__reversed__())

    # Extract the EPSG code from the WKT string
    crs_as_wkt = gdal_info.get("coordinateSystem", {}).get("wkt")
    if not crs_as_wkt:
        crs_as_wkt = gdal_info.get("metadata", {}).get("GEOLOCATION", {}).get("SRS", {})
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

    # TODO: wgs84Extent gives us a polygon in lot-long directly, so it may be worth extracting.
    #   However since wgs84Extent is a polygon it might not be what we want after all.

    return proj_metadata


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
    abs_asset_path = Path(asset_filename)
    if not abs_asset_path.is_absolute():
        abs_asset_path = Path(job_dir).resolve() / asset_filename

    return abs_asset_path


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
        return {}
    else:
        return data_gdalinfo


def _get_raster_statistics(gdal_info: GDALInfo, band_name: Optional[str] = None) -> RasterStatistics:
    """Helper function that parses gdal.Info output without processing subdatasets.

    :param gdal_info: Dictionary that contains the output from gdal.Info.
    :band_name:
        The band name extracted from a subdataset's name, if it was a subdataset.
        If it is a regular file: None

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
        band_name_out = (
            band_name or gdal_band_stats.get("long_name") or gdal_band_stats.get("DESCRIPTION") or str(band_num)
        )

        def to_float_or_none(x: Optional[str]):
            return None if x is None else float(x)

        band_stats = BandStatistics(
            minimum=to_float_or_none(gdal_band_stats.get("STATISTICS_MINIMUM")),
            maximum=to_float_or_none(gdal_band_stats.get("STATISTICS_MAXIMUM")),
            mean=to_float_or_none(gdal_band_stats.get("STATISTICS_MEAN")),
            stddev=to_float_or_none(gdal_band_stats.get("STATISTICS_STDDEV")),
            valid_percent=to_float_or_none(gdal_band_stats.get("STATISTICS_VALID_PERCENT")),
        )
        raster_stats[band_name_out] = band_stats

    return raster_stats
