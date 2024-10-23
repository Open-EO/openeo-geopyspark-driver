import json
import logging
import os
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union
from urllib.parse import urlparse

import pyproj
from openeo.util import Rfc3339, dict_no_none
from shapely.geometry import mapping, Polygon
from shapely.geometry.base import BaseGeometry

from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import (ImageCollectionResult, SaveResult, NullResult, )
from openeo_driver.util.geometry import spatial_extent_union, reproject_bounding_box
from openeo_driver.util.utm import area_in_square_meters
from openeo_driver.utils import temporal_extent_union
from openeogeotrellis._version import __version__
from openeogeotrellis.backend import GeoPySparkBackendImplementation, JOB_METADATA_FILENAME
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.integrations.gdal import _extract_gdal_asset_raster_metadata
from openeogeotrellis.utils import (get_jvm, _make_set_for_key, to_s3_url, )

logger = logging.getLogger(__name__)


def _assemble_result_metadata(
    tracer: DryRunDataTracer,
    result: SaveResult,
    output_file: Path,
    unique_process_ids: Set[str],
    apply_gdal,
    asset_metadata: Dict = None,
    ml_model_metadata: Dict = None,
) -> dict:
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
            if apply_gdal:
                try:
                    _extract_asset_metadata(
                        job_result_metadata=metadata,
                        asset_metadata=asset_metadata,
                        job_dir=output_file.parent,
                        epsg=epsg,
                    )
                except Exception as e:
                    error_summary = GeoPySparkBackendImplementation.summarize_exception_static(e)
                    logger.exception("Error while creating asset metadata: " + error_summary.summary)
            else:
                metadata["assets"] = asset_metadata

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
    if isinstance(result,SaveResult):
        global_metadata = result.options.get("file_metadata",{})
    metadata["providers"] = global_metadata.get("providers",[])

    if ml_model_metadata is not None:
        metadata['ml_model_metadata'] = ml_model_metadata

    return metadata


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
    lonlat_geometry = None
    area = None
    if len(extents) > 0:
        spatial_extent = spatial_extent_union(*extents)
        bbox_crs = spatial_extent["crs"]
        temp_bbox = [spatial_extent[b] for b in ["west", "south", "east", "north"]]
        if all(b is not None for b in temp_bbox):
            bbox = temp_bbox  # Only set bbox once we are sure we have all the info
            area = area_in_square_meters(Polygon.from_bounds(*bbox), bbox_crs)
            lonlat_geometry = mapping(Polygon.from_bounds(*convert_bbox_to_lat_long(bbox, bbox_crs)))

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
            lonlat_geometry = mapping(agg_geometry)
            area = area_in_square_meters(agg_geometry, bbox_crs)
        elif isinstance(agg_geometry, DelayedVector):
            bbox = agg_geometry.bounds
            bbox_crs = agg_geometry.crs
            # Intentionally don't return the complete vector file. https://github.com/Open-EO/openeo-api/issues/339
            lonlat_geometry = mapping(Polygon.from_bounds(*convert_bbox_to_lat_long(bbox, bbox_crs)))
            area = DriverVectorCube.from_fiona([agg_geometry.path]).get_area()
        elif isinstance(agg_geometry, DriverVectorCube):
            if agg_geometry.geometry_count() != 0:
                bbox = agg_geometry.get_bounding_box()
                bbox_crs = agg_geometry.get_crs()
                lonlat_geometry = agg_geometry.get_bounding_box_geojson()
                area = agg_geometry.get_area()
        else:
            logger.warning(f"Result metadata: no bbox/area support for {type(agg_geometry)}")

        # The aggregation geometries return tuples for their bounding box.
        # Keep the end result consistent and convert it to a list.
        if isinstance(bbox, tuple):
            bbox = list(bbox)

    links = tracer.get_metadata_links()
    links = [link for k, v in links.items() for link in v]

    # TODO: dedicated type?
    # TODO: match STAC format?
    return {
        "geometry": lonlat_geometry,
        "bbox": convert_bbox_to_lat_long(bbox, bbox_crs),
        "area": {"value": area, "unit": "square meter"} if area else None,
        "start_datetime": start_date,
        "end_datetime": end_date,
        "links": links,
    }


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
    raster_metadata, is_some_raster_md_missing = _extract_gdal_asset_raster_metadata(asset_metadata, job_dir)

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


def _get_tracker(tracker_id=""):
    return get_jvm().org.openeo.geotrelliscommon.BatchJobMetadataTracker.tracker(tracker_id)


def _get_tracker_metadata(tracker_id="") -> dict:
    tracker = _get_tracker(tracker_id)

    if tracker is not None:
        tracker_results = tracker.asDict()

        usage = {}
        pu = tracker_results.get("Sentinelhub_Processing_Units", None)
        if pu is not None:
            usage["sentinelhub"] = {"value": pu, "unit": "sentinelhub_processing_unit"}

        pixels = tracker_results.get("InputPixels", None)
        if pixels is not None:
            usage["input_pixel"] = {"value": pixels / (1024 * 1024), "unit": "mega-pixel"}

        links = tracker_results.get("links", None)
        all_links = None
        if links is not None:
            all_links = list(chain(*links.values()))
            # TODO: when in the future these links point to STAC objects we will need to update the type.
            #   https://github.com/openEOPlatform/architecture-docs/issues/327
            all_links = [
                {
                    "href": link.getSelfUrl(),
                    "rel": "derived_from",
                    "title": f"Derived from {link.getId()}",
                    "type": "application/json",
                }
                for link in all_links
            ]

        return dict_no_none(usage=usage if usage != {} else None, links=all_links)
