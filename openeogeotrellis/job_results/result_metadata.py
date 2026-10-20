"""
Plain-dict result metadata: extraction from the dry-run tracer, assembly of
`job_metadata.json`'s top-level fields, and href helpers.
"""
import datetime as dt
import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pyproj
import shapely.geometry
from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.util import Rfc3339
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.save_result import NullResult, SaveResult
from openeo_driver.util.geometry import BoundingBox, reproject_bounding_box, spatial_extent_union
from openeo_driver.util.utm import area_in_square_meters
from openeo_driver.utils import temporal_extent_union
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry

from .settings import JobResultsSettings, ResultCubeMetadata
from .util import to_s3_url

logger = logging.getLogger(__name__)


class CollectUniqueProcessIdsVisitor(ProcessGraphVisitor):
    def __init__(self):
        super().__init__()
        self.process_ids = set()

    def enterProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        self.process_ids.add(process_id)

    def _accept_dict(self, value: dict):
        if "process_graph" in value:
            self.accept_process_graph(value["process_graph"])


def _bbox_to_geojson(bbox: Union[Tuple, List]) -> dict:
    """Convert a lon-lat bounding box (xmin, ymin, xmax, ymax) to a GeoJSON (Polygon) dict."""
    xmin, ymin, xmax, ymax = bbox
    geometry = BoundingBox(xmin, ymin, xmax, ymax, crs=4326).as_geometry()
    return shapely.geometry.mapping(geometry)


def _extract_temporal_extent_from_items(stac_items: List[dict]) -> Tuple[Optional[dt.datetime], Optional[dt.datetime]]:
    def _parse(dt_str: Optional[str]):
        if not dt_str:
            return None
        return Rfc3339(propagate_none=True).parse_datetime(dt_str)

    starts = []
    ends = []
    for item in stac_items:
        props = item.get("properties", {})
        datetime = _parse(props.get("datetime"))
        start_datetime = _parse(props.get("start_datetime")) or datetime
        end_datetime = _parse(props.get("end_datetime")) or datetime
        if start_datetime is not None:
            starts.append(start_datetime)
        if end_datetime is not None:
            ends.append(end_datetime)

    return min(starts) if starts else None, max(ends) if ends else None


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
        latlon_spatial_extent = reproject_bounding_box(latlon_spatial_extent, from_crs=None, to_crs="EPSG:4326")
        return [latlon_spatial_extent[b] for b in ["west", "south", "east", "north"]]

    return bbox


def extract_result_metadata(tracer: DryRunDataTracer, stac_items: Optional[List[dict]] = None) -> dict:
    logger.info("Extracting result metadata from {t!r}".format(t=tracer))

    rfc3339 = Rfc3339(propagate_none=True)

    source_constraints = tracer.get_source_constraints()

    # Take union of extents
    temporal_extent = temporal_extent_union(
        *[sc["temporal_extent"] for _, sc in source_constraints if "temporal_extent" in sc]
    )

    if stac_items and (not temporal_extent or not temporal_extent[0] or not temporal_extent[1]):
        item_start, item_end = _extract_temporal_extent_from_items(stac_items)
        if item_start is not None or item_end is not None:
            logger.info(
                "Could not get temporal_extent from source constraints. "
                f"Extracting extent from items: [{item_start}, {item_end}]."
            )
            # TODO: use stac_items by default to get extent. Avoid relying on source_constraints.
            temporal_extent = (
                item_start if item_start is not None else temporal_extent[0],
                item_end if item_end is not None else temporal_extent[1],
            )

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
            area = area_in_square_meters(shapely.geometry.box(*bbox), bbox_crs)
            lonlat_geometry = mapping(shapely.geometry.box(*convert_bbox_to_lat_long(bbox, bbox_crs)))

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
            lonlat_geometry = _bbox_to_geojson(convert_bbox_to_lat_long(bbox, bbox_crs))
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


def assemble_result_metadata(
    *,
    tracer: DryRunDataTracer,
    result: SaveResult,
    job_dir: Path,
    unique_process_ids: Set[str],
    apply_gdal: bool,
    result_cube_metadata: Callable[[SaveResult], ResultCubeMetadata],
    settings: JobResultsSettings,
    summarize_exception: Callable[[Exception], str],
    extract_asset_metadata: Callable[[Dict[str, Any], Dict[str, Any], Path, Optional[int]], None],
    asset_metadata: Optional[Dict] = None,  # TODO: include "items" instead of "assets"
    ml_model_metadata: Optional[Dict] = None,
    is_item: bool = False,
    result_items: Optional[List[dict]] = None,
) -> dict:
    metadata = extract_result_metadata(tracer, stac_items=result_items)

    cube_metadata = result_cube_metadata(result)
    epsg = cube_metadata.epsg
    instruments = cube_metadata.instruments

    if not isinstance(result, NullResult):
        if apply_gdal:
            if is_item:
                items_metadata = dict()
                for item_key, item in asset_metadata.items():
                    temp_asset_metadata = metadata.copy()
                    try:
                        extract_asset_metadata(temp_asset_metadata, item["assets"], job_dir, epsg)
                        items_metadata[item_key] = temp_asset_metadata
                    except Exception as e:
                        error_summary = summarize_exception(e)
                        logger.exception("Error while creating asset metadata: " + error_summary)
                metadata["items"] = items_metadata
            else:
                try:
                    extract_asset_metadata(metadata, asset_metadata, job_dir, epsg)
                except Exception as e:
                    error_summary = summarize_exception(e)
                    logger.exception("Error while creating asset metadata: " + error_summary)
        else:
            if is_item:
                metadata["items"] = asset_metadata
            else:
                metadata["assets"] = asset_metadata

    # _extract_asset_metadata may already fill in metadata["epsg"], but only
    # if the value of epsg was None. So we don't want to overwrite it with
    # None here.
    # TODO: would be better to eliminate this complication.
    if "epsg" not in metadata:
        metadata["epsg"] = epsg

    metadata["instruments"] = instruments
    metadata["processing:facility"] = settings.processing_facility
    metadata["processing:software"] = settings.processing_software
    metadata["unique_process_ids"] = list(unique_process_ids)
    if isinstance(result, SaveResult):
        global_metadata = result.options.get("file_metadata", {})
    metadata["providers"] = global_metadata.get("providers", [])

    if ml_model_metadata is not None:
        metadata["ml_model_metadata"] = ml_model_metadata

    return metadata


def convert_asset_outputs_to_s3_urls(job_metadata: dict, *, output_href: Callable[[str], str]) -> dict:
    """Convert each asset's href value to a URL on S3 in the metadata dictionary."""

    job_metadata = deepcopy(job_metadata)

    def replace_hrefs(assets: dict):
        for asset in assets.values():
            if "href" in asset and not str(asset["href"]).startswith("s3://"):
                asset["href"] = output_href(asset["href"])

    replace_hrefs(job_metadata.get("assets", {}))  # top-level

    for item in job_metadata.get("items", []):  # those nested in items
        replace_hrefs(item.get("assets", {}))

    return job_metadata


def href_from_job_local_path(
    path: Union[os.PathLike, str], *, job_local_href_format: str, s3_bucket_name: Optional[str] = None
) -> str:
    """
    Convert a file path from a local job context
    (e.g. a path on a job-specific mount)
    to a href that also makes sense outside the job context,
    e.g. in the web app context (without the same mounts)
    """
    if job_local_href_format == "s3":
        return to_s3_url(path, s3_bucket_name)
    else:
        # By default, assume job local path is directly usable
        return f"file://{path}"
