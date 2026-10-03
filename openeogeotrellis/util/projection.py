from typing import Tuple

import pyproj
from openeo_driver.util.geometry import reproject_bounding_box
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from shapely.geometry import box


def is_utm_epsg_code(epsg: int) -> bool:
    """Is given EPSG code a UTM CRS?"""
    return isinstance(epsg, int) and ((32601 <= epsg <= 32660) or (32701 <= epsg <= 32760))


def reproject_cellsize(
        spatial_extent: dict,
        input_resolution: tuple,
        input_crs: str,
        to_crs: str,
) -> Tuple[float, float]:
    """
    :param spatial_extent: The spatial extent is needed, because conversion is often
    different when done at the poles compared to the equator.
    eg: When converting 1meter to degrees (in LatLon) at the North Pole, it can be way more degrees more in LatLon
    compared to the same conversion at the equator.
    :param input_resolution:
    :param input_crs:
    :param to_crs:
    """
    if "crs" not in spatial_extent:
        spatial_extent = spatial_extent.copy()
        spatial_extent["crs"] = "EPSG:4326"
    west, south = spatial_extent["west"], spatial_extent["south"]
    east, north = spatial_extent["east"], spatial_extent["north"]
    spatial_extent_shaply = box(west, south, east, north)
    if to_crs == "Auto42001" or input_crs == "Auto42001":
        # Find correct UTM zone
        utm_zone_crs = auto_utm_epsg_for_geometry(spatial_extent_shaply, spatial_extent["crs"])
        if to_crs == "Auto42001":
            to_crs = utm_zone_crs
        if input_crs == "Auto42001":
            input_crs = utm_zone_crs

    p = spatial_extent_shaply.representative_point()
    transformer = pyproj.Transformer.from_crs(spatial_extent["crs"], input_crs, always_xy=True)
    x, y = transformer.transform(p.x, p.y)

    cell_bbox = {
        "west": x,
        "east": x + input_resolution[0],
        "south": y,
        "north": y + input_resolution[1],
        "crs": input_crs
    }
    cell_bbox_reprojected = reproject_bounding_box(cell_bbox, from_crs=cell_bbox["crs"], to_crs=to_crs)

    cell_width_reprojected = abs(cell_bbox_reprojected["east"] - cell_bbox_reprojected["west"])
    cell_height_reprojected = abs(cell_bbox_reprojected["north"] - cell_bbox_reprojected["south"])

    return cell_width_reprojected, cell_height_reprojected
