"""
Target CRS and cell-size determination for load_stac.

Given the resolution/EPSG information collected during per-item/per-asset
analysis, determines:
1. The target EPSG code for the output datacube.
2. The output cell size (width, height) in the units of that CRS.

Both can be overridden by load_params (target_crs / target_resolution).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Set, Tuple

import pyproj

from openeo_driver.util.geometry import BoundingBox
from openeo_driver.util.utm import utm_zone_from_epsg
from openeo_driver.util.geometry import GeometryBufferer

from openeogeotrellis.stac.opensearch_features import ResolutionTracker
from openeogeotrellis.util.projection import is_utm_epsg_code

logger = logging.getLogger(__name__)


def determine_target_epsg(
    *,
    resolution_tracker: ResolutionTracker,
    observed_epsgs: Set[int],
    source_band_names,
    target_bbox: BoundingBox,
) -> int:
    """
    Determine the target EPSG code for the output datacube.

    Priority:
    1. Single EPSG from resolution tracker (all assets agree)
    2. Multiple UTM EPSGs from resolution tracker → best UTM for bbox
    3. Single EPSG from observed (no resolution info)
    4. Multiple UTM EPSGs from observed → best UTM for bbox
    5. Fallback: best UTM for bbox (legacy behaviour, may be ill-defined)
    """
    unique_epsgs, _ = resolution_tracker.finest_for(keys=source_band_names)

    if len(unique_epsgs) == 1:
        [target_epsg] = unique_epsgs
        logger.info(f"{target_epsg=} from {unique_epsgs=}")
    elif unique_epsgs and all(is_utm_epsg_code(e) for e in unique_epsgs):
        target_epsg = target_bbox.best_utm()
        logger.info(f"{target_epsg=} from {unique_epsgs=}")
    elif not unique_epsgs and len(observed_epsgs) == 1:
        # No resolution info available, but all assets agree on a single EPSG (e.g. from item-level proj:code)
        [target_epsg] = observed_epsgs
        logger.info(f"{target_epsg=} from {observed_epsgs=} (no resolution info available)")
    elif not unique_epsgs and observed_epsgs and all(is_utm_epsg_code(e) for e in observed_epsgs):
        target_epsg = target_bbox.best_utm()
        logger.info(f"{target_epsg=} from {observed_epsgs=} (no resolution info available)")
    else:
        # TODO: picking UTM as target, while the source CRSes are not is probably not ideal.
        target_epsg = target_bbox.best_utm()
        logger.warning(f"{target_epsg=} from {unique_epsgs=} {observed_epsgs=}: legacy behavior, but possibly ill-defined")

    return target_epsg


def determine_cell_size(
    *,
    resolution_tracker: ResolutionTracker,
    observed_epsgs: Set[int],
    source_band_names,
    target_epsg: int,
    target_bbox: BoundingBox,
    feature_flags: Dict[str, Any],
) -> Tuple[float, float]:
    """
    Determine the output cell size (width, height) in units of the target CRS.

    Priority:
    1. cellsize_override feature flag
    2. Finest resolution from tracker (exact, from asset metadata)
    3. cellsize_fallback feature flag
    4. Hardcoded 10 m fallback (with unit conversion for non-UTM CRS)
    """
    unique_epsgs, finest_cell_size = resolution_tracker.finest_for(keys=source_band_names)

    cellsize_override = feature_flags.get("cellsize_override")
    cellsize_fallback = feature_flags.get("cellsize_fallback", None)

    if cellsize_override:
        cell_width, cell_height = cellsize_override
    elif finest_cell_size:  # exact resolution from asset metadata
        cell_width, cell_height = finest_cell_size
    elif cellsize_fallback:
        cell_width, cell_height = cellsize_fallback
    elif len(unique_epsgs) == 1 or (not unique_epsgs and len(observed_epsgs) == 1):
        logger.warning(f"cellsize: fallback on hardcoded 10m assumption")
        cell_width, cell_height = (10.0, 10.0)
        # TODO: there is assumption here that cellsize_fallback is given in meter, which is not true in general
        try:
            utm_zone_from_epsg(target_epsg)
        except ValueError:
            # Cannot convert EPSG to UTM zone. Use unit from CRS instead of meters.
            target_bbox_center = target_bbox.as_polygon().centroid
            # TODO: GeometryBufferer.transform_meter_to_crs doesn't work properly in y-dimension
            cell_width = GeometryBufferer.transform_meter_to_crs(
                cell_width, f"EPSG:{target_epsg}", loi=(target_bbox_center.x, target_bbox_center.y)
            )
            cell_height = GeometryBufferer.transform_meter_to_crs(
                cell_height, f"EPSG:{target_epsg}", loi=(target_bbox_center.x, target_bbox_center.y)
            )
    else:
        logger.warning(f"cellsize: fallback on hardcoded 10m assumption")
        cell_width, cell_height = (10.0, 10.0)

    logger.info(
        f"cellsize: {cell_width=} {cell_height=} from {unique_epsgs=} {cellsize_override=} {finest_cell_size=} {cellsize_fallback=}"
    )

    return float(cell_width), float(cell_height)


def apply_load_params_overrides(
    *,
    cell_width: float,
    cell_height: float,
    target_epsg: int,
    target_bbox: BoundingBox,
    load_params: Any,
) -> Tuple[float, float, int]:
    """
    Apply target_resolution and target_crs overrides from load_params.

    Returns updated (cell_width, cell_height, target_epsg).
    """
    if load_params.target_resolution is not None:
        if load_params.target_resolution[0] != 0.0 and load_params.target_resolution[1] != 0.0:
            cell_width = float(load_params.target_resolution[0])
            cell_height = float(load_params.target_resolution[1])

    if load_params.target_crs is not None:
        if (
            load_params.target_resolution is not None
            and load_params.target_resolution[0] != 0.0
            and load_params.target_resolution[1] != 0.0
        ):
            if isinstance(load_params.target_crs, int):
                target_epsg = load_params.target_crs
            elif (
                isinstance(load_params.target_crs, dict)
                and load_params.target_crs.get("id", {}).get("code") == "Auto42001"
            ):
                target_epsg = target_bbox.best_utm()
            else:
                target_epsg = pyproj.CRS.from_user_input(load_params.target_crs).to_epsg()

    return cell_width, cell_height, target_epsg

