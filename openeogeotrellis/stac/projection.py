"""
Projection metadata helpers for load_stac.

Covers extracting and interpreting the STAC Projection Extension fields
(proj:code, proj:epsg, proj:bbox, proj:shape, proj:transform) from STAC
Items and Assets.  This underpins the per-item/per-asset analysis step that
collects CRS and resolution information used later to determine the target
EPSG and cell size of the output datacube.
"""
from __future__ import annotations

import functools
import logging
import re
from typing import Any, Optional, Sequence, Tuple, Union

import pystac

from openeo_driver.util.geometry import BoundingBox

from openeogeotrellis.util.geometry import GridSnapper

logger = logging.getLogger(__name__)

_REGEX_EPSG_CODE = re.compile(r"^EPSG:(\d+)$", re.IGNORECASE)


@functools.lru_cache
def _proj_code_to_epsg(proj_code: str) -> Union[int, None]:
    if isinstance(proj_code, str) and (match := _REGEX_EPSG_CODE.match(proj_code)):
        return int(match.group(1))
    # TODO pass-through integers as-is?
    return None


def get_asset_property(asset: pystac.Asset, field: str) -> Union[Any, None]:
    """
    Helper to get a property directly from asset,
    or from bands metadata embedded in asset metadata (if consistent across all bands).
    """
    if field in asset.extra_fields:
        return asset.extra_fields.get(field)
    if "bands" in asset.extra_fields:
        # TODO: Is it actually ok to look for projection properties at bands level?
        #       See https://github.com/stac-extensions/projection/issues/25
        values = []
        for band in asset.extra_fields["bands"]:
            if field in band and band[field] and band[field] not in values:
                values.append(band.get(field))
        if len(values) == 1:
            return values[0]
        if len(values) > 1:
            # For now, using debug level here instead of warning,
            # as this can be done for each asset, which might be too much
            logger.debug(f"Multiple differing values for {field=} found in asset bands: {values=}")

    return None


class ProjectionMetadata:
    """
    Container of and conversion interface for projection metadata from STAC Projection Extension.
    https://github.com/stac-extensions/projection

    Covering these fields:
    - "proj:code" (preferably, with (less ideal) alternative sources:
        "proj:epsg" (deprecated), "proj:wkt2" or "proj:projjson")
    - "proj:bbox"
    - "proj:shape"
    - "proj:transform"
    """

    # TODO: enforce immutability better (e.g. by implementing through dataclasses/attrs)?
    # TODO: move to more generic geometry/projection utility module for better reuse and cleaner separation?
    # TODO: any added value to leverage projection extension support from pystac in some way?

    __slots__ = ("_code", "_bbox", "_shape", "_transform", "_ref")

    def __init__(
        self,
        *,
        code: Optional[str] = None,
        epsg: Optional[int] = None,
        bbox: Optional[Sequence[float]] = None,
        shape: Optional[Sequence[int]] = None,
        transform: Optional[Sequence[float]] = None,
        # Reference describing where the metadata came from (STAC item, asset, ...)
        ref: Optional[str] = None,
    ):
        # TODO: support wkt2 and projjson as well in some way?
        self._code = code or (f"EPSG:{epsg}" if epsg is not None else None)
        self._bbox = tuple(bbox) if bbox else None
        self._shape = tuple(shape) if shape else None
        self._transform = tuple(transform) if transform else None
        self._ref = ref

    def __repr__(self) -> str:
        return (
            f"ProjectionMetadata(code={self._code!r}, bbox={self._bbox!r}, shape={self._shape!r}, ref={self._ref!r})"
        )

    def _key(self) -> tuple:
        # TODO: use normalized `self.bbox` instead of `self._bbox` + `self._transform`
        #       to also cover equivalence of these two?
        return (self._code, self._shape, self._bbox, self._transform, self._ref)

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other):
        if isinstance(other, ProjectionMetadata):
            return self._key() == other._key()
        return NotImplemented

    @property
    def code(self) -> Union[str, None]:
        return self._code

    @property
    def epsg(self) -> Union[int, None]:
        # Note: The field `proj:epsg` has been deprecated in v1.2.0 of projection extension
        # in favor of `proj:code` and has been removed in v2.0.0.
        return _proj_code_to_epsg(self._code) if self._code else None

    @property
    def bbox(self) -> Union[Tuple[float, float, float, float], None]:
        """
        Bounding box of the assets represented by this Item in the asset data CRS.
        Specified as 4 or 6 coordinates ... e.g., [west, south, east, north], ...
        """
        if self._bbox and len(self._bbox) in {4, 6}:
            # TODO: need for support of 6 values?
            return self._bbox[:4]
        elif self._shape and self._transform:
            # per https://github.com/soxofaan/projection/blob/reformat-best-practices/README.md#projtransform
            a0, a1, a2, a3, a4, a5 = self._transform[:6]

            def project(x: float, y: float) -> Tuple[float, float]:
                return a0 * x + a1 * y + a2, a3 * x + a4 * y + a5

            sy, sx = self._shape
            p00 = project(0, 0)
            px0 = project(sx, 0)
            p0y = project(0, sy)
            pxy = project(sx, sy)
            xs, ys = zip(p00, px0, p0y, pxy)
            return (min(xs), min(ys), max(xs), max(ys))

    def to_bounding_box(self) -> Union[BoundingBox, None]:
        """Get bbox (if any) as BoundingBox object."""
        if bbox := self.bbox:
            return BoundingBox.from_wsen_tuple(bbox, crs=self.code)

    @property
    def shape(self) -> Union[Tuple[int, int], None]:
        """Number of pixels in the most common pixel grid used by the assets (in Y, X order)."""
        if self._shape and len(self._shape) == 2:
            return self._shape
        # TODO: calculate from bbox and transform?

    def resolution(self, *, fail_on_miss: bool = True) -> Union[Tuple[float, float], None]:
        """
        Calculate resolution (xres, yres) expressed as distance in the projection CRS
        based on bbox/shape/transform.
        """
        if self._bbox and self._shape:
            xmin, ymin, xmax, ymax = self._bbox[:4]
            yn, xn = self.shape
            return float(xmax - xmin) / xn, float(ymax - ymin) / yn
        elif self._transform:
            a0, _, _, _, a4, _ = self._transform[:6]
            return abs(a0), abs(a4)

        if fail_on_miss:
            raise ValueError(
                f"Unable to calculate cell size with {self._shape=}, {self._bbox=}, {self._transform=} ({self._ref})"
            )
        else:
            return None

    @classmethod
    def from_item(cls, item: pystac.Item) -> "ProjectionMetadata":
        return cls(
            code=item.properties.get("proj:code"),
            epsg=item.properties.get("proj:epsg"),
            bbox=item.properties.get("proj:bbox"),
            shape=item.properties.get("proj:shape"),
            transform=item.properties.get("proj:transform"),
            ref=f"item {item.id!r}",
        )

    @classmethod
    def from_asset(
        cls,
        asset: pystac.Asset,
        *,
        item: Optional[pystac.Item] = None,
        fix_proj_transform: bool = False,
    ) -> "ProjectionMetadata":
        """
        Extract projection metadata from asset, with fallback to asset bands or (owning) item.
        """
        if item is None:
            item = asset.owner

        def get(field):
            return get_asset_property(asset, field=field) or (item and item.properties.get(field))

        transform = get("proj:transform")
        if fix_proj_transform and transform:
            transform = cls._fix_gdal_ordered_transform(transform)

        ref = f"asset with href={asset.href!r}"
        if item:
            ref += f" from item {item.id!r}"

        return cls(
            code=get("proj:code"),
            epsg=get("proj:epsg"),
            bbox=get("proj:bbox"),
            shape=get("proj:shape"),
            transform=transform,
            ref=ref,
        )

    @staticmethod
    def _fix_gdal_ordered_transform(
        transform: Sequence[float], also_check_yx_transposed: bool = False
    ) -> Sequence[float]:
        """
        Detect and fix a proj:transform that is wrongly in GDAL GetGeoTransform order

            [xOrigin, xPixelSize, xSkew, yOrigin, ySkew, yPixelSize]

        instead of the expected order (rasterio/affine style):

            [xPixelSize, xSkew, xOrigin, ySkew, yPixelSize, yOrigin]

        Detection heuristic (for the common XY oriented case):
        - In rasterio order: (non-zero) pixel sizes are at positions 0 and 4,
          skew values at positions 1 and 3 are small/zero
        - In GDAL order: (non-zero) pixel sizes are at positions 1 and 5,
          skew values at positions 2 and 4 are small/zero

        :param also_check_yx_transposed: whether to also consider the possibility of YX oriented data
        """
        if len(transform) < 6:
            return transform
        t0, t1, t2, t3, t4, t5 = transform[:6]

        # Data consistent with rasterio order?
        rasterio_xy_consistent = (abs(t0) > abs(t1)) and (abs(t3) < abs(t4))
        rasterio_yx_consistent = also_check_yx_transposed and (abs(t0) < abs(t1)) and (abs(t3) > abs(t4))

        # Data consistent with GDAL order?
        gdal_xy_consistent = (abs(t1) > abs(t2)) and (abs(t4) < abs(t5))
        gdal_yx_consistent = also_check_yx_transposed and (abs(t1) < abs(t2)) and (abs(t4) > abs(t5))

        if rasterio_xy_consistent or rasterio_yx_consistent:
            pass
        elif gdal_xy_consistent or gdal_yx_consistent:
            transform = [t1, t2, t0, t4, t5, t3] + list(transform[6:])
        else:
            logger.debug(f"Failed to detect proj:transform order from {transform=}, leaving as-is.")

        return transform

    @functools.lru_cache
    def _snappers(self) -> Tuple[GridSnapper, GridSnapper]:
        """Lazy init of x and y coordinate snappers based on bbox and shape"""
        xres, yres = self.resolution(fail_on_miss=True)
        xmin, ymin, xmax, ymax = self.bbox
        x_snapper = GridSnapper(origin=xmin, resolution=xres)
        y_snapper = GridSnapper(origin=ymin, resolution=yres)
        return x_snapper, y_snapper

    def coverage_for(self, extent: BoundingBox, snap: bool = True) -> Union[BoundingBox, None]:
        """
        Find the coverage (as bounding box) of the given extent
        within the pixel grid defined by this `ProjectionMetadata`,
        including reprojection (if necessary), aligning/snapping to the pixel grid
        and clamping to the bounds.

        Returns None if no intersection or bbox.
        """
        bbox = self.to_bounding_box()
        if not bbox:
            logger.warning(f"coverage_for: missing bbox.")
            return None
        intersection = bbox.intersection(extent)
        if not intersection:
            return None

        if snap:
            x_snapper, y_snapper = self._snappers()
            return BoundingBox(
                west=x_snapper.down(intersection.west),
                south=y_snapper.down(intersection.south),
                east=x_snapper.up(intersection.east),
                north=y_snapper.up(intersection.north),
                crs=self.code,
            )
        else:
            return intersection


def get_proj_metadata(
    asset: pystac.Asset,
    *,
    item: pystac.Item,
    fix_proj_transform: bool = False,
) -> Tuple[Optional[int], Optional[Tuple[float, float, float, float]], Optional[Tuple[int, int]]]:
    """
    Get projection metadata from asset:
    EPSG code (int), bbox (in that EPSG) and number of pixels (rows, cols), if available.
    """
    # TODO: phase out usage and switch to using ProjectionMetadata directly?
    metadata = ProjectionMetadata.from_asset(asset, item=item, fix_proj_transform=fix_proj_transform)
    return metadata.epsg, metadata.bbox, metadata.shape


def compute_cellsize(
    proj_bbox: Tuple[float, float, float, float],
    proj_shape: Tuple[float, float],
) -> Tuple[float, float]:
    # TODO: replace usage with ProjectionMetadata.resolution()?
    xmin, ymin, xmax, ymax = proj_bbox
    rows, cols = proj_shape
    cell_width = (xmax - xmin) / cols
    cell_height = (ymax - ymin) / rows
    return cell_width, cell_height

