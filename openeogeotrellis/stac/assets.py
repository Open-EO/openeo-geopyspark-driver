"""
Asset-level predicates shared between `item_collection` and `asset_table`.

Split into their own module (rather than living in either of those two)
because `asset_table` imports `ItemCollection` from `item_collection`, so
`item_collection` cannot import asset-level helpers back out of
`asset_table` without an import cycle.
"""
from __future__ import annotations

import logging

import pystac

logger = logging.getLogger(__name__)


def is_supported_raster_mime_type(mime_type: str) -> bool:
    mime_type = mime_type.lower()
    # https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac
    return (
        mime_type.startswith("image/tiff")  # No 'image/tif', only double 'f' in spec
        or mime_type.startswith("image/vnd.stac.geotiff")
        or mime_type.startswith("image/jp2")
        or mime_type.startswith("image/png")
        or mime_type.startswith("image/jpeg")
        or mime_type.startswith("application/x-hdf")  # matches hdf5 and hdf
        or mime_type.startswith("application/x-netcdf")
        or mime_type.startswith("application/netcdf")
    )


def is_band_asset(asset: pystac.Asset) -> bool:
    # TODO: what does this function actually detect?
    #       Name seems to suggest that it's about having necessary band metadata (e.g. a band name)
    #       but implementation also seems to be happy with just being loadable as raster data in some sense.

    # Skip unsupported media types (if known)
    if asset.media_type:
        if asset.media_type == "image/vnd.stac.geotiff; cloud-optimized=true":
            return True
        if not is_supported_raster_mime_type(asset.media_type):
            return False

    # Decide based on role (if known)
    if asset.roles is None:
        pass
    elif len(asset.roles) > 0:
        # https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#list-of-asset-roles
        roles_with_bands = {
            "data",
            "data-mask",
            "snow-ice",
            "land-water",
            "water-mask",
        }
        return bool(roles_with_bands.intersection(asset.roles))
    else:
        logger.warning(f"is_band_asset with {asset.href=}: ignoring empty {asset.roles=}")

    # Fallback based on presence of any band metadata
    return (
        "eo:bands" in asset.extra_fields
        or "bands" in asset.extra_fields  # TODO: built-in "bands" support seems to be scheduled for pystac V2
    )
