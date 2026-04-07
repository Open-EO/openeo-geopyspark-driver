import logging
from typing import Dict, Optional, Union, Tuple
from openeo_driver.util.geometry import BoundingBox
from openeogeotrellis.load_stac import _spatiotemporal_extent_from_load_params, construct_item_collection

import pystac

logger = logging.getLogger(__name__)

def item_collection_from_stac_query(
        url: str,
        spatial_extent: Union[Dict, BoundingBox, None],
        temporal_extent: Tuple[Optional[str], Optional[str]],
) -> pystac.ItemCollection:
    """
    Construct pystac ItemCollection from given load_stac URL.
    Note that this, function returns `pystac's ItemCollection`_ type,
    unlike :func:`openeogeotrellis.load_stac.construct_item_collection`, which returns the local implementation
    :func:`openeogeotrellis.load_stac.ItemCollection` serving a similar purpose.
    :func:`openeogeotrellis.load_stac.construct_item_collection` which is used internally by this function.

    .. _pystac ItemCollection: https://pystac.readthedocs.io/en/latest/api/pystac.html#pystac.ItemCollection
    """
    spatiotemporal_extent = _spatiotemporal_extent_from_load_params(
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
    )
    property_filter_pg_map = None
    item_collection, *_tail = construct_item_collection(
        url=url,
        spatiotemporal_extent=spatiotemporal_extent,
        property_filter_pg_map=property_filter_pg_map,
    )
    logger.info(f"Query to '{url}' with spatial_extent '{spatial_extent}' and temporal_extent '{temporal_extent}'")
    return pystac.ItemCollection(item_collection.items)
