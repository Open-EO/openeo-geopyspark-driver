"""
STAC source resolution and per-asset analysis behind `load_stac`.

The modules in this package hold the STAC-side decision logic: resolving a URL
to a set of STAC Items, deciding per asset which bands/scaling/datatype apply,
and deriving the output grid. Building the actual GeoPySpark datacube from
those decisions lives in `openeogeotrellis.load_stac` and in this package's
`geopyspark_features` / `pyramid_factory` modules, which are the only two that
depend on the JVM.
"""

from openeogeotrellis.stac.asset_table import AssetTable, AssetTableItem, build_asset_table
from openeogeotrellis.stac.exceptions import LoadStacException
from openeogeotrellis.stac.item_collection import (
    ItemCollection,
    LiveStacSourceResolver,
    OwnJobStacSourceResolver,
    StacSource,
    StacSourceResolver,
    construct_item_collection,
)
from openeogeotrellis.stac.target_grid import TargetGrid, select_target_grid

__all__ = [
    "AssetTable",
    "AssetTableItem",
    "ItemCollection",
    "LiveStacSourceResolver",
    "LoadStacException",
    "OwnJobStacSourceResolver",
    "StacSource",
    "StacSourceResolver",
    "TargetGrid",
    "build_asset_table",
    "construct_item_collection",
    "select_target_grid",
]
