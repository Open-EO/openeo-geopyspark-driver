"""
Pyramid factory construction for load_stac.

Given the resolved band names, cell size, and the populated OpenSearch client,
constructs the JVM PyramidFactory (or NetCDFCollection) that will later be
driven by `_build_datacube` to load the actual raster tiles.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from openeo_driver.utils import EvalEnv

from openeogeotrellis.constants import EVAL_ENV_KEY

logger = logging.getLogger(__name__)


def build_pyramid_factory(
    *,
    netcdf_with_time_dimension: bool,
    opensearch_client: Any,
    opensearch_link_titles_map: Dict[str, str],
    source_band_names: List[str],
    requested_band_names: List[str],
    asset_band_names: Optional[List[str]],
    cell_width: float,
    cell_height: float,
    url: str,
    env: EvalEnv,
    jvm: Any,
) -> Any:
    """
    Build and return the JVM PyramidFactory (or NetCDFCollection) for the datacube.

    For NetCDF collections with an embedded time dimension a NetCDFCollection class
    reference is returned.  For all other cases a fully initialised PyramidFactory
    instance is returned.
    """
    if netcdf_with_time_dimension:
        # TODO: avoid `asset_band_names` as it is ill-defined here (outside its original for-loop scoped life cycle)
        if asset_band_names:  # When no products are found, asset_band_names is None
            sorted_bands_from_catalog = sorted(asset_band_names)
            if requested_band_names != sorted_bands_from_catalog:
                # TODO: Pass band_names to NetCDFCollection, just like PyramidFactory.
                logger.warning(
                    f"load_stac: Band order should be alphabetical for NetCDF STAC-catalog with a time dimension. "
                    f"Was {requested_band_names}, but should be {sorted_bands_from_catalog} instead.",
                )
        logger.info("Creating NetCDFCollection pyramid factory")
        return jvm.org.openeo.geotrellis.layers.NetCDFCollection
    else:
        opensearch_link_titles = [opensearch_link_titles_map.get(b, b) for b in source_band_names]
        logger.info(f"Creating PyramidFactory for {len(opensearch_link_titles)} band(s): {opensearch_link_titles}")
        logger.debug(f"{opensearch_link_titles=} (from {source_band_names=} and {opensearch_link_titles_map=})")
        max_soft_errors_ratio = env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0)
        return jvm.org.openeo.geotrellis.file.PyramidFactory(
            opensearch_client,
            url,  # openSearchCollectionId, not important
            opensearch_link_titles,  # openSearchLinkTitles
            None,  # rootPath, not important
            # TODO how does this work? Specifying a cell size without any reference to the corresponding CRS?
            jvm.geotrellis.raster.CellSize(float(cell_width), float(cell_height)),  # maxSpatialResolution
            False,  # experimental
            max_soft_errors_ratio,
        )

