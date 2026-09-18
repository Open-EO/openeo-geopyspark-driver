"""
GeoPySpark/JVM-specific translation of an `AssetTable` into an OpenSearch
FixedFeaturesOpenSearchClient.

Thin mechanical translation of the engine-agnostic decisions already made in
`openeogeotrellis.stac.asset_table.build_asset_table` into JVM calls. No
decision logic lives here.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Tuple

from openeogeotrellis.stac.asset_table import AssetTable

logger = logging.getLogger(__name__)


def build_opensearch_features(asset_table: AssetTable, jvm: Any) -> Tuple[Any, Dict[str, str]]:
    """
    Translate an `AssetTable` into a populated JVM FixedFeaturesOpenSearchClient.

    Returns:
        opensearch_client: populated JVM FixedFeaturesOpenSearchClient
        opensearch_link_titles_map: mapping of band name -> link title (for granule_metadata bands)
    """
    opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

    for item in asset_table.items:
        builder = (
            jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
            .withId(item.item_id)
            .withCollectionId(item.collection_id)
            .withNominalDate(item.nominal_date)
        )

        for link in item.links:
            if link.data_type is not None:
                if link.nodata is not None:
                    builder = builder.addLink(
                        link.href,  # scala arg `href: String`
                        link.asset_id,  # scala arg `title: String`
                        link.pixel_value_scale,  # scala arg `pixelValueScale: Double`
                        link.pixel_value_offset,  # scala arg `pixelValueOffset: Double`
                        link.band_names,  # scala arg `bandNames: java.util.List[String]`
                        link.data_type,
                        link.nodata,
                    )
                else:
                    builder = builder.addLink(
                        link.href,
                        link.asset_id,
                        link.pixel_value_scale,
                        link.pixel_value_offset,
                        link.band_names,
                        link.data_type,
                    )
            else:
                builder = builder.addLink(
                    link.href,
                    link.asset_id,
                    link.pixel_value_scale,
                    link.pixel_value_offset,
                    link.band_names,
                )

        for metadata_link in item.metadata_links:
            builder = builder.addLink(metadata_link.href, metadata_link.asset_id, metadata_link.band_names)

        if item.crs_epsg:
            builder = builder.withCRS(f"EPSG:{item.crs_epsg}")
        if item.raster_extent:
            builder = builder.withRasterExtent(*item.raster_extent)
        if item.resolution is not None:
            builder = builder.withResolution(item.resolution)
        if item.bbox_wsen:
            builder = builder.withBBox(*item.bbox_wsen)
        if item.geometry_wkt is not None:
            builder = builder.withGeometryFromWkt(item.geometry_wkt)
        if item.self_url:
            builder = builder.withSelfUrl(item.self_url)

        logger.debug(f"opensearch.addFeature {item.item_id=}")
        opensearch_client.addFeature(builder.build())

    return opensearch_client, asset_table.opensearch_link_titles_map
