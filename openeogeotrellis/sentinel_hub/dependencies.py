import logging
from typing import Optional

import shapely.geometry.base
from shapely.geometry import Polygon

from openeo_driver.datacube import DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.utm import area_in_square_meters
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata


class SentinelHubDependencies:

    @classmethod
    def schedule_for_load_collection(
        cls,
        collection_id: str,
        properties_criteria,
        constraints: dict,
        job_id: str,
        job_options: dict,
        sentinel_hub_client_alias: str,
        logger_adapter: logging.LoggerAdapter,
        jvm,
        vault,
        default_sentinel_hub_client_id: Optional[str],
        default_sentinel_hub_client_secret: Optional[str],
        get_vault_token,
        catalog,
        batch_request_cache: dict,
    ) -> Optional[dict]:
        """
        Schedule Sentinel Hub batch processing for a load_collection source constraint.
        Returns a job dependency dict if a batch process was scheduled, or None otherwise.
        """
        band_names = constraints.get('bands')

        metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata(collection_id))
        if band_names:
            metadata = metadata.filter_bands(band_names)

        layer_source_info = metadata.get("_vito", "data_source")
        sar_backscatter_compatible = layer_source_info.get("sar_backscatter_compatible", False)

        if "sar_backscatter" in constraints and not sar_backscatter_compatible:
            raise OpenEOApiException(message=
                                     """Process "sar_backscatter" is not applicable for collection {c}."""
                                     .format(c=collection_id), status_code=400)

        if layer_source_info['type'] != 'sentinel-hub':
            return None

        spatial_extent = constraints['spatial_extent']
        crs = spatial_extent['crs']

        def get_geometries():
            return (constraints.get("aggregate_spatial", {}).get("geometries") or
                    constraints.get("filter_spatial", {}).get("geometries"))

        def area() -> float:
            def bbox_area() -> float:
                geom = Polygon.from_bounds(
                    xmin=spatial_extent['west'],
                    ymin=spatial_extent['south'],
                    xmax=spatial_extent['east'],
                    ymax=spatial_extent['north'])

                return area_in_square_meters(geom, crs)

            geometries = get_geometries()

            if not geometries:
                return bbox_area()
            elif isinstance(geometries, DelayedVector):
                # TODO: can this case and the next be replaced with a combination of to_projected_polygons
                #  and ProjectedPolygons#areaInSquareMeters?
                return (jvm
                        .org.openeo.geotrellis.ProjectedPolygons.fromVectorFile(geometries.path)
                        .areaInSquareMeters())
            elif isinstance(geometries, DriverVectorCube):
                return geometries.get_area()
            elif isinstance(geometries, shapely.geometry.base.BaseGeometry):
                return area_in_square_meters(geometries, crs)
            else:
                logger_adapter.error(f"GpsBatchJobs._scheduled_sentinelhub_batch_processes:area Unhandled geometry type {type(geometries)}")
                raise ValueError(geometries)

        actual_area = area()
        absolute_maximum_area = 1e+12  # 1 million km²

        if actual_area > absolute_maximum_area:
            raise OpenEOApiException(message=
                                     "Requested area {a} m² for collection {c} exceeds maximum of {m} m²."
                                     .format(a=actual_area, c=collection_id, m=absolute_maximum_area),
                                     status_code=400)

        endpoint = layer_source_info['endpoint']
        supports_batch_processes = (endpoint.startswith("https://services.sentinel-hub.com") or
                                    endpoint.startswith("https://services-uswest2.sentinel-hub.com"))

        if not supports_batch_processes:  # always sync approach
            logger_adapter.info("endpoint {e} does not support batch processing".format(e=endpoint))
            return None
        else:
            logger_adapter.info("this backend does not support polling for batch processes")
            return None
