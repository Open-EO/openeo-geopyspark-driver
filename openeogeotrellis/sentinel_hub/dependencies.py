import logging
import os
import uuid
from typing import Dict, Optional

import shapely.geometry.base
from py4j.protocol import Py4JJavaError
from shapely.geometry import Polygon

from openeo.util import deep_get, dict_no_none
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.utm import area_in_square_meters
from openeo_driver.utils import generate_unique_id, to_hashable
from openeogeotrellis import sentinel_hub
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata
from openeogeotrellis.sentinel_hub.batchprocessing import SentinelHubBatchProcessing
from openeogeotrellis.utils import (
    normalize_temporal_extent,
    to_projected_polygons,
    BadlyHashable,
)


class SentinelHubDependencies:

    @classmethod
    def schedule_for_load_collection(
        cls,
        supports_async_tasks: bool,
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

        sar_backscatter_arguments: Optional[SarBackscatterArgs] = (
            constraints.get("sar_backscatter", SarBackscatterArgs()) if sar_backscatter_compatible
            else None
        )

        card4l = (sar_backscatter_arguments is not None
                  and sar_backscatter_arguments.coefficient == "gamma0-terrain"
                  and sar_backscatter_arguments.mask
                  and sar_backscatter_arguments.local_incidence_angle)

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

        def large_area() -> bool:
            batch_process_threshold_area = 50 * 1000 * 50 * 1000  # 50x50 km²
            large_enough = actual_area >= batch_process_threshold_area

            logger_adapter.info("deemed collection {c} AOI ({a} m²) {s} for batch processing (threshold {t} m²)"
                        .format(c=collection_id, a=actual_area,
                                s="large enough" if large_enough else "too small",
                                t=batch_process_threshold_area))

            return large_enough

        endpoint = layer_source_info['endpoint']
        supports_batch_processes = (endpoint.startswith("https://services.sentinel-hub.com") or
                                    endpoint.startswith("https://services-uswest2.sentinel-hub.com"))

        shub_input_approach = deep_get(job_options, 'sentinel-hub', 'input', default=None)

        if not supports_batch_processes:  # always sync approach
            logger_adapter.info("endpoint {e} does not support batch processing".format(e=endpoint))
            return None
        elif not supports_async_tasks:  # always sync approach
            logger_adapter.info("this backend does not support polling for batch processes")
            return None
        elif card4l:  # always batch approach
            logger_adapter.info("deemed collection {c} request CARD4L compliant ({s})"
                                .format(c=collection_id, s=sar_backscatter_arguments))
        elif shub_input_approach == 'sync':
            logger_adapter.info("forcing sync input processing for collection {c}".format(c=collection_id))
            return None
        elif shub_input_approach == 'batch':
            logger_adapter.info("forcing batch input processing for collection {c}".format(c=collection_id))
        elif not large_area():  # 'auto'
            return None  # skip SHub batch process and use sync approach instead

        sample_type = jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
            layer_source_info.get('sample_type', 'UINT16'))

        from_date, to_date = normalize_temporal_extent(constraints['temporal_extent'])

        west = spatial_extent['west']
        south = spatial_extent['south']
        east = spatial_extent['east']
        north = spatial_extent['north']
        bbox = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))

        bucket_name = layer_source_info.get('bucket', sentinel_hub.OG_BATCH_RESULTS_BUCKET)

        logger_adapter.debug(f"Sentinel Hub client alias: {sentinel_hub_client_alias}")

        if sentinel_hub_client_alias == 'default':
            sentinel_hub_client_id = default_sentinel_hub_client_id
            sentinel_hub_client_secret = default_sentinel_hub_client_secret
        else:
            sentinel_hub_client_id, sentinel_hub_client_secret = (
                vault.get_sentinel_hub_credentials(sentinel_hub_client_alias,
                                                   get_vault_token(sentinel_hub_client_alias)))

        batch_processing_service = (
            SentinelHubBatchProcessing.get_batch_processing_service(
                endpoint=endpoint,
                bucket_name=bucket_name,
                sentinel_hub_client_id=sentinel_hub_client_id,
                sentinel_hub_client_secret=sentinel_hub_client_secret,
                sentinel_hub_client_alias=sentinel_hub_client_alias,
                jvm=jvm,
            )
        )

        shub_band_names = metadata.band_names

        if sar_backscatter_arguments and sar_backscatter_arguments.mask:
            shub_band_names.append('dataMask')

        if sar_backscatter_arguments and sar_backscatter_arguments.local_incidence_angle:
            shub_band_names.append('localIncidenceAngle')

        def metadata_properties_from_criteria() -> Dict[str, Dict[str, object]]:
            def as_dicts(criteria):
                return {criterion[0]: criterion[1] for criterion in criteria}  # (operator -> value)

            metadata_properties_return = {property_name: as_dicts(criteria) for property_name, criteria in properties_criteria}
            sentinel_hub.assure_polarization_from_sentinel_bands(metadata,
                                                                 metadata_properties_return, job_id)
            return metadata_properties_return

        metadata_properties = metadata_properties_from_criteria()

        geometries = get_geometries()

        if not geometries:
            geometry = bbox
            # string crs is unchanged
        else:
            projected_polygons = to_projected_polygons(jvm, geometry=geometries, crs=crs, buffer_points=True)
            geometry = projected_polygons.polygons()
            crs = projected_polygons.crs()

        if not geometries:
            hashable_geometry = (bbox.xmin(), bbox.ymin(), bbox.xmax(), bbox.ymax())
        elif isinstance(geometries, DelayedVector):
            hashable_geometry = geometries.path
        else:
            hashable_geometry = BadlyHashable(geometries)

        collecting_folder: Optional[str] = None

        if card4l:
            # TODO: not obvious but this does the validation as well
            dem_instance = sentinel_hub.processing_options(collection_id, sar_backscatter_arguments)\
                .get('demInstance')

            # these correspond to the .start_card4l_batch_processes arguments
            batch_request_cache_key = (
                collection_id,  # for 'collection_id' and 'dataset_id'
                hashable_geometry,
                str(crs),
                from_date,
                to_date,
                to_hashable(shub_band_names),
                dem_instance,
                to_hashable(metadata_properties)
            )

            batch_request_ids, subfolder = batch_request_cache.get(batch_request_cache_key, (None, None))

            if batch_request_ids is None:
                # cannot be the batch job ID because results for multiple collections would end up in
                #  the same S3 dir
                request_group_uuid = str(uuid.uuid4())
                subfolder = request_group_uuid

                # return type py4j.java_collections.JavaList is not JSON serializable
                batch_request_ids = list(batch_processing_service.start_card4l_batch_processes(
                    layer_source_info['collection_id'],
                    layer_source_info['dataset_id'],
                    geometry,
                    crs,
                    from_date,
                    to_date,
                    shub_band_names,
                    dem_instance,
                    metadata_properties,
                    subfolder,
                    request_group_uuid)
                )

                batch_request_cache[batch_request_cache_key] = (batch_request_ids, subfolder)

                logger_adapter.info("saved newly scheduled CARD4L batch processes {b} for near future use"
                            " (key {k!r})".format(b=batch_request_ids, k=batch_request_cache_key))
            else:
                logger_adapter.debug("recycling saved CARD4L batch processes {b} (key {k!r})".format(
                    b=batch_request_ids, k=batch_request_cache_key))
        else:
            shub_caching_flag = deep_get(job_options, 'sentinel-hub', 'cache-results', default=None)
            try_cache = (ConfigParams().cache_shub_batch_results if shub_caching_flag is None  # auto
                         else shub_caching_flag)
            can_cache = layer_source_info.get('cacheable', False)
            cache = try_cache and can_cache

            processing_options = (sentinel_hub.processing_options(collection_id, sar_backscatter_arguments)
                                  if sar_backscatter_arguments else {})

            # these correspond to the .start_batch_process/start_batch_process_cached arguments
            batch_request_cache_key = (
                collection_id,  # for 'collection_id', 'dataset_id' and sample_type
                hashable_geometry,
                str(crs),
                from_date,
                to_date,
                to_hashable(shub_band_names),
                to_hashable(metadata_properties),
                to_hashable(processing_options)
            )

            if cache:
                (batch_request_id, subfolder,
                 collecting_folder) = batch_request_cache.get(batch_request_cache_key, (None, None, None))

                if collecting_folder is None:
                    subfolder = generate_unique_id()  # batch process context JSON is written here as well

                    # collecting_folder must be writable from driver (cached tiles) and async_task
                    # handler (new tiles))
                    collecting_folder = f"/tmp_epod/openeo_collecting/{subfolder}"
                    os.mkdir(collecting_folder)
                    os.chmod(collecting_folder, mode=0o770)  # umask prevents group write

                    batch_request_id = batch_processing_service.start_batch_process_cached(
                        layer_source_info['collection_id'],
                        layer_source_info['dataset_id'],
                        geometry,
                        crs,
                        from_date,
                        to_date,
                        shub_band_names,
                        sample_type,
                        metadata_properties,
                        processing_options,
                        subfolder,
                        collecting_folder
                    )

                    batch_request_cache[batch_request_cache_key] = (batch_request_id, subfolder,
                                                                    collecting_folder)

                    logger_adapter.info("saved newly scheduled cached batch process {b} for near future use"
                                " (key {k!r})".format(b=batch_request_id, k=batch_request_cache_key))
                else:
                    logger_adapter.debug("recycling saved cached batch process {b} (key {k!r})".format(
                        b=batch_request_id, k=batch_request_cache_key))

                batch_request_ids = [batch_request_id]
            else:
                try:
                    batch_request_id = batch_request_cache.get(batch_request_cache_key)

                    if batch_request_id is None:
                        batch_request_id = batch_processing_service.start_batch_process(
                            layer_source_info['collection_id'],
                            layer_source_info['dataset_id'],
                            geometry,
                            crs,
                            from_date,
                            to_date,
                            shub_band_names,
                            sample_type,
                            metadata_properties,
                            processing_options
                        )

                        batch_request_cache[batch_request_cache_key] = batch_request_id

                        logger_adapter.info("saved newly scheduled batch process {b} for near future use"
                                    " (key {k!r})".format(b=batch_request_id, k=batch_request_cache_key))
                    else:
                        logger_adapter.debug("recycling saved batch process {b} (key {k!r})".format(
                            b=batch_request_id, k=batch_request_cache_key))

                    subfolder = batch_request_id
                    batch_request_ids = [batch_request_id]
                except Py4JJavaError as e:
                    java_exception = e.java_exception

                    if (java_exception.getClass().getName() ==
                            'org.openeo.geotrellissentinelhub.BatchProcessingService$NoSuchFeaturesException'):
                        raise OpenEOApiException(
                            message=f"{java_exception.getClass().getName()}: {java_exception.getMessage()}",
                            status_code=400)
                    else:
                        raise e

        return dict_no_none(
            collection_id=collection_id,
            batch_request_ids=batch_request_ids,  # to poll SHub
            collecting_folder=collecting_folder,  # temporary cached and new single band tiles, also a flag
            results_location=f"s3://{bucket_name}/{subfolder}",  # new multiband tiles
            card4l=card4l  # should the batch job expect CARD4L metadata?
        )