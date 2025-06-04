import datetime as dt
import json
import time
from functools import partial
import logging
import os
from typing import Union, Optional, Tuple, Dict, List, Iterable, Any, Set
from urllib.parse import urlparse

import dateutil.parser
import geopyspark as gps
import planetary_computer
import pyproj
import pystac
import pystac_client
import pystac_client.exceptions
from geopyspark import LayerType, TiledRasterLayer
from openeo.util import dict_no_none, Rfc3339
import openeo.metadata
import openeo_driver.backend
from openeo_driver import filter_properties
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.backend import LoadParameters, BatchJobMetadata
from openeo_driver.errors import (
    OpenEOApiException,
    ProcessParameterUnsupportedException,
    JobNotFoundException,
    ProcessParameterInvalidException,
    ProcessParameterRequiredException,
)
from openeo_driver.jobregistry import PARTIAL_JOB_STATUS
from openeo_driver.ProcessGraphDeserializer import DEFAULT_TEMPORAL_EXTENT
from openeo_driver.users import User
from openeo_driver.util.geometry import BoundingBox, GeometryBufferer
from openeo_driver.util.http import requests_with_retry
from openeo_driver.util.utm import utm_zone_from_epsg
from openeo_driver.utils import EvalEnv
from pathlib import Path
from pystac import STACObject
from shapely.geometry import Polygon, shape

from openeogeotrellis import datacube_parameters
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.geopysparkcubemetadata import GeopysparkCubeMetadata
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import normalize_temporal_extent, get_jvm, to_projected_polygons, map_optional, unzip
from openeogeotrellis.integrations.stac import ResilientStacIO

logger = logging.getLogger(__name__)
REQUESTS_TIMEOUT_SECONDS = 60


def load_stac(
    url: str,
    *,
    load_params: LoadParameters,
    env: EvalEnv,
    layer_properties: Optional[Dict[str, object]] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    override_band_names: Optional[List[str]] = None,
    apply_lcfm_improvements: bool = False,
) -> GeopysparkDataCube:
    if override_band_names is None:
        override_band_names = []

    apply_lcfm_improvements = apply_lcfm_improvements or env.get(EVAL_ENV_KEY.LOAD_STAC_APPLY_LCFM_IMPROVEMENTS, False)

    logger.info("load_stac from url {u!r} with load params {p!r}".format(u=url, p=load_params))

    feature_flags = load_params.get("featureflags", {})
    allow_empty_cubes = feature_flags.get("allow_empty_cube", env.get(EVAL_ENV_KEY.ALLOW_EMPTY_CUBES, False))

    no_data_available_exception = OpenEOApiException(message="There is no data available for the given extents.",
                                                     code="NoDataAvailable", status_code=400)
    properties_unsupported_exception = ProcessParameterUnsupportedException("load_stac", "properties")

    all_properties = {**layer_properties, **load_params.properties} if layer_properties else load_params.properties

    user: Optional[User] = env.get("user")

    requested_bbox = BoundingBox.from_dict_or_none(
        load_params.spatial_extent, default_crs="EPSG:4326"
    )

    temporal_extent = load_params.temporal_extent
    from_date, until_date = map(dt.datetime.fromisoformat, normalize_temporal_extent(temporal_extent))
    to_date = (dt.datetime.combine(until_date, dt.time.max, until_date.tzinfo) if from_date == until_date
               else until_date - dt.timedelta(milliseconds=1))

    def intersects_spatiotemporally(itm: pystac.Item) -> bool:
        def intersects_temporally() -> bool:
            nominal_date = itm.datetime or dateutil.parser.parse(itm.properties["start_datetime"])
            return from_date <= nominal_date <= to_date

        def intersects_spatially() -> bool:
            if not requested_bbox or itm.bbox is None:
                return True

            requested_bbox_lonlat = requested_bbox.reproject("EPSG:4326")
            return requested_bbox_lonlat.as_polygon().intersects(
                Polygon.from_bounds(*itm.bbox)
            )

        return intersects_temporally() and intersects_spatially()

    def supports_item_search(coll: pystac.Collection) -> bool:
        # TODO: use pystac_client instead?
        conforms_to = coll.get_root().extra_fields.get("conformsTo", [])
        return any(conformance_class.endswith("/item-search") for conformance_class in conforms_to)

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
        if asset.media_type and not is_supported_raster_mime_type(asset.media_type):
            return False

        # Decide based on role (if known)
        if asset.roles is not None:
            roles_with_bands = {
                "data",
                "data-mask",
                "snow-ice",
                "land-water",
                "water-mask",
            }
            return bool(roles_with_bands.intersection(asset.roles))

        # Fallback based on presence of any band metadata
        return (
            "eo:bands" in asset.extra_fields
            or "bands" in asset.extra_fields  # TODO: built-in "bands" support seems to be scheduled for pystac V2
        )

    def get_band_names(item: pystac.Item, asset: pystac.Asset) -> List[str]:
        # TODO: this whole function can be replaced with
        #       _StacMetadataParser().bands_from_stac_asset(asset=asset).band_names()
        #       once the legacy eo:bands integer index support can be dropped
        #       See https://github.com/Open-EO/openeo-geopyspark-driver/issues/619
        def get_band_name(eo_band) -> str:
            if isinstance(eo_band, dict):
                return eo_band["name"]

            # can also be an index into a list of bands elsewhere.
            logger.warning(
                "load_stac-get_band_names: eo:bands with integer indices. This is deprecated and support will be removed in the future."
            )
            assert isinstance(eo_band, int)
            eo_band_index = eo_band

            eo_bands_location = (
                item.properties if "eo:bands" in item.properties else item.get_collection().summaries.to_dict()
            )
            return get_band_name(eo_bands_location["eo:bands"][eo_band_index])

        if "eo:bands" in asset.extra_fields:
            # TODO: eliminate this special case for that deprecated integer index hack above
            return [get_band_name(eo_band) for eo_band in asset.extra_fields["eo:bands"]]

        return _StacMetadataParser().bands_from_stac_asset(asset=asset).band_names()

    def get_proj_metadata(itm: pystac.Item, asst: pystac.Asset) -> (Optional[int],
                                                                    Optional[Tuple[float, float, float, float]],
                                                                    Optional[Tuple[int, int]]):
        """Returns EPSG code, bbox (in that EPSG) and number of pixels (rows, cols), if available."""

        def to_epsg(proj_code: str) -> Optional[int]:
            prefix = "EPSG:"
            return int(proj_code[len(prefix):]) if proj_code.upper().startswith(prefix) else None

        code = (
            asst.extra_fields.get("proj:code") or itm.properties.get("proj:code") if apply_lcfm_improvements
            else None
        )
        epsg = map_optional(to_epsg, code) or asst.extra_fields.get("proj:epsg") or itm.properties.get("proj:epsg")
        bbox = asst.extra_fields.get("proj:bbox") or itm.properties.get("proj:bbox")

        if not bbox and epsg == 4326:
            bbox = itm.bbox

        shape = asst.extra_fields.get("proj:shape") or itm.properties.get("proj:shape")

        return (epsg,
                tuple(map(float, bbox)) if bbox else None,
                tuple(shape) if shape else None)

    def get_pixel_value_offset(itm: pystac.Item, asst: pystac.Asset) -> float:
        raster_scale = asst.extra_fields.get("raster:scale", itm.properties.get("raster:scale", 1.0))
        raster_offset = asst.extra_fields.get("raster:offset", itm.properties.get("raster:offset", 0.0))

        return raster_offset / raster_scale

    literal_matches = {
        property_name: filter_properties.extract_literal_match(condition, env)
        for property_name, condition in all_properties.items()
    }

    def matches_metadata_properties(itm: pystac.Item) -> bool:
        def operator_value(criterion: Dict[str, object]) -> (str, object):
            if len(criterion) != 1:
                raise ValueError(f'expected a single criterion, was {criterion}')

            (operator, value), = criterion.items()
            return operator, value

        for property_name, criterion in literal_matches.items():
            if property_name not in itm.properties:
                return False

            item_value = itm.properties[property_name]
            operator, criterion_value = operator_value(criterion)

            if operator == 'eq' and item_value != criterion_value:
                return False
            if operator == 'lte' and item_value is not None and item_value > criterion_value:
                return False
            if operator == 'gte' and item_value is not None and item_value < criterion_value:
                return False

        return True

    collection = None
    metadata = None

    netcdf_with_time_dimension = False

    backend_config = get_backend_config()
    poll_interval_seconds = backend_config.job_dependencies_poll_interval_seconds
    max_poll_delay_seconds = backend_config.job_dependencies_max_poll_delay_seconds
    max_poll_time = time.time() + max_poll_delay_seconds

    dependency_job_info = (
        _await_dependency_job(
            url=url,
            user=user,
            batch_jobs=batch_jobs,
            poll_interval_seconds=poll_interval_seconds,
            max_poll_delay_seconds=max_poll_delay_seconds,
            max_poll_time=max_poll_time,
        )
        if user
        else None
    )

    remote_request_info = None
    try:
        if dependency_job_info:
            intersecting_items = []

            for asset_id, asset in batch_jobs.get_result_assets(job_id=dependency_job_info.id,
                                                                user_id=user.user_id).items():
                rfc3339 = Rfc3339(propagate_none=True)
                parse_datetime = partial(rfc3339.parse_datetime, with_timezone=True)

                item_geometry = asset.get("geometry", dependency_job_info.geometry)
                item_bbox = asset.get("bbox", dependency_job_info.bbox)
                item_datetime = parse_datetime(asset.get("datetime"))
                item_start_datetime = None
                item_end_datetime = None

                if not item_datetime:
                    item_start_datetime = parse_datetime(asset.get("start_datetime")) or dependency_job_info.start_datetime
                    item_end_datetime = parse_datetime(asset.get("end_datetime")) or dependency_job_info.end_datetime

                    if item_start_datetime == item_end_datetime:
                        item_datetime = item_start_datetime

                pystac_item = pystac.Item(id=asset_id, geometry=item_geometry, bbox=item_bbox, datetime=item_datetime,
                                          properties=dict_no_none({
                                              "datetime": rfc3339.datetime(item_datetime),
                                              "start_datetime": rfc3339.datetime(item_start_datetime),
                                              "end_datetime": rfc3339.datetime(item_end_datetime),
                                              "proj:epsg": asset.get("proj:epsg"),
                                              "proj:bbox": asset.get("proj:bbox"),
                                              "proj:shape": asset.get("proj:shape"),
                                          }))

                if intersects_spatiotemporally(pystac_item) and "data" in asset.get("roles", []):
                    pystac_asset = pystac.Asset(
                        href=asset["href"],
                        extra_fields={
                            "eo:bands": [{"name": b.name} for b in asset["bands"]]
                            # TODO #1109 #1015 also add common "bands"?
                        },
                    )
                    pystac_item.add_asset(asset_id, pystac_asset)
                    intersecting_items.append(pystac_item)

            band_names = []
        else:
            logger.info(f"load_stac of arbitrary URL {url}")

            stac_object = _await_stac_object(
                url=url,
                poll_interval_seconds=poll_interval_seconds,
                max_poll_delay_seconds=max_poll_delay_seconds,
                max_poll_time=max_poll_time,
            )

            if isinstance(stac_object, pystac.Item):
                if load_params.properties:
                    raise properties_unsupported_exception  # as dictated by the load_stac spec

                item = stac_object
                band_names = _StacMetadataParser().bands_from_stac_item(item=item).band_names()
                intersecting_items = [item] if intersects_spatiotemporally(item) else []
            elif isinstance(stac_object, pystac.Collection) and supports_item_search(stac_object):
                collection = stac_object
                netcdf_with_time_dimension = contains_netcdf_with_time_dimension(collection)
                collection_id = collection.id
                metadata = GeopysparkCubeMetadata(
                    metadata=collection.to_dict(include_self_link=False, transform_hrefs=False)
                )
                root_catalog = collection.get_root()

                band_names = _StacMetadataParser().bands_from_stac_collection(collection=collection).band_names()

                if root_catalog.get_self_href().startswith("https://planetarycomputer.microsoft.com/api/stac/v1"):
                    modifier = planetary_computer.sign_inplace
                    # by default, returns all properties and an invalid STAC Item if fields are specified
                    fields = None
                elif (
                    root_catalog.get_self_href().startswith("https://tamn.snapplanet.io")
                    or root_catalog.get_self_href().startswith("https://stac.eurac.edu")
                    or root_catalog.get_self_href().startswith("https://catalogue.dataspace.copernicus.eu/stac")
                    or root_catalog.get_self_href().startswith("https://pgstac.demo.cloudferro.com")
                ):
                    modifier = None
                    # by default, returns all properties and "none" if fields are specified
                    fields = None
                else:
                    modifier = None
                    # Those now also return all fields by default as well:
                    # https://stac.openeo.vito.be/ and https://stac.terrascope.be
                    fields = None

                client = pystac_client.Client.open(root_catalog.get_self_href(), modifier=modifier)

                cql2_filter = _cql2_filter(
                    client,
                    literal_matches,
                    use_filter_extension=feature_flags.get("use-filter-extension", True),
                )

                search_request = client.search(
                    method="POST" if isinstance(cql2_filter, dict) else "GET",
                    collections=collection_id,
                    bbox=requested_bbox.reproject("EPSG:4326").as_wsen_tuple() if requested_bbox else None,
                    limit=20,
                    datetime=(
                        None
                        if ((temporal_extent is DEFAULT_TEMPORAL_EXTENT) or netcdf_with_time_dimension)
                        else f"{from_date.isoformat().replace('+00:00', 'Z')}/"
                        f"{to_date.isoformat().replace('+00:00', 'Z')}"  # end is inclusive
                    ),
                    filter=cql2_filter,
                    fields=fields,
                )

                if isinstance(cql2_filter, dict):
                    remote_request_info = (f"{search_request.method} {search_request.url} " +
                                           f"with body {json.dumps(search_request.get_parameters())}")
                else:
                    remote_request_info = f"{search_request.method} {search_request.url_with_parameters()}"
                logger.info(f"STAC API request: {remote_request_info}")

                # STAC API might not support Filter Extension so always use client-side filtering as well
                intersecting_items = filter(matches_metadata_properties, search_request.items())
            else:
                assert isinstance(stac_object, pystac.Catalog)  # static Catalog + Collection
                catalog = stac_object
                metadata = GeopysparkCubeMetadata(metadata=catalog.to_dict(include_self_link=False, transform_hrefs=False))

                if load_params.properties:
                    raise properties_unsupported_exception  # as dictated by the load_stac spec

                if isinstance(catalog, pystac.Collection):
                    collection = catalog
                    netcdf_with_time_dimension = contains_netcdf_with_time_dimension(collection)

                band_names = _StacMetadataParser().bands_from_stac_object(obj=stac_object).band_names()

                def intersecting_catalogs(root: pystac.Catalog) -> Iterable[pystac.Catalog]:
                    def intersects_spatiotemporally(coll: pystac.Collection) -> bool:
                        def intersects_spatially(bbox) -> bool:
                            if not requested_bbox:
                                return True

                            requested_bbox_lonlat = requested_bbox.reproject("EPSG:4326")
                            return requested_bbox_lonlat.as_polygon().intersects(
                                Polygon.from_bounds(*bbox)
                            )

                        def intersects_temporally(interval) -> bool:
                            start, end = interval

                            if start is not None and end is not None:
                                return to_date >= start and from_date <= end
                            if start is not None:
                                return to_date >= start
                            if end is not None:
                                return from_date <= end
                            return True

                        bboxes = coll.extent.spatial.bboxes
                        intervals = coll.extent.temporal.intervals

                        if len(bboxes) > 1 and not any(intersects_spatially(bbox) for bbox in bboxes[1:]):
                            return False
                        if len(bboxes) == 1 and not intersects_spatially(bboxes[0]):
                            return False

                        if len(intervals) > 1 and not any(intersects_temporally(interval)
                                                          for interval in intervals[1:]):
                            return False
                        if len(intervals) == 1 and not intersects_temporally(intervals[0]):
                            return False

                        return True

                    if isinstance(root, pystac.Collection) and not intersects_spatiotemporally(root):
                        return []

                    yield root
                    for child in root.get_children():
                        yield from intersecting_catalogs(child)

                intersecting_items = (
                    itm
                    for intersecting_catalog in intersecting_catalogs(root=catalog)
                    for itm in intersecting_catalog.get_items()
                    if intersects_spatiotemporally(itm)
                )

        jvm = get_jvm()

        opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

        stac_bbox = None
        items_found = False
        start_datetime = end_datetime = None
        proj_epsg = None
        proj_bbox = None
        proj_shape = None

        band_cell_size: Dict[str, Tuple[float, float]] = {}  # assumes a band has the same resolution across features/assets
        band_epsgs: Dict[str, Set[int]] = {}


        for itm in intersecting_items:
            items_found = True

            item_start_datetime = dateutil.parser.parse(itm.properties.get("datetime") or itm.properties["start_datetime"])
            item_end_datetime = dateutil.parser.parse(itm.properties.get("datetime") or itm.properties["end_datetime"])

            if not start_datetime or item_start_datetime < start_datetime:
                start_datetime = item_start_datetime
            if not end_datetime or item_end_datetime > end_datetime:
                end_datetime = item_end_datetime

            band_assets = {
                asset_id: asset for asset_id, asset in dict(sorted(itm.assets.items())).items() if is_band_asset(asset)
            }

            builder = (jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
                       .withId(itm.id)
                       .withNominalDate(itm.properties.get("datetime") or itm.properties["start_datetime"]))

            band_names_tracker = NoveltyTracker()
            for asset_id, asset in sorted(
                # Go through assets ordered by asset GSD (from finer to coarser) if possible,
                # falling back on deterministic alphabetical asset_id order.
                # see https://github.com/Open-EO/openeo-geopyspark-driver/pull/1213#discussion_r2107353442
                band_assets.items(),
                key=lambda kv: (
                    float(kv[1].extra_fields.get("gsd") or itm.properties.get("gsd") or 40e6),
                    kv[0],
                ),
            ):
                proj_epsg, proj_bbox, proj_shape = get_proj_metadata(itm, asset)

                asset_band_names_from_metadata = get_band_names(item=itm, asset=asset)
                logger.info(f"from intersecting_items: {itm.id=} {asset_id=} {asset_band_names_from_metadata=}")

                if not load_params.bands:
                    # No user-specified band filtering: follow band names from metadata (if possible)
                    asset_band_names = asset_band_names_from_metadata or [asset_id]
                elif isinstance(load_params.bands, list) and asset_id in load_params.bands:
                    # User-specified asset_id as band name: use that directly
                    asset_band_names = [asset_id]
                elif set(asset_band_names_from_metadata).intersection(load_params.bands or []):
                    # User-specified bands match with band names in metadata
                    asset_band_names = asset_band_names_from_metadata
                else:
                    # No match with load_params.bands in some way -> skip this asset
                    continue

                if band_names_tracker.already_seen(sorted(asset_band_names)):
                    # We've already seen these bands (e.g. at finer GSD), so skip this asset.
                    continue

                for asset_band_name in asset_band_names:
                    if asset_band_name not in band_names:
                        band_names.append(asset_band_name)

                    if proj_bbox and proj_shape:
                        band_cell_size[asset_band_name] = _compute_cellsize(proj_bbox, proj_shape)
                    if proj_epsg:
                        band_epsgs.setdefault(asset_band_name, set()).add(proj_epsg)

                pixel_value_offset = get_pixel_value_offset(itm, asset) if apply_lcfm_improvements else 0.0
                logger.info(
                    f"FeatureBuilder.addlink {itm.id=} {asset_id=} {asset_band_names_from_metadata=} {asset_band_names=}"
                )
                builder = builder.addLink(get_best_url(asset), asset_id, pixel_value_offset, asset_band_names)

            if proj_epsg:
                builder = builder.withCRS(f"EPSG:{proj_epsg}")
            if proj_bbox:
                builder = builder.withRasterExtent(*proj_bbox)

            # TODO: does not seem right conceptually; an Item's assets can have different resolutions (and CRS)
            if proj_bbox and proj_shape:
                cell_width, cell_height = _compute_cellsize(proj_bbox, proj_shape)
                builder = builder.withResolution(cell_width)

            latlon_bbox = BoundingBox.from_wsen_tuple(itm.bbox,4326) if itm.bbox else None
            item_bbox = latlon_bbox
            if proj_bbox is not None and proj_epsg is not None:
                item_bbox = BoundingBox.from_wsen_tuple(proj_bbox, crs=proj_epsg)
                latlon_bbox = item_bbox.reproject(4326)

            if latlon_bbox is not None:
                builder = builder.withBBox(*map(float, latlon_bbox.as_wsen_tuple()))

            if itm.geometry is not None:
                builder = builder.withGeometryFromWkt(str(shape(itm.geometry)))

            opensearch_client.addFeature(builder.build())

            stac_bbox = (item_bbox if stac_bbox is None
                         else BoundingBox.from_wsen_tuple(item_bbox.as_polygon().union(stac_bbox.as_polygon()).bounds,
                                                          stac_bbox.crs))

    except Exception as e:
        if isinstance(e, OpenEOApiException):
            raise e
        elif isinstance(e, pystac_client.exceptions.APIError):
            if( remote_request_info is not None):
                raise OpenEOApiException(
                    message=f"load_stac: error when constructing datacube from {remote_request_info}: {e}.",
                    status_code=400,
                ) from e
            else:
                raise OpenEOApiException(
                    message=f"load_stac: error when constructing datacube from {url}: {e}. Please check the remote catalog. More information can be found in the job logs.",
                    status_code=500,
                ) from e
        else:
            raise OpenEOApiException(
                message=f"load_stac: Error when constructing datacube from {url}: {e}",
                status_code=500,
            ) from e



    if not allow_empty_cubes and not items_found:
        raise no_data_available_exception

    target_bbox = requested_bbox or stac_bbox

    if not target_bbox:
        raise ProcessParameterInvalidException(
            process='load_stac',
            parameter='spatial_extent',
            reason=f'Unable to derive a spatial extent from provided STAC metadata: {url}, '
                   f'please provide a spatial extent.'
            )

    if not metadata:
        metadata = GeopysparkCubeMetadata(metadata={})

    if "x" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="x", extent=[])
    if "y" not in metadata.dimension_names():
        metadata = metadata.add_spatial_dimension(name="y", extent=[])

    metadata = metadata.with_temporal_extent(
        temporal_extent=(
            map_optional(dt.datetime.isoformat, start_datetime) or temporal_extent[0],
            map_optional(dt.datetime.isoformat, end_datetime) or temporal_extent[1],
        ),
        allow_adding_dimension=True,
    )
    # Overwrite band_names because new bands could be detected in stac items:
    metadata = metadata.with_new_band_names(override_band_names or band_names)

    if allow_empty_cubes and not metadata.band_names:
        # no knowledge of bands except for what the user requested
        if load_params.bands:
            metadata = metadata.with_new_band_names(load_params.bands)
        else:
            raise ProcessParameterRequiredException(process="load_stac", parameter="bands")

    if load_params.global_extent is None or len(load_params.global_extent) == 0:
        layer_native_extent = metadata.get_layer_native_extent()
        if layer_native_extent:
            load_params = load_params.copy()
            load_params.global_extent = layer_native_extent.as_dict()

    if load_params.bands:
        metadata = metadata.filter_bands(load_params.bands)

    band_names = metadata.band_names

    if apply_lcfm_improvements:
        logger.info("applying LCFM resolution improvements")

        requested_band_epsgs = [epsgs for band_name, epsgs in band_epsgs.items() if band_name in band_names]
        unique_epsgs = {epsg for epsgs in requested_band_epsgs for epsg in epsgs}
        requested_band_cell_sizes = [size for band_name, size in band_cell_size.items() if band_name in band_names]

        if len(unique_epsgs) == 1 and requested_band_cell_sizes:  # exact resolution
            target_epsg = unique_epsgs.pop()
            cell_widths, cell_heights = unzip(*requested_band_cell_sizes)
            cell_width = min(cell_widths)
            cell_height = min(cell_heights)
        elif len(unique_epsgs) == 1:  # about 10m in given CRS
            target_epsg = unique_epsgs.pop()
            try:
                utm_zone_from_epsg(proj_epsg)
                cell_width = cell_height = 10.0
            except ValueError:
                target_bbox_center = target_bbox.as_polygon().centroid
                cell_width = cell_height = GeometryBufferer.transform_meter_to_crs(
                    10.0, f"EPSG:{proj_epsg}", loi=(target_bbox_center.x, target_bbox_center.y)
                )
        else:  # 10m UTM
            target_epsg = target_bbox.best_utm()
            cell_width = cell_height = 10.0
    else:
        if proj_epsg and proj_bbox and proj_shape:  # exact resolution
            target_epsg = proj_epsg
            cell_width, cell_height = _compute_cellsize(proj_bbox, proj_shape)
        elif proj_epsg:  # about 10m in given CRS
            target_epsg = proj_epsg
            try:
                utm_zone_from_epsg(proj_epsg)
                cell_width = cell_height = 10.0
            except ValueError:
                target_bbox_center = target_bbox.as_polygon().centroid
                cell_width = cell_height = GeometryBufferer.transform_meter_to_crs(
                    10.0, f"EPSG:{proj_epsg}", loi=(target_bbox_center.x, target_bbox_center.y)
                )
        else:  # 10m UTM
            target_epsg = target_bbox.best_utm()
            cell_width = cell_height = 10.0

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

    if netcdf_with_time_dimension:
        pyramid_factory = jvm.org.openeo.geotrellis.layers.NetCDFCollection
    else:
        max_soft_errors_ratio = env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0)

        pyramid_factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
            opensearch_client,
            url,  # openSearchCollectionId, not important
            band_names,  # openSearchLinkTitles
            None,  # rootPath, not important
            jvm.geotrellis.raster.CellSize(cell_width, cell_height),
            False,  # experimental
            max_soft_errors_ratio,
        )

    extent = jvm.geotrellis.vector.Extent(*map(float, target_bbox.as_wsen_tuple()))
    extent_crs = target_bbox.crs

    geometries = load_params.aggregate_spatial_geometries
    if isinstance(geometries, DriverVectorCube) and geometries.geometry_count() == 0:
        geometries = None

    if not geometries:
        projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, extent_crs)
    else:
        projected_polygons = to_projected_polygons(
            jvm, geometries, crs=extent_crs, buffer_points=True
        )

    projected_polygons = getattr(
        getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$"
    ).reproject(projected_polygons, target_epsg)

    metadata_properties = {}
    correlation_id = env.get(EVAL_ENV_KEY.CORRELATION_ID, "")

    data_cube_parameters, single_level = datacube_parameters.create(load_params, env, jvm)
    getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

    tilesize = feature_flags.get("tilesize", None)
    if tilesize:
        getattr(data_cube_parameters, "tileSize_$eq")(tilesize)

    try:
        if netcdf_with_time_dimension:
            pyramid = pyramid_factory.datacube_seq(projected_polygons, from_date.isoformat(), to_date.isoformat(),
                                                   metadata_properties, correlation_id, data_cube_parameters,
                                                   opensearch_client)
        elif single_level:
            if not items_found and allow_empty_cubes:
                pyramid = pyramid_factory.empty_datacube_seq(
                    projected_polygons,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    data_cube_parameters,
                )
            else:
                pyramid = pyramid_factory.datacube_seq(
                    projected_polygons,
                    from_date.isoformat(),
                    to_date.isoformat(),
                    metadata_properties,
                    correlation_id,
                    data_cube_parameters,
                )
        else:
            if requested_bbox:
                extent = jvm.geotrellis.vector.Extent(*map(float, requested_bbox.as_wsen_tuple()))
                extent_crs = requested_bbox.crs
            else:
                extent = jvm.geotrellis.vector.Extent(-180.0, -90.0, 180.0, 90.0)
                extent_crs = "EPSG:4326"

            if not items_found and allow_empty_cubes:
                pyramid = pyramid_factory.empty_pyramid_seq(extent, extent_crs, from_date.isoformat(), to_date.isoformat())
            else:
                pyramid = pyramid_factory.pyramid_seq(
                    extent, extent_crs, from_date.isoformat(), to_date.isoformat(), metadata_properties, correlation_id
                )
    except Exception as e:
        raise OpenEOApiException(
            message=f"load_stac: Error when constructing datacube from {url}: {e}",
            status_code=500,
        ) from e


    metadata = metadata.filter_temporal(from_date.isoformat(), to_date.isoformat())

    metadata = metadata.filter_bbox(
        west=extent.xmin(),
        south=extent.ymin(),
        east=extent.xmax(),
        north=extent.ymax(),
        crs=extent_crs,
    )

    temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
    option = jvm.scala.Option

    # noinspection PyProtectedMember
    levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
        option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
              range(0, pyramid.size())}

    return GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)


def contains_netcdf_with_time_dimension(collection):
    """
    Checks if the STAC collection contains netcdf files with multiple time stamps.
    This collection organization is used for storing small patches of EO data, and requires special loading because the
    default readers will not handle this case properly.

    """
    if collection is not None:
        # we found some collection level metadata
        item_assets = collection.extra_fields.get("item_assets", {})
        dimensions = set([tuple(v.get("dimensions")) for i in item_assets.values() if "cube:variables" in i for v in
                          i.get("cube:variables", {}).values()])
        # this is one way to determine if a time dimension is used, but it does depend on the use of item_assets and datacube extension.
        return len(dimensions) == 1 and "time" in dimensions.pop()
    return False


def get_best_url(asset: pystac.Asset):
    """
    Relevant doc: https://github.com/stac-extensions/alternate-assets
    """
    for key, alternate_asset in asset.extra_fields.get("alternate", {}).items():
        if key in {"local", "s3"}:
            href = alternate_asset["href"]
            # Checking if file exists takes around 10ms on /data/MTDA mounted on laptop
            # Checking if URL exists takes around 100ms on https://services.terrascope.be
            # Checking if URL exists depends also on what Datasource is used in the scala code.
            # That would be hacky to predict here.
            url = urlparse(href)
            # Support paths like "file:///data/MTDA", but also "//data/MTDA" just in case.

            file_path = None
            if url.scheme in ["", "file"]:
                file_path = url.path
            elif url.scheme == "s3":
                file_path = f"/{url.netloc}{url.path}"

            if file_path and Path(file_path).exists():
                logger.debug(f"Using local alternate file path {file_path}")
                return file_path
            else:
                logger.warning(f"Only support file paths as local alternate urls, but found {href}")

    href = asset.get_absolute_href() or asset.href

    return (
        href.replace("s3://eodata/", "/vsis3/EODATA/") if os.environ.get("AWS_DIRECT") == "TRUE"
        else href.replace("s3://eodata/", "/eodata/")
    )


def _compute_cellsize(
    proj_bbox: Tuple[float, float, float, float],
    proj_shape: Tuple[float, float],
) -> Tuple[float, float]:
    xmin, ymin, xmax, ymax = proj_bbox
    rows, cols = proj_shape
    cell_width = (xmax - xmin) / cols
    cell_height = (ymax - ymin) / rows
    return cell_width, cell_height


def extract_own_job_info(
    url: str, user_id: str, batch_jobs: openeo_driver.backend.BatchJobs
) -> Optional[BatchJobMetadata]:
    path_segments = urlparse(url).path.split("/")

    if len(path_segments) < 3:
        return None

    jobs_position_segment, job_id, results_position_segment = path_segments[-3:]
    if jobs_position_segment != "jobs" or results_position_segment != "results":
        return None

    try:
        return batch_jobs.get_job_info(job_id=job_id, user_id=user_id)
    except JobNotFoundException:
        logger.debug(f"job {job_id} does not belong to current user {user_id}", exc_info=True)
        return None


def _await_dependency_job(
    url: str,
    *,
    user: Optional[User] = None,
    batch_jobs: Optional[openeo_driver.backend.BatchJobs] = None,
    poll_interval_seconds: float,
    max_poll_delay_seconds: float,
    max_poll_time: float,
) -> Optional[BatchJobMetadata]:
    def get_dependency_job_info() -> Optional[BatchJobMetadata]:
        return extract_own_job_info(url, user.user_id, batch_jobs) if user and batch_jobs else None

    dependency_job_info = get_dependency_job_info()
    if not dependency_job_info:
        return None

    logger.info(f"load_stac of results of own job {dependency_job_info.id}")

    while True:
        partial_job_status = PARTIAL_JOB_STATUS.for_job_status(dependency_job_info.status)

        logger.debug(f"OpenEO batch job results status of own job {dependency_job_info.id}: {partial_job_status}")

        if partial_job_status in [PARTIAL_JOB_STATUS.ERROR, PARTIAL_JOB_STATUS.CANCELED]:
            logger.error(f"Failing because own OpenEO batch job {dependency_job_info.id} failed")
        elif partial_job_status in [None, PARTIAL_JOB_STATUS.FINISHED]:
            break  # not a partial job result or success: proceed

        # still running: continue polling
        if time.time() >= max_poll_time:
            max_poll_delay_reached_error = (f"OpenEO batch job results dependency of"
                                            f"own job {dependency_job_info.id} was not satisfied after"
                                            f" {max_poll_delay_seconds} s, aborting")

            raise Exception(max_poll_delay_reached_error)

        time.sleep(poll_interval_seconds)

        dependency_job_info = get_dependency_job_info()

    return dependency_job_info


def _await_stac_object(
    url: str,
    *,
    poll_interval_seconds: float,
    max_poll_delay_seconds: float,
    max_poll_time: float,
) -> STACObject:
    session = requests_with_retry(total=5, backoff_factor=0.1, status_forcelist={500, 502, 503, 504})

    while True:
        stac_io = ResilientStacIO(session)
        stac_object = pystac.read_file(href=url, stac_io=stac_io)

        if isinstance(stac_object, pystac.Catalog):
            stac_object._stac_io = stac_io  # TODO: avoid accessing internals (fix pystac)

        partial_job_status = stac_object.to_dict(include_self_link=False, transform_hrefs=False).get("openeo:status")

        logger.debug(f"OpenEO batch job results status of {url}: {partial_job_status}")

        if partial_job_status in [PARTIAL_JOB_STATUS.ERROR, PARTIAL_JOB_STATUS.CANCELED]:
            logger.error(f"Failing because OpenEO batch job with results at {url} failed")
        elif partial_job_status in [None, PARTIAL_JOB_STATUS.FINISHED]:
            break  # not a partial job result or success: proceed

        # still running: continue polling
        if time.time() >= max_poll_time:
            max_poll_delay_reached_error = (f"OpenEO batch job results dependency at {url} was not satisfied after"
                                            f" {max_poll_delay_seconds} s, aborting")

            raise Exception(max_poll_delay_reached_error)

        time.sleep(poll_interval_seconds)

    return stac_object


def _cql2_filter(
    client: pystac_client.Client,
    literal_matches: Dict[str, Dict[str, Any]],
    use_filter_extension: Union[bool, str],
) -> Union[str, dict, None]:
    if use_filter_extension == "cql2-json":  # force POST JSON
        return _cql2_json_filter(literal_matches)

    if use_filter_extension == "cql2-text":  # force GET text
        return _cql2_text_filter(literal_matches)

    if use_filter_extension:  # auto-detect, favor POST
        search_links = client.get_links(rel="search")
        supports_post_search = any(link.extra_fields.get("method") == "POST" for link in search_links)

        return (
            _cql2_json_filter(literal_matches) if supports_post_search
            else _cql2_text_filter(literal_matches)  # assume serves ignores filter if no "search" method advertised
        )

    return None  # explicitly disabled


def _cql2_text_filter(literal_matches: Dict[str, Dict[str, Any]]) -> str:
    cql2_text_formatter = get_jvm().org.openeo.geotrellissentinelhub.Cql2TextFormatter()

    return cql2_text_formatter.format(
        # Cql2TextFormatter won't add necessary quotes so provide them up front
        {f'"properties.{name}"': criteria for name, criteria in literal_matches.items()}
    )


def _cql2_json_filter(literal_matches: Dict[str, Dict[str, Any]]) -> Optional[dict]:
    if len(literal_matches) == 0:
        return None

    operator_mapping = {
        "eq": "=",
        "neq": "<>",
        "lt": "<",
        "lte": "<=",
        "gt": ">",
        "gte": ">=",
    }

    def single_filter(property, operator, value) -> dict:
        cql2_json_operator = operator_mapping.get(operator)

        if cql2_json_operator is None:
            raise ValueError(f"unsupported operator {operator}")

        return {
            "op": cql2_json_operator,
            "args": [
                {"property": f"properties.{property}"},
                value
            ]
        }

    filters = [
        single_filter(property, operator, value)
        for property, criteria in literal_matches.items()
        for operator, value in criteria.items()
    ]

    if len(filters) == 1:
        return filters[0]

    return {
        "op": "and",
        "args": filters,
    }


class _StacMetadataParser:
    """
    Helper to extract openEO metadata from STAC metadata resource
    """

    # TODO: better, more compact name: StacMetadata is a bit redundant, technically we're also not "parsing" here either
    # TODO merge with implementation in openeo-python-client

    class _Bands(list):
        """Internal wrapper for list of ``openeo.metadata.Band`` objects"""

        def __init__(self, bands: Iterable[openeo.metadata.Band]):
            super().__init__(bands)

        def band_names(self) -> List[str]:
            return [band.name for band in self]

    def __init__(self):
        # TODO: toggles for how to handle strictness, warnings, logging, etc
        pass

    def _band_from_eo_bands_metadata(self, data: dict) -> openeo.metadata.Band:
        """Construct band from metadata dict in eo v1.1 style"""
        return openeo.metadata.Band(
            name=data["name"],
            common_name=data.get("common_name"),
            wavelength_um=data.get("center_wavelength"),
        )

    def _band_from_common_bands_metadata(self, data: dict) -> openeo.metadata.Band:
        """Construct band from metadata dict in STAC 1.1 + eo v2 style metadata"""
        return openeo.metadata.Band(
            name=data["name"],
            common_name=data.get("eo:common_name"),
            wavelength_um=data.get("eo:center_wavelength"),
        )

    def bands_from_stac_object(
        self, obj: Union[pystac.Catalog, pystac.Collection, pystac.Item, pystac.Asset]
    ) -> _Bands:
        # Note: first check for Collection, as it is a subclass of Catalog
        if isinstance(obj, pystac.Collection):
            return self.bands_from_stac_collection(collection=obj)
        elif isinstance(obj, pystac.Catalog):
            return self.bands_from_stac_catalog(catalog=obj)
        elif isinstance(obj, pystac.Item):
            return self.bands_from_stac_item(item=obj)
        elif isinstance(obj, pystac.Asset):
            return self.bands_from_stac_asset(asset=obj)
        else:
            raise ValueError(obj)

    def bands_from_stac_catalog(self, catalog: pystac.Catalog) -> _Bands:
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        summaries = catalog.extra_fields.get("summaries", {})
        logger.warning(f"bands_from_stac_catalog with {summaries.keys()=} (which is non-standard)")
        if "eo:bands" in summaries:
            return self._Bands(self._band_from_eo_bands_metadata(b) for b in summaries["eo:bands"])
        elif "bands" in summaries:
            return self._Bands(self._band_from_common_bands_metadata(b) for b in summaries["bands"])

        # TODO: instead of warning: exception, or return None?
        logger.warning("_StacMetadataParser.bands_from_stac_catalog no band name source found")
        return self._Bands([])

    def bands_from_stac_collection(self, collection: pystac.Collection) -> _Bands:
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        logger.info(f"bands_from_stac_collection with {collection.summaries.lists.keys()=}")
        if "eo:bands" in collection.summaries.lists:
            return self._Bands(self._band_from_eo_bands_metadata(b) for b in collection.summaries.lists["eo:bands"])
        elif "bands" in collection.summaries.lists:
            return self._Bands(self._band_from_common_bands_metadata(b) for b in collection.summaries.lists["bands"])

        # TODO: instead of warning: exception, or return None?
        logger.warning("_StacMetadataParser.bands_from_stac_collection no band name source found")
        return self._Bands([])

    def bands_from_stac_item(self, item: pystac.Item) -> _Bands:
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        logger.info(f"bands_from_stac_item with {item.properties.keys()=}")
        if "eo:bands" in item.properties:
            return self._Bands(self._band_from_eo_bands_metadata(b) for b in item.properties["eo:bands"])
        elif "bands" in item.properties:
            return self._Bands(self._band_from_common_bands_metadata(b) for b in item.properties["bands"])
        elif (parent_collection := item.get_collection()) is not None:
            return self.bands_from_stac_collection(collection=parent_collection)

        # TODO: instead of warning: exception, or return None?
        logger.warning("_StacMetadataParser.bands_from_stac_item no band name source found")
        return self._Bands([])

    def bands_from_stac_asset(self, asset: pystac.Asset) -> _Bands:
        # TODO: "eo:bands" vs "bands" priority based on STAC and EO extension version information
        logger.info(f"bands_from_stac_asset with {asset.extra_fields.keys()=}")
        if "eo:bands" in asset.extra_fields:
            return self._Bands(self._band_from_eo_bands_metadata(b) for b in asset.extra_fields["eo:bands"])
        elif "bands" in asset.extra_fields:
            # TODO: avoid extra_fields, but built-in "bands" support seems to be scheduled for pystac V2
            return self._Bands(self._band_from_common_bands_metadata(b) for b in asset.extra_fields["bands"])

        # TODO: instead of warning: exception, or return None?
        logger.warning("_StacMetadataParser.bands_from_stac_asset no band name source found")
        return self._Bands([])


class NoveltyTracker:
    """Utility to detect new things."""

    # TODO: move to more general utility module

    def __init__(self):
        self._seen: set = set()

    def is_new(self, x) -> bool:
        """Check if the item is new (not seen before)."""
        if isinstance(x, list):
            key = tuple(x)
        else:
            # TODO: wider coverage to make the thing hashable
            key = x
        if key in self._seen:
            return False
        else:
            self._seen.add(key)
            return True

    def already_seen(self, x) -> bool:
        """Check if the item was seen before."""
        return not self.is_new(x)
