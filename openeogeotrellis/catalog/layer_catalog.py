import datetime
import logging
import math
from typing import Optional, Tuple, Union

import dateutil.parser
import flask
import pytz

from openeo.util import deep_get, str_truncate
from openeo_driver.backend import CollectionCatalog, LoadParameters, QueryablesListing
from openeo_driver.errors import OpenEOApiException

from openeogeotrellis.util.datetime import normalize_temporal_extent

from .collection_metadata import GeopysparkCubeMetadata
from .validation import check_missing_products

logger = logging.getLogger(__name__)


def _parse_approximate_isoduration(s: str) -> datetime.timedelta:
    """
    Parse the ISO8601 duration as years,months,weeks,days, hours,minutes,seconds.
    Approximate, because it does not care about leap years, months with different number of days, etc.
    Examples: "PT1H30M15.460S", "P5DT4M", "P2WT3H", "P1D"
    Based on: https://stackoverflow.com/questions/36976138/is-there-an-easy-way-to-convert-iso-8601-duration-to-timedelta
    """

    def get_isosplit(s_arg, split):
        if split in s_arg:
            n, s_arg = s_arg.split(split, 1)
        else:
            n = '0'
        return float(n.replace(',', '.')), s_arg  # to handle like "P0,5Y"

    s = s.split('P', 1)[-1]  # Remove prefix
    # M can mean month or minute, so we split the day and time part:
    if 'T' in s:
        s_date0, s_time0 = s.split('T', 1)
    else:
        s_date0 = s
        s_time0 = ''
    s_date, s_time = s_date0, s_time0
    s_yr, s_date = get_isosplit(s_date, 'Y')  # Step through letter dividers
    s_mo, s_date = get_isosplit(s_date, 'M')
    s_wk, s_date = get_isosplit(s_date, 'W')
    s_dy, s_date = get_isosplit(s_date, 'D')

    s_hr, s_time = get_isosplit(s_time, 'H')
    s_mi, s_time = get_isosplit(s_time, 'M')
    s_sc, s_time = get_isosplit(s_time, 'S')
    n_yr = s_yr * 365  # approx days for year, month, week
    n_mo = s_mo * 30.4  # Average days per month
    n_wk = s_wk * 7
    dt = datetime.timedelta(days=n_yr + n_mo + n_wk + s_dy, hours=s_hr, minutes=s_mi,
                            seconds=s_sc)
    return dt


class LayerCatalog(CollectionCatalog):
    """Collection metadata, without any engine-specific loading."""

    def resolve_merged_by_common_name(
        self, collection_id: str, metadata: GeopysparkCubeMetadata, load_params: LoadParameters,
        temporal_extent: Tuple[str, str], spatial_extent: dict
    ) -> GeopysparkCubeMetadata:
        upstream_metadatas = [GeopysparkCubeMetadata(self.get_collection_metadata(cid))
                              for cid in metadata.get("_vito", "data_source", "merged_collections")]
        # Check sources in order of priority and skip ones where we can detect missing products.
        for m in sorted(upstream_metadatas, key=lambda m: m.common_name_priority(), reverse=True):
            if m.get("_vito", "data_source", "check_missing_products"):
                missing = check_missing_products(
                    collection_metadata=m,
                    temporal_extent=temporal_extent, spatial_extent=spatial_extent,
                    properties=load_params.properties,
                )
                if missing:
                    logger.info(
                        f"(common_name) {collection_id!r}: skipping {m.provider_backend()!r} because of {len(missing)} missing products: {str_truncate(repr(missing), 1000)}"
                    )
                    continue
            logger.info(f"(common_name) {collection_id!r}: using {m.provider_backend()!r}.")
            return m

        raise OpenEOApiException(message=f"No fitting provider:backend found for {collection_id!r}")

    def native_crs(self, metadata: GeopysparkCubeMetadata) -> str:
        dimension_crss = [d.crs for d in metadata.spatial_dimensions]

        if len(dimension_crss) > 0:
            crs = dimension_crss[0]
            if isinstance(crs, dict):  # PROJJSON
                crs_id = crs['id']
                authority: str = crs_id['authority']
                code: str = crs_id['code']

                if authority.lower() == 'ogc' and code.lower() == 'auto42001':
                    return "UTM"

                if authority.lower() == 'epsg':
                    return f"EPSG:{code}"

                raise NotImplementedError(f"unsupported CRS: {crs}")

            if isinstance(crs, int):  # EPSG code
                return f"EPSG:{crs}"

            raise NotImplementedError(f"unsupported CRS format: {crs} in cube:dimension, provide an int for epsg codes or a projjson dict.")

        return "UTM"  # LANDSAT7_ETM_L2 doesn't have any, for example

    def derive_temporal_extent(
        self, collection_id: str, load_params: LoadParameters
    ) -> Tuple[Optional[str], Optional[str]]:
        metadata_json = self.get_collection_metadata(collection_id=collection_id)
        metadata = GeopysparkCubeMetadata(metadata_json)

        temporal_extent_constraints = load_params.temporal_extent

        # The first temporal interval should encompass the other temporal intervals.
        # The outer bounds are still calculated just in case.
        # https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#temporal-extent-object
        catalog_temporal_extent = metadata.get("extent", "temporal", "interval", default=None)
        outer_bounds = [None, None]
        if catalog_temporal_extent:
            for extent in catalog_temporal_extent:
                if extent[0]:
                    if outer_bounds[0] is None:
                        outer_bounds[0] = extent[0]
                    else:
                        outer_bounds[0] = min(outer_bounds[0], extent[0])
                if extent[1]:
                    if outer_bounds[1] is None:
                        outer_bounds[1] = extent[1]
                    else:
                        outer_bounds[1] = max(outer_bounds[1], extent[1])
        if temporal_extent_constraints is None:
            temporal_extent = outer_bounds
        else:
            # take the intersection of outer_bounds and temporal_extent
            beginnings = []
            if outer_bounds[0]:
                beginnings.append(outer_bounds[0])
            if temporal_extent_constraints[0]:
                beginnings.append(temporal_extent_constraints[0])
            if not beginnings:
                beginnings.append(None)

            ends = []
            if outer_bounds[1]:
                ends.append(outer_bounds[1])
            if temporal_extent_constraints[1]:
                ends.append(temporal_extent_constraints[1])
            if not ends:
                ends.append(None)

            temporal_extent = (
                max(beginnings),  # ISO date is sortable like a string
                min(ends),
            )
        return temporal_extent

    def estimate_number_of_temporal_observations(self,
                                                 collection_id: str,
                                                 load_params: LoadParameters,
                                                 ) -> int:
        temporal_extent = self.derive_temporal_extent(collection_id, load_params)

        metadata_json = self.get_collection_metadata(collection_id=collection_id)
        metadata = GeopysparkCubeMetadata(metadata_json)

        consider_as_singular_time_step = deep_get(metadata_json, "_vito", "data_source",
                                                  "consider_as_singular_time_step", default=False)
        if consider_as_singular_time_step:
            return 1

        # step could be explicitly 'None', so we use 'or' to specify the default
        temporal_step = metadata.get("cube:dimensions", "t", "step", default=None) or "P10D"

        # https://github.com/stac-extensions/datacube?tab=readme-ov-file#temporal-dimension-object
        temporal_step = _parse_approximate_isoduration(temporal_step)
        temporal_step = temporal_step.total_seconds()

        from_date, to_date = normalize_temporal_extent((temporal_extent[0], temporal_extent[1]))
        to_date_parsed = dateutil.parser.parse(to_date).replace(tzinfo=pytz.UTC)
        from_date_parsed = dateutil.parser.parse(from_date).replace(tzinfo=pytz.UTC)
        number_of_temporal_observations = (to_date_parsed - from_date_parsed).total_seconds() / temporal_step
        number_of_temporal_observations = max(math.floor(number_of_temporal_observations), 1)
        return number_of_temporal_observations

    def get_collection_queryables(self, collection_id: Union[str, None]) -> Union[QueryablesListing, flask.Response]:
        metadata = self.get_collection_metadata(collection_id)
        data_source = deep_get(metadata, "_vito", "data_source", default={})
        if data_source.get("type") == "stac" and (url := data_source.get("url")):
            # TODO: for now (experimental phase), we just do naive redirect here.
            #       Instead: proxy+cache this document.
            #       Or include it in (precompiled) layercatalog (#1175)?
            return flask.redirect(location=f"{url}/queryables")

        return super().get_collection_queryables(collection_id=collection_id)
