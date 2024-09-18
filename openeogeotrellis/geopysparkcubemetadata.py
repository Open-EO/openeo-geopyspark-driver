import logging
from typing import List, Union

import dateutil.parser
from openeo.metadata import CollectionMetadata, Dimension, TemporalDimension
from openeogeotrellis.utils import reproject_cellsize

_log = logging.getLogger(__name__)


def clean_number_pair(tuple_to_clean):
    """
    Convert input an (x,y) tuple if possible.
    """
    if not tuple_to_clean:
        return None
    if isinstance(tuple_to_clean, float) or isinstance(tuple_to_clean, int):
        return tuple_to_clean, tuple_to_clean
    if (tuple_to_clean[0] is None) or (tuple_to_clean[1] is None):
        return None
    if isinstance(tuple_to_clean, (tuple, list)) and len(tuple_to_clean) == 2:
        # in case of list, this will make a simple copy
        return tuple_to_clean[0], tuple_to_clean[1]
    # Not able to parse:
    return None


class GeopysparkCubeMetadata(CollectionMetadata):
    """
    GeoPySpark Cube metadata (additional tracking of spatial and temporal extent
    """

    # TODO move to python driver?

    def __init__(
            self, metadata: dict, dimensions: List[Dimension] = None,
            spatial_extent: dict = None, temporal_extent: tuple = None
    ):
        super().__init__(metadata=metadata, dimensions=dimensions)
        # TODO: why do we need these in addition to those in dimensions?
        # TODO: normalize extents or user proper types altogether
        self._spatial_extent = spatial_extent
        self._temporal_extent = temporal_extent
        if self.has_temporal_dimension() and temporal_extent is not None:
            self.temporal_dimension.extent = temporal_extent

    def _clone_and_update(
            self, metadata: dict = None, dimensions: List[Dimension] = None,
            spatial_extent: dict = None, temporal_extent: tuple = None, **kwargs
    ) -> 'GeopysparkCubeMetadata':
        # noinspection PyTypeChecker
        return super()._clone_and_update(
            metadata=metadata, dimensions=dimensions,
            spatial_extent=spatial_extent or self._spatial_extent,
            temporal_extent=temporal_extent or self._temporal_extent,
            **kwargs
        )

    def filter_bbox(self, west, south, east, north, crs) -> 'GeopysparkCubeMetadata':
        """Create new metadata instance with spatial extent"""
        # TODO take intersection with existing extent
        return self._clone_and_update(
            spatial_extent={"west": west, "south": south, "east": east, "north": north, "crs": crs}
        )

    @property
    def spatial_extent(self) -> dict:
        return self._spatial_extent

    def filter_temporal(self, start, end) -> 'GeopysparkCubeMetadata':
        """Create new metadata instance with temporal extent"""
        if self._temporal_extent is None:  # TODO: only for backwards compatibility
            return self._clone_and_update(temporal_extent=(start, end))

        this_start, this_end = self._temporal_extent

        # TODO: support time zones other than UTC
        if this_start > end or this_end < start:  # compared lexicographically
            # no overlap
            raise ValueError(start, end)

        return self._clone_and_update(temporal_extent=(max(this_start, start), min(this_end, end)))

    @property
    def temporal_extent(self) -> tuple:
        return self._temporal_extent

    def with_temporal_extent(self, temporal_extent: tuple):
        assert self.has_temporal_dimension()

        return self._clone_and_update(
            dimensions=[
                TemporalDimension(d.name, temporal_extent) if isinstance(d, TemporalDimension) else d
                for d in self._dimensions
            ],
            temporal_extent=temporal_extent,
        )

    @property
    def opensearch_link_titles(self) -> List[str]:
        """Get opensearch_link_titles from band dimension"""
        names_with_aliases = zip(self.band_dimension.band_names, self.band_dimension.band_aliases)
        return [n[1][0] if n[1] else n[0] for n in names_with_aliases]

    def provider_backend(self) -> Union[str, None]:
        return self.get("_vito", "data_source", "provider:backend", default=None)

    def auto_polarization(self) -> Union[str, None]:
        return self.get("_vito", "data_source", "auto_polarization", default=False)

    def parallel_query(self) -> Union[str, None]:
        return self.get("_vito", "data_source", "parallel_query", default=False)

    def common_name_priority(self) -> int:
        priority = self.get("_vito", "data_source", "common_name_priority", default=None)
        if priority is not None:
            return priority
        # fallback based on provider:backend property (if any)
        return {
            None: 0,
            "terrascope": 10,
            "sentinelhub": 5,
        }.get(self.provider_backend(), 0)

    def get_nodata_value(self, requested_bands, default_value) -> float:
        bands_metadata = self.get("summaries", "eo:bands",
                                  default=self.get("summaries", "raster:bands", default=[]))
        no_data_value = "undefined"
        for band_metadata in bands_metadata:
            if requested_bands is not None and band_metadata["name"] not in requested_bands:
                continue
            if "nodata" not in band_metadata:
                continue
            nodata = band_metadata["nodata"]
            if no_data_value == "undefined":
                no_data_value = nodata
            if no_data_value != nodata:
                # TODO: Support different nodata values per band in a layer.
                raise Exception(f"Requested bands have different nodata values: {no_data_value} and {nodata}")
        if no_data_value == "undefined":
            no_data_value = default_value
        return float(no_data_value)

    def get_layer_crs(self):
        crs = self.get("cube:dimensions", "x", "reference_system", default="EPSG:4326")
        if isinstance(crs, int):
            crs = "EPSG:%s" % str(crs)
        elif isinstance(crs, dict):
            if crs.get("name") == "AUTO 42001 (Universal Transverse Mercator)":
                crs = "Auto42001"
        return crs

    def get_overall_spatial_extent(self):
        global_extent_latlon = {"west": -180.0, "south": -90, "east": 180, "north": 90}
        bboxes = self.get("extent", "spatial", "bbox")
        if not bboxes:
            return global_extent_latlon
        # https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md#spatial-extent-object
        # "The first bounding box always describes the overall spatial extent of the data"
        bbox = bboxes[0]
        overall_extent = dict(zip(["west", "south", "east", "north"], bbox))

        likely_latlon = abs(overall_extent["west"] + 180) < 0.01 and abs(overall_extent["east"] - 180) < 0.01
        if self.get_layer_crs() != "EPSG:4326" and not likely_latlon:
            # We can only trust bbox values in LatLon
            return global_extent_latlon

        return overall_extent

    def get_GSD_in_meters(self) -> Union[tuple, dict, None]:
        bands_metadata = self.get("summaries", "eo:bands",
                                  default=self.get("summaries", "raster:bands", default=[]))
        band_to_gsd = {}
        for band_metadata in bands_metadata:
            band_name = band_metadata.get("name")
            band_gsd = band_metadata.get("gsd") or band_metadata.get("resolution")
            if not band_gsd and "openeo:gsd" in band_metadata:
                unit = band_metadata["openeo:gsd"]["unit"]
                if unit and unit != "m":
                    # Often degrees. Probably LatLon, but no need to figure that out now
                    continue
                band_gsd = band_metadata["openeo:gsd"]["value"]
            band_gsd = clean_number_pair(band_gsd)
            if band_gsd:
                band_to_gsd[band_name] = band_gsd

        if len(band_to_gsd) > 0:
            return band_to_gsd

        gsd_layer_wide = clean_number_pair(self.get("item_assets", "classification", "gsd", default=None))
        if gsd_layer_wide:
            return gsd_layer_wide

        crs = self.get_layer_crs()
        if crs == "EPSG:4326":
            # step could be expressed in LatLon or layer native crs.
            # Only when the layer native CRS is LatLon, we can trust it
            # https://github.com/stac-extensions/datacube#dimension-object

            bboxes = self.get("extent", "spatial", "bbox")
            if bboxes and len(bboxes) > 0:
                bbox = bboxes[0]
                # All spatial extends seem to be in LatLon:
                spatial_extent = {'west': bbox[0], 'east': bbox[2], 'south': bbox[1], 'north': bbox[3],
                                  'crs': "EPSG:4326"}

                dimensions_step = clean_number_pair((
                    self.get("cube:dimensions", "x", "step", default=None),
                    self.get("cube:dimensions", "y", "step", default=None)
                ))

                if dimensions_step:
                    resolution_meters = reproject_cellsize(spatial_extent, dimensions_step, crs, "Auto42001")
                    return resolution_meters
        return None
