import dataclasses
import logging
from typing import TYPE_CHECKING, Dict, List, Optional

import pyproj
from openeo_driver import filter_properties
from openeo_driver.backend import LoadParameters
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv
from shapely.geometry import box

from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.util.datetime import normalize_temporal_extent

from .collection_metadata import CollectionCubeMetadata

if TYPE_CHECKING:
    # imported lazily to avoid a load_request.py <-> layer_catalog.py import cycle at runtime
    from .layer_catalog import LayerCatalog

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class PropertyFilters:
    """
    Replaces the old `metadata_properties(flatten_eqs)` closure with a value object.

    `conditions()`/`flattened()` recompute from `_custom_properties` on every call
    instead of caching, because that dict is `load_params.properties` itself (not a
    copy): the PLANETSCOPE branch in the sentinel-hub pyramid builder does
    `del load_params.properties['byoc_id']` between two calls and relies on the
    second call no longer seeing it (doc 03 §7.2).
    """

    _layer_properties: Dict[str, dict]
    _custom_properties: Dict[str, dict]
    _env: EvalEnv

    @classmethod
    def resolve(cls, *, layer_properties: dict, custom_properties: dict, env: EvalEnv) -> "PropertyFilters":
        return cls(_layer_properties=layer_properties, _custom_properties=custom_properties, _env=env)

    def conditions(self) -> Dict[str, dict]:
        return {
            property_name: filter_properties.extract_literal_match(condition, self._env)
            for property_name, condition in {**self._layer_properties, **self._custom_properties}.items()
        }

    def flattened(self) -> Dict[str, object]:
        def eq_value(criterion: Dict[str, object]) -> object:
            if len(criterion) != 1:
                raise ValueError(f'expected a single "eq" criterion, was {criterion}')
            # TODO https://github.com/Open-EO/openeo-geotrellis-extensions/issues/39
            return list(criterion.values())[0]

        return {property_name: eq_value(criterion) for property_name, criterion in self.conditions().items()}


@dataclasses.dataclass(frozen=True)
class CollectionLoadRequest:
    """
    Every decision `_load_collection_cached` makes before it touches the JVM,
    resolved once by `resolve_load_request`.
    """

    collection_id: str  # after merged_by_common_name resolution
    metadata: CollectionCubeMetadata  # band-filtered, renamed, temporal+bbox filtered
    source_info: dict  # the "_vito"."data_source" block
    source_type: str  # lowercased
    west: float
    south: float
    east: float
    north: float
    srs: str
    from_date: str
    to_date: str
    cell_width: float
    cell_height: float
    native_crs: str
    target_epsg: int
    bands: Optional[List[str]]
    band_indices: Optional[List[int]]
    normalized_band_selection: Optional[List[str]]
    feature_flags: dict
    correlation_id: str
    max_soft_errors_ratio: float
    opensearch_endpoint: str
    postprocessing_band_graph: Optional[dict]
    sar_backscatter_compatible: bool
    property_filters: PropertyFilters

    @property
    def is_utm(self) -> bool:
        return self.source_info.get("is_utm", False)

    @property
    def catalog_type(self) -> str:
        return self.source_info.get("catalog_type", "")

    @property
    def experimental(self) -> bool:
        return self.feature_flags.get("experimental", False)


def resolve_load_request(
    *,
    collection_id: str,
    load_params: LoadParameters,
    env: EvalEnv,
    catalog: "LayerCatalog",
    default_opensearch_endpoint: str,
) -> CollectionLoadRequest:
    from_date, to_date = temporal_extent = normalize_temporal_extent(load_params.temporal_extent)
    spatial_extent = load_params.spatial_extent

    west = spatial_extent.get("west", None)
    east = spatial_extent.get("east", None)
    north = spatial_extent.get("north", None)
    south = spatial_extent.get("south", None)
    srs = spatial_extent.get("crs", "EPSG:4326")
    if isinstance(srs, int):
        srs = "EPSG:%s" % str(srs)

    spatial_bounds_present = all(b is not None for b in [west, south, east, north])
    if not spatial_bounds_present:
        if env.get(EVAL_ENV_KEY.REQUIRE_BOUNDS, False):
            raise OpenEOApiException(
                code="MissingSpatialFilter",
                status_code=400,
                message="No spatial filter could be derived to load this collection: {c} . Please specify a bounding box, or polygons to define your area of interest.".format(
                    c=collection_id
                ),
            )
        else:
            # whole world processing, for instance in viewing services
            srs = "EPSG:4326"
            west = -180.0
            south = -90
            east = 180
            north = 90

    metadata = CollectionCubeMetadata(catalog.get_collection_metadata(collection_id))
    layer_source_info = metadata.get("_vito", "data_source", default={})

    if layer_source_info.get("type") == "merged_by_common_name":
        logger.info(f"Resolving 'merged_by_common_name' collection {metadata.get('id')}")
        metadata = catalog.resolve_merged_by_common_name(
            collection_id=collection_id,
            metadata=metadata,
            load_params=load_params,
            temporal_extent=temporal_extent,
            spatial_extent=spatial_extent,
        )
        collection_id = metadata.get("id")
        layer_source_info = metadata.get("_vito", "data_source", default={})
        logger.info(f"Resolved 'merged_by_common_name' to collection {metadata.get('id')}")

    sar_backscatter_compatible = layer_source_info.get("sar_backscatter_compatible", False)
    if load_params.sar_backscatter is not None and not sar_backscatter_compatible:
        raise OpenEOApiException(
            message="""Process "sar_backscatter" is not applicable for collection {c}.""".format(c=collection_id),
            status_code=400,
        )

    layer_source_type = layer_source_info.get("type", "Accumulo").lower()

    postprocessing_band_graph = metadata.get("_vito", "postprocessing_bands", default=None)
    logger.debug("Cube source type: {s!r}".format(s=layer_source_type))
    cell_width = float(metadata.get("cube:dimensions", "x", "step", default=10.0))
    cell_height = float(metadata.get("cube:dimensions", "y", "step", default=10.0))

    bands = load_params.bands
    if bands:
        band_indices = [metadata.get_band_index(b) for b in bands]
        metadata = metadata.filter_bands(bands)
        # Note: the `metadata.filter_bands()` includes resolving band naming ("common_name" and aliases)
        #       which we want to preserve in `normalized_band_selection`,
        #       before `metadata.rename_labels()` changes it back to the originally requested band names.
        normalized_band_selection = metadata.band_names
        metadata = metadata.rename_labels(metadata.band_dimension.name, target=bands, source=metadata.band_names)
    else:
        band_indices = None
        # Use ordered bands selection from metadata
        normalized_band_selection = metadata.band_names if metadata.has_band_dimension() else None

    logger.debug("band_indices: {b!r}".format(b=band_indices))

    # band specific gsd can override collection default
    band_gsds = [band.gsd["value"] for band in metadata.bands if band.gsd is not None]
    if len(band_gsds) > 0:

        def smallest_cell_size(band_gsd, coordinate_index):
            return (
                min(size[coordinate_index] for size in band_gsd)
                if isinstance(band_gsd[0], list)
                else band_gsd[coordinate_index]
            )

        cell_width = float(min(smallest_cell_size(band_gsd, coordinate_index=0) for band_gsd in band_gsds))
        cell_height = float(min(smallest_cell_size(band_gsd, coordinate_index=1) for band_gsd in band_gsds))

    native_crs = catalog.native_crs(metadata)

    metadata = metadata.filter_temporal(from_date, to_date)

    correlation_id = env.get(EVAL_ENV_KEY.CORRELATION_ID, "")
    logger.info("Correlation ID is '{cid}'".format(cid=correlation_id))
    logger.info("Detected process types:" + str(load_params.process_types))

    feature_flags = load_params.get("featureflags", {})

    metadata = metadata.filter_bbox(west=west, south=south, east=east, north=north, crs=srs)

    if native_crs == "UTM":
        target_epsg = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
    else:
        target_epsg = int(native_crs.split(":")[-1])

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
                target_epsg = auto_utm_epsg_for_geometry(box(west, south, east, north), srs)
            else:
                target_epsg = pyproj.CRS.from_user_input(load_params.target_crs).to_epsg()

    opensearch_endpoint = layer_source_info.get("opensearch_endpoint", default_opensearch_endpoint)
    max_soft_errors_ratio = env.get(EVAL_ENV_KEY.MAX_SOFT_ERRORS_RATIO, 0.0)

    property_filters = PropertyFilters.resolve(
        layer_properties=metadata.get("_vito", "properties", default={}),
        custom_properties=load_params.properties,
        env=env,
    )

    return CollectionLoadRequest(
        collection_id=collection_id,
        metadata=metadata,
        source_info=layer_source_info,
        source_type=layer_source_type,
        west=west,
        south=south,
        east=east,
        north=north,
        srs=srs,
        from_date=from_date,
        to_date=to_date,
        cell_width=cell_width,
        cell_height=cell_height,
        native_crs=native_crs,
        target_epsg=target_epsg,
        bands=bands,
        band_indices=band_indices,
        normalized_band_selection=normalized_band_selection,
        feature_flags=feature_flags,
        correlation_id=correlation_id,
        max_soft_errors_ratio=max_soft_errors_ratio,
        opensearch_endpoint=opensearch_endpoint,
        postprocessing_band_graph=postprocessing_band_graph,
        sar_backscatter_compatible=sar_backscatter_compatible,
        property_filters=property_filters,
    )

