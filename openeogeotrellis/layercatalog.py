import logging
from functools import lru_cache
from typing import List, Dict, Optional

import geopyspark

from openeo.util import TimingLogger
from openeo_driver.backend import LoadParameters
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException, ProcessGraphComplexityException
from openeo_driver.utils import EvalEnv, WhiteListEvalEnv, smart_bool

from openeogeotrellis import datacube_parameters
from openeogeotrellis.catalog.files import dump_layer_catalog, load_catalog_files
from openeogeotrellis.catalog.layer_catalog import LayerCatalog
from openeogeotrellis.catalog.load_request import resolve_load_request
from openeogeotrellis.catalog.validation import extra_validation_load_collection
from openeogeotrellis._backend import post_dry_run
from openeogeotrellis.collections.pyramid_sources import SOURCE_BUILDERS, JvmLoadContext
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.constants import EVAL_ENV_KEY, WHITELIST
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.processgraphvisiting import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.utils import (
    to_projected_polygons,
    get_jvm,
)
from openeogeotrellis.vault import Vault

logger = logging.getLogger(__name__)


class GeoPySparkLayerCatalog(LayerCatalog):
    def __init__(self, all_metadata: List[dict], vault: Vault = None):
        super().__init__(all_metadata=all_metadata)
        self._geotiff_pyramid_factories = {}
        self._default_sentinel_hub_client_id = None
        self._default_sentinel_hub_client_secret = None
        self._vault = vault

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self._default_sentinel_hub_client_id = client_id
        self._default_sentinel_hub_client_secret = client_secret

    @TimingLogger(title="load_collection", logger=logger)
    def load_collection(
        self, collection_id: str, load_params: LoadParameters, env: EvalEnv, pg_node_id: Optional[str] = None
    ) -> GeopysparkDataCube:

        if smart_bool(env.get(EVAL_ENV_KEY.DO_EXTENT_CHECK, True)):
            env_validate = env.push({
                "allow_check_missing_products": False,
            })
            try:
                issues = extra_validation_load_collection(
                    collection_id, load_params, env_validate, global_extent_provider=post_dry_run.get_global_extent
                )
            except Exception as e:
                issues = [{"code": "Internal", "message": str(e)}]
                logger.warning(f"Error during extra_validation_load_collection: {e!r}")
            # Only care for certain errors and make list of strings:
            issues = [e["message"] for e in issues if e["code"] == "ExtentTooLarge"]
            if issues:
                if env.get("sync_job", False):
                    raise ProcessGraphComplexityException(
                        ProcessGraphComplexityException.message + f" Reasons: {' '.join(issues)}"
                    )
                else:
                    raise ProcessGraphComplexityException(
                        "The process graph is computationally too heavy and will likely time out. Disable this check with 'job_options.do_extent_check': "
                        + " ".join(issues)
                    )

        return self._load_collection_cached(
            collection_id, load_params, WhiteListEvalEnv(env, WHITELIST), pg_node_id=pg_node_id
        )

    @lru_cache(maxsize=40)
    def _load_collection_cached(
        self, collection_id: str, load_params: LoadParameters, env: EvalEnv, pg_node_id: Optional[str] = None
    ) -> GeopysparkDataCube:
        logger.info(
            f"load_collection: Creating raster datacube for {collection_id=} ({pg_node_id=}) with {load_params=}, {env=}"
        )

        request = resolve_load_request(
            collection_id=collection_id,
            load_params=load_params,
            env=env,
            catalog=self,
            default_opensearch_endpoint=get_backend_config().default_opensearch_endpoint,
        )
        collection_id = request.collection_id
        metadata = request.metadata

        pysc = geopyspark.get_spark_context()
        description = f"load_collection_{collection_id}"
        if request.bands:
            description += f"_{'-'.join(request.bands)}"
        pysc.setJobDescription(description)

        jvm = get_jvm()

        extent = jvm.geotrellis.vector.Extent(
            float(request.west), float(request.south), float(request.east), float(request.north)
        )

        geometries = load_params.aggregate_spatial_geometries
        empty_geometries = isinstance(geometries, DriverVectorCube) and len(geometries.get_geometries()) == 0
        geometries = None if empty_geometries else geometries  # TODO: ensure that driver vector cube can not have empty geometries.
        if not geometries:
            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, request.srs)
        else:
            projected_polygons = to_projected_polygons(
                jvm, geometries, crs=request.srs, buffer_points=True
            )

        projected_polygons_native_crs = (getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$")
                                         .reproject(projected_polygons, request.target_epsg))
        logger.debug(projected_polygons_native_crs)
        logger.debug(projected_polygons_native_crs.geometries())
        logger.debug(projected_polygons_native_crs.extent())
        logger.debug(projected_polygons_native_crs.polygons()[0].toString())

        datacubeParams, single_level = datacube_parameters.create(load_params, env, jvm)
        feature_flags = request.feature_flags
        if feature_flags.get("no_resample_on_read", False):
            logger.info("Setting NoResampleOnRead to true")
            datacubeParams.setNoResampleOnRead(True)

        val = smart_bool(feature_flags.get("use_new_feature_extent_intersection", False))
        datacubeParams.setUseNewFeatureExtentIntersection(val)

        if "use_new_feature_extent_intersection_2" in feature_flags:
            val = smart_bool(feature_flags.get("use_new_feature_extent_intersection_2"))
            logger.info(f"Setting useNewFeatureExtentIntersection2 to {val}")
            datacubeParams.setUseNewFeatureExtentIntersection2(val)

        def metadata_properties(flatten_eqs=True) -> Dict[str, object]:
            return request.property_filters.flattened() if flatten_eqs else request.property_filters.conditions()

        ctx = JvmLoadContext(
            jvm=jvm,
            extent=extent,
            geometries=geometries,
            projected_polygons=projected_polygons,
            projected_polygons_native_crs=projected_polygons_native_crs,
            datacube_params=datacubeParams,
            single_level=single_level,
            load_params=load_params,
            env=env,
            pg_node_id=pg_node_id,
            metadata_properties=metadata_properties,
            get_sar_backscatter_arguments=lambda: _get_sar_backscatter_arguments(load_params=load_params, env=env),
            sentinel_hub_client_id=self._default_sentinel_hub_client_id,
            sentinel_hub_client_secret=self._default_sentinel_hub_client_secret,
            vault=self._vault,
            geotiff_pyramid_factories=self._geotiff_pyramid_factories,
        )

        builder = SOURCE_BUILDERS.get(request.source_type)
        if builder is None:
            raise OpenEOApiException(message="Invalid layer source type {t!r}".format(t=request.source_type))
        result = builder(request, ctx)

        pyramid = result.pyramid
        metadata = result.metadata or metadata
        still_needs_band_filter = result.still_needs_band_filter

        if isinstance(pyramid, dict):
            levels = pyramid
        else:
            temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
            option = jvm.scala.Option

            levels = {
                pyramid.apply(index)._1(): geopyspark.TiledRasterLayer(
                    geopyspark.LayerType.SPACETIME,
                    temporal_tiled_raster_layer(option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())
                )
                for index in range(0, pyramid.size())
            }

        if single_level:
            max_zoom = max(levels.keys())
            levels = {max_zoom: levels[max_zoom]}

        image_collection = GeopysparkDataCube(
            pyramid=geopyspark.Pyramid(levels),
            metadata=metadata
        )

        if request.postprocessing_band_graph != None:
            visitor = GeotrellisTileProcessGraphVisitor()
            image_collection = image_collection.apply_dimension(
                process=visitor.accept_process_graph(request.postprocessing_band_graph),
                dimension=image_collection.metadata.band_dimension.name,
                context={},
                env=EvalEnv(),
            )

        if still_needs_band_filter:
            # TODO: avoid this `still_needs_band_filter` ugliness.
            #       Also see https://github.com/Open-EO/openeo-geopyspark-driver/issues/29
            image_collection = image_collection.filter_bands(request.band_indices)

        pysc.setJobDescription("")

        return image_collection


def get_layer_catalog(
    vault: Vault = None,
    # TODO: just call this arg `enrich_metadata` is this is about more than just OpenSearch
    opensearch_enrich: Optional[bool] = None,
) -> GeoPySparkLayerCatalog:
    backend_config = get_backend_config()
    enrich_metadata = opensearch_enrich if opensearch_enrich is not None else backend_config.opensearch_enrich
    metadata = load_catalog_files(
        catalog_files=backend_config.layer_catalog_files,
        enrich_metadata=enrich_metadata,
        default_opensearch_endpoint=backend_config.default_opensearch_endpoint,
    )
    return GeoPySparkLayerCatalog(
        all_metadata=list(metadata.values()),
        vault=vault,
    )


def _get_sar_backscatter_arguments(load_params: LoadParameters, env: EvalEnv) -> SarBackscatterArgs:
    """
    Get SarBackscatterArgs from LoadParameters if available,
    otherwise: look in process registry schema to pick defaults that
    are possibly overridden in deployment configuration.
    """
    if load_params.sar_backscatter:
        sar_backscatter_arguments = load_params.sar_backscatter
    else:
        try:
            # TODO: is it possible to avoid hardcoding `GpsProcessing` here?
            #       Note that `env.get("backend_implementation").processing` is not available here anymore
            #       because of that WhiteListEvalEnv caching business.
            #       Also note that this requires a local import to break an import cycle between
            #       openeogeotrellis.backend and openeogeotrellis.layercatalog
            import openeogeotrellis.backend
            processing = openeogeotrellis.backend.GpsProcessing()
            api_version = env.openeo_api_version()
            sar_backscatter_arguments = processing.get_default_sar_backscatter_arguments(api_version=api_version)
        except Exception as e:
            logger.warning(f"_get_sar_backscatter_arguments failed: {e!r}")
            sar_backscatter_arguments = SarBackscatterArgs()
    return sar_backscatter_arguments


if __name__ == "__main__":
    dump_layer_catalog()
