import logging

from openeo_driver.backend import LoadParameters
from openeo_driver.utils import EvalEnv
from py4j.java_gateway import JVMView

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.constants import EvalEnvKeys
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.utils import get_jvm


logger = logging.getLogger(__name__)


def create(load_params: LoadParameters, env: EvalEnv, jvm: JVMView):
    feature_flags = load_params.get("featureflags", {})
    tilesize = feature_flags.get("tilesize", 256)
    default_temporal_resolution = "ByDay"
    default_indexReduction = 6
    # if len(load_params.process_types) == 1 and ProcessType.GLOBAL_TIME in load_params.process_types:
    # for pure timeseries processing, adjust partitioning strategy
    # default_temporal_resolution = "None"
    # default_indexReduction = 0
    indexReduction = feature_flags.get("indexreduction", default_indexReduction)
    temporalResolution = feature_flags.get("temporalresolution", default_temporal_resolution)
    datacubeParams = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
    # WTF simple assignment to a var in a scala class doesn't work??
    getattr(datacubeParams, "tileSize_$eq")(tilesize)
    getattr(datacubeParams, "maskingStrategyParameters_$eq")(load_params.custom_mask)
    logger.debug(f"Using load_params.data_mask {load_params.data_mask!r}")
    if isinstance(load_params.data_mask, GeopysparkDataCube):
        datacubeParams.setMaskingCube(load_params.data_mask.get_max_level().srdd.rdd())
    datacubeParams.setPartitionerIndexReduction(indexReduction)
    datacubeParams.setPartitionerTemporalResolution(temporalResolution)

    datacubeParams.setAllowEmptyCube(
        feature_flags.get("allow_empty_cube", env.get(EvalEnvKeys.ALLOW_EMPTY_CUBES, False))
    )

    globalbounds = feature_flags.get("global_bounds", True)
    if globalbounds and load_params.global_extent is not None and len(load_params.global_extent) > 0:
        ge = load_params.global_extent
        datacubeParams.setGlobalExtent(
            float(ge["west"]), float(ge["south"]), float(ge["east"]), float(ge["north"]), ge["crs"]
        )
    single_level = env.get(EvalEnvKeys.PYRAMID_LEVELS, "all") != "all"
    if single_level:
        getattr(datacubeParams, "layoutScheme_$eq")("FloatingLayoutScheme")

    if load_params.pixel_buffer is not None:
        datacubeParams.setPixelBuffer(load_params.pixel_buffer[0], load_params.pixel_buffer[1])

    load_per_product = feature_flags.get("load_per_product", None)
    if load_per_product is not None:
        datacubeParams.setLoadPerProduct(load_per_product)
    elif get_backend_config().default_reading_strategy == "load_per_product":
        datacubeParams.setLoadPerProduct(True)

    if get_backend_config().default_tile_size is not None:
        if "tilesize" not in feature_flags:
            getattr(datacubeParams, "tileSize_$eq")(get_backend_config().default_tile_size)

    datacubeParams.setResampleMethod(GeopysparkDataCube._get_resample_method(load_params.resample_method))

    if load_params.filter_temporal_labels is not None:
        from openeogeotrellis.backend import GeoPySparkBackendImplementation

        labels_filter = GeoPySparkBackendImplementation.accept_process_graph(
            load_params.filter_temporal_labels["process_graph"]
        )
        datacubeParams.setTimeDimensionFilter(labels_filter.builder)
    return datacubeParams, single_level
