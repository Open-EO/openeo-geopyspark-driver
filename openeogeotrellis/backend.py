from typing import List

from openeo_driver import backend
from openeogeotrellis import ConfigParams
from openeogeotrellis.layercatalog import get_layer_catalog
from openeogeotrellis.service_registry import InMemoryServiceRegistry, ZooKeeperServiceRegistry
from openeogeotrellis.utils import kerberos, normalize_date
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection

from py4j.java_gateway import JavaGateway
import geopyspark as gps
from geopyspark import TiledRasterLayer, LayerType


def _merge(original: dict, key, value) -> dict:
    # TODO: move this to a more general module (e.g. openeo client)?
    copy = dict(original)
    copy[key] = value
    return copy


class GpsSecondaryServices(backend.SecondaryServices):
    """Secondary Services implementation for GeoPySpark backend"""

    def __init__(self, service_registry: InMemoryServiceRegistry):
        self.service_registry = service_registry

    def service_types(self) -> dict:
        return {
            "WMTS": {
                "parameters": {
                    "version": {
                        "type": "string",
                        "description": "The WMTS version to use.",
                        "default": "1.0.0",
                        "enum": [
                            "1.0.0"
                        ]
                    }
                },
                "attributes": {
                    "layers": {
                        "type": "array",
                        "description": "Array of layer names.",
                        "example": [
                            "roads",
                            "countries",
                            "water_bodies"
                        ]
                    }
                }
            }
        }

    def list_services(self) -> List[dict]:
        return [
            _merge(details, 'service_id', service_id)
            for service_id, details in self.service_registry.get_all_specifications().items()
        ]

    def service_info(self, service_id: str) -> dict:
        # TODO: add fields: id, url, enabled, parameters, attributes
        details = self.service_registry.get_specification(service_id)
        return _merge(details, 'service_id', service_id)

    def remove_service(self, service_id: str) -> None:
        self.service_registry.stop_service(service_id)


class GeoPySparkBackendImplementation(backend.OpenEoBackendImplementation):

    def __init__(self):
        # TODO: do this with a config instead of hardcoding rules?
        self._service_registry = (
            InMemoryServiceRegistry() if ConfigParams().is_ci_context
            else ZooKeeperServiceRegistry()
        )

        super().__init__(
            secondary_services=GpsSecondaryServices(service_registry=self._service_registry),
            catalog=get_layer_catalog(service_registry=self._service_registry),
        )

    def health_check(self) -> str:
        from pyspark import SparkContext
        sc = SparkContext.getOrCreate()
        count = sc.parallelize([1, 2, 3]).count()
        return 'Health check: ' + str(count)

    def load_disk_data(self, format: str, glob_pattern: str, options: dict, viewing_parameters: dict) -> object:
        if format != 'GTiff':
            raise NotImplemented("The format is not supported by the backend: " + format)

        date_regex = options['date_regex']

        if glob_pattern.startswith("hdfs:"):
            kerberos()

        from_date = normalize_date(viewing_parameters.get("from", None))
        to_date = normalize_date(viewing_parameters.get("to", None))

        left = viewing_parameters.get("left", None)
        right = viewing_parameters.get("right", None)
        top = viewing_parameters.get("top", None)
        bottom = viewing_parameters.get("bottom", None)
        srs = viewing_parameters.get("srs", None)
        band_indices = viewing_parameters.get("bands")

        sc = gps.get_spark_context()

        gateway = JavaGateway(eager_load=True, gateway_parameters=sc._gateway.gateway_parameters)
        jvm = gateway.jvm

        if left is not None and right is not None and top is not None and bottom is not None:
            extent = jvm.geotrellis.vector.Extent(float(left), float(bottom), float(right), float(top))

        pyramid = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex) \
            .pyramid_seq(extent, srs, from_date, to_date)

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option
        levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
            option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                  range(0, pyramid.size())}

        image_collection = GeotrellisTimeSeriesImageCollection(
            pyramid=gps.Pyramid(levels),
            service_registry=self._service_registry,
            metadata={}
        )

        return image_collection.band_filter(band_indices) if band_indices else image_collection


def get_openeo_backend_implementation() -> backend.OpenEoBackendImplementation:
    return GeoPySparkBackendImplementation()
