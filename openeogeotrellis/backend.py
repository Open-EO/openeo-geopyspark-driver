from typing import List

from openeo_driver import backend
from openeo_driver.utils import read_json
from openeogeotrellis import ConfigParams
from openeogeotrellis.layercatalog import GeoPySparkLayerCatalog
from openeogeotrellis.service_registry import InMemoryServiceRegistry, ZooKeeperServiceRegistry


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
        service_registry = (
            InMemoryServiceRegistry() if ConfigParams().is_ci_context
            else ZooKeeperServiceRegistry()
        )
        catalog = read_json("layercatalog.json")
        super().__init__(
            secondary_services=GpsSecondaryServices(service_registry=service_registry),
            catalog=GeoPySparkLayerCatalog(all_metadata=catalog, service_registry=service_registry),
        )

    def health_check(self) -> str:
        from pyspark import SparkContext
        sc = SparkContext.getOrCreate()
        count = sc.parallelize([1, 2, 3]).count()
        return 'Health check: ' + str(count)


def get_openeo_backend_implementation() -> backend.OpenEoBackendImplementation:
    return GeoPySparkBackendImplementation()
