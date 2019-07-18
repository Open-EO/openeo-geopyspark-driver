from typing import List

from openeo_driver import backend
from openeogeotrellis import _service_registry
from openeogeotrellis.service_registry import InMemoryServiceRegistry


def _merge(original: dict, key, value) -> dict:
    # TODO: move this to a more general module (e.g. openeo client)?
    copy = dict(original)
    copy[key] = value
    return copy


class GpsSecondaryServices(backend.SecondaryServices):
    """Secondary Services implementation for GeoPySpark backend"""

    def __init__(self, service_registry: InMemoryServiceRegistry = _service_registry):
        self._service_registry = service_registry

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
            for service_id, details in self._service_registry.get_all_specifications().items()
        ]

    def service_info(self, service_id: str) -> dict:
        # TODO: add fields: id, url, enabled, parameters, attributes
        details = self._service_registry.get_specification(service_id)
        return _merge(details, 'service_id', service_id)

    def remove_service(self, service_id: str) -> None:
        self._service_registry.stop_service(service_id)


def get_openeo_backend_implementation() -> backend.OpenEoBackendImplementation:
    return backend.OpenEoBackendImplementation(
        secondary_services=GpsSecondaryServices()
    )
