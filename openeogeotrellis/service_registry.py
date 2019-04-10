from typing import Dict


class InMemoryServiceRegistry:
    """Keeps the mapping only in memory; Gatekeeper/Nginx will not be able to expose the service to the outside
    world."""

    def __init__(self):
        self._mapping = {}

    def register(self, service_id: str, specification: Dict, port: int):
        details = {
            'port': port,
            'specification': specification
        }

        self._mapping[service_id] = details

    def get(self, service_id) -> Dict:
        return self._mapping[service_id]

    def get_all(self) -> Dict[str, Dict]:
        return self._mapping


class ZooKeeperServiceRegistry:
    """The idea is that 1) Gatekeeper/Nginx will use this to map an url to a port and 2) this application will use it
    to map ID's to service details (exposed in the API)."""

    pass
