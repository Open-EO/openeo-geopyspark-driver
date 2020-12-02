from abc import ABC, abstractmethod
from typing import List
import requests


class OpenSearch(ABC):
    @abstractmethod
    def get_collections(self) -> List[dict]:
        pass


class OpenSearchClient(OpenSearch):
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    def get_collections(self) -> List[dict]:
        resp = requests.get(url=self.endpoint + "/collections")
        return resp.json()["features"]

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.endpoint)

    def __str__(self):
        return self.endpoint


if __name__ == '__main__':
    opensearch = OpenSearchClient("http://oscars-dev.vgt.vito.be")
    collections = opensearch.get_collections()

    ids = (feature["id"] for feature in collections)

    for feature in collections:
        print(feature["id"])
