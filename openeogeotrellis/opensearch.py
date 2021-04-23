import logging
from datetime import date
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

import requests

from openeo.util import Rfc3339, dict_no_none, deep_get

logger = logging.getLogger(__name__)


class OpenSearch:
    def get_metadata(self, collection_id: str) -> dict:
        return {}


class OpenSearchOscars(OpenSearch):
    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        self._cache = None

    def _get_collection(self, collection_id: str) -> dict:
        if not self._cache:
            url = self.endpoint + "/collections"
            logger.info(f"Getting collection metadata from {url}")
            resp = requests.get(url=url)
            resp.raise_for_status()
            self._cache = {f["id"]: f for f in resp.json()["features"]}
        return self._cache[collection_id]

    def get_metadata(self, collection_id: str) -> dict:
        rfc3339 = Rfc3339(propagate_none=True)

        collection = self._get_collection(collection_id)

        def transform_link(opensearch_link: dict) -> dict:
            return dict_no_none(
                rel="alternate",
                href=opensearch_link["href"],
                title=opensearch_link.get("title")
            )

        def search_link(opensearch_link: dict) -> dict:
            def replace_endpoint(url: str) -> str:
                components = urlparse(url)

                return urlunparse(components._replace(
                    scheme="https",
                    netloc="services.terrascope.be",
                    path="/catalogue" + components.path
                ))

            return dict_no_none(
                rel="alternate",
                href=replace_endpoint(opensearch_link["href"]),
                title=opensearch_link.get("title")
            )

        def date_bounds() -> (date, Optional[date]):
            acquisition_information = collection["properties"]["acquisitionInformation"]
            earliest_start_date = None
            latest_end_date = None

            for info in acquisition_information:
                start_datetime = rfc3339.parse_datetime(rfc3339.normalize(info["acquisitionParameters"]["beginningDateTime"]))
                end_datetime = rfc3339.parse_datetime(rfc3339.normalize(info["acquisitionParameters"].get("endingDateTime")))

                if not earliest_start_date or start_datetime.date() < earliest_start_date:
                    earliest_start_date = start_datetime.date()

                if end_datetime and (not latest_end_date or end_datetime.date() > latest_end_date):
                    latest_end_date = end_datetime.date()

            return earliest_start_date, latest_end_date

        earliest_start_date, latest_end_date = date_bounds()

        bands = collection["properties"].get("bands")

        def instruments() -> List[str]:
            instruments_short_names = [info.get("instrument", {}).get("instrumentShortName") for info in
                                       collection["properties"]["acquisitionInformation"]]

            return list(set([name for name in instruments_short_names if name]))

        return {
            "title": collection["properties"]["title"],
            "description": collection["properties"]["abstract"],
            "extent": {
                "spatial": {"bbox": [collection["bbox"]]},
                "temporal": {"interval": [
                    [earliest_start_date.isoformat(), latest_end_date.isoformat() if latest_end_date else None]
                ]}
            },
            "links": [transform_link(l) for l in collection["properties"]["links"]["describedby"]] +
                     [search_link(l) for l in collection["properties"]["links"].get("search", [])],
            "cube:dimensions": {
                "bands": {
                    "type": "bands",
                    "values": [band["title"] for band in bands] if bands else None
                }
            },
            "summaries": {
                "eo:bands": [dict(band, name=band["title"]) for band in bands] if bands else None,
                "instruments": instruments()
            }
        }

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.endpoint)

    def __str__(self):
        return self.endpoint


class OpenSearchCreodias(OpenSearch):
    """
    See https://creodias.eu/eo-data-finder-api-manual
    """

    def __init__(self, endpoint: str = "https://finder.creodias.eu"):
        self.endpoint = endpoint
        self._collections_cache = None

    def _get_collection(self, collection_id: str) -> dict:
        if not self._collections_cache:
            url = self.endpoint + "/resto/collections.json"
            logger.info(f"Getting collection metadata from {url}")
            resp = requests.get(url=url)
            resp.raise_for_status()
            self._collections_cache = {c["name"]: c for c in resp.json()["collections"]}
        return self._collections_cache[collection_id]

    def get_metadata(self, collection_id: str) -> dict:
        collection = self._get_collection(collection_id)
        return {
            "title": deep_get(collection, "osDescription", "LongName", default=collection["name"]),
            "description": deep_get(collection, "osDescription", "Description", default=collection["name"]),
            "keywords": deep_get(collection, "osDescription", "Tags", default="").split(),
            # TODO more metadata
        }
