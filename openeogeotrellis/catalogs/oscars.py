import logging
import re
from typing import List

import requests
from requests.adapters import HTTPAdapter, Retry

_log = logging.getLogger(__name__)


class OscarsCatalogEntry:
    def __init__(self, product_id):
        self._product_id = product_id

    def getProductId(self):
        return self._product_id

    def getTileId(self):
        m = re.search('\d{6}_(\d{2}\w{3})_TOC', self._product_id)
        return m.group(1)


class OscarsCatalogClient:

    def __init__(self, collection):
        self._collection = collection

    @staticmethod
    def _parse_product_ids(response) -> List[OscarsCatalogEntry]:
        return [OscarsCatalogEntry(f['properties']['title']) for f in response['features']]

    def _query_page(self, start_date, end_date,
              ulx=-180, uly=90, brx=180, bry=-90, from_index=1,cldPrcnt=100.):

        oscars_url = "https://services.terrascope.be/catalogue/products"
        query_params = [('collection', self._collection),
                        ('bbox', '{},{},{},{}'.format(ulx, bry, brx, uly)),
                        ('sortKeys', 'title'),
                        ('startIndex', from_index),
                        ('start', start_date.isoformat()),
                        ('end', end_date.isoformat()),
                        ('cloudCover', f'[0,{cldPrcnt}]')
                        ]
        try:
            session = requests.Session()
            session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.1)))
            response = session.get(oscars_url, params=query_params)
            response.raise_for_status()
        except requests.RequestException as e:
            _log.error(f"OSCARS query failed: {oscars_url!r} with {query_params}: {e}")
            raise

        return response.json()

    def query(self, start_date, end_date,
              ulx=-180, uly=90, brx=180, bry=-90,cldPrcnt=100.) -> List[OscarsCatalogEntry]:

        result = []

        products_left = True
        from_index = 1

        while products_left:
            response = self._query_page(start_date, end_date, ulx, uly, brx, bry, from_index,cldPrcnt=cldPrcnt)
            chunk = self._parse_product_ids(response)
            if len(chunk) == 0:
                products_left = False
            else:
                result += chunk
                from_index += len(chunk)

        return result