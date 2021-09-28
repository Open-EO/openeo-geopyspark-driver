import re
import requests


class CatalogEntry:
    # TODO: put "oscars" in class name
    def __init__(self, product_id):
        self._product_id = product_id

    def getProductId(self):
        return self._product_id

    def getTileId(self):
        m = re.search('\d{6}_(\d{2}\w{3})_TOC', self._product_id)
        return m.group(1)


class CatalogClient:
    # TODO: put "oscars" in class name

    def __init__(self, collection):
        self._collection = collection

    @staticmethod
    def _parse_product_ids(response):
        return [CatalogEntry(f['properties']['title']) for f in response['features']]

    def _query_page(self, start_date, end_date,
              ulx=-180, uly=90, brx=180, bry=-90, from_index=1):

        query_params = [('collection', self._collection),
                        ('bbox', '{},{},{},{}'.format(ulx, bry, brx, uly)),
                        ('sortKeys', 'title'),
                        ('startIndex', from_index),
                        ('start', start_date.isoformat()),
                        ('end', end_date.isoformat())]

        response = requests.get('https://services.terrascope.be/catalogue/products', params=query_params)

        try:
            response = response.json()
        except ValueError:
            response.raise_for_status()

        return response

    def query(self, start_date, end_date,
              ulx=-180, uly=90, brx=180, bry=-90):

        result = []

        products_left = True
        from_index = 1

        while products_left:
            response = self._query_page(start_date, end_date, ulx, uly, brx, bry, from_index)
            chunk = self._parse_product_ids(response)
            if len(chunk) == 0:
                products_left = False
            else:
                result += chunk
                from_index += len(chunk)

        return result