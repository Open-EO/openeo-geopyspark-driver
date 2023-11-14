from typing import List

import re
from datetime import datetime, time
from pathlib import Path

import requests
from shapely.geometry import Polygon

from openeogeotrellis.catalogs.base import CatalogClientBase, CatalogEntryBase, CatalogStatus
from openeogeotrellis.catalogs.creo_ordering import CreoOrder
from requests.adapters import HTTPAdapter, Retry

class CreoCatalogEntry(CatalogEntryBase):
    # product_id expected as one of:
    #   /eodata/Sentinel-2/MSI/L2A/2019/11/17/S2B_MSIL2A_20191117T105229_N0213_R051_T31UET_20191117T134337.SAFE
    #   S2B_MSIL2A_20191117T105229_N0213_R051_T31UET_20191117T134337
    def __init__(self, product_id, status, s3_bucket=None, s3_key=None):
        prarr = product_id.replace('.SAFE', '').split('/')
        self._product_id = prarr[-1]
        self._tile_id = re.split('_T([0-9][0-9][A-Z][A-Z][A-Z])_', product_id)[1].split('_')[0]
        self._date_str = re.search(r"_(\d{8})T\d{6}_", self._product_id).group(1)
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key
        if self._s3_bucket is None:
            if len(prarr) == 1:
                self._s3_bucket = 'EODATA'
            else:
                self._s3_bucket = prarr[1].upper()
        if self._s3_key is None:
            if len(prarr) == 1:
                self._s3_key = '/'.join([
                    'Sentinel-2',
                    self._product_id[4:7],  # MSI
                    self._product_id[7:10],  # L2A
                    self._product_id[11:15],  # 2019
                    self._product_id[15:17],  # 11
                    self._product_id[17:19],  # 17
                    self._product_id
                ]) + '.SAFE'
            else:
                self._s3_key = '/'.join(prarr[2:]) + '.SAFE'
        self._status = status
        self.relfilerelpathbuffer = None  # in order not to request the xml every time for a band, this is prepared the first time

    def __str__(self):
        return self._product_id

    def getProductId(self):
        return self._product_id

    def getDateStr(self) -> str:
        return self._date_str

    def getS3Bucket(self):
        return self._s3_bucket

    def getS3Key(self):
        return self._s3_key

    def getTileId(self):
        return self._tile_id

    def getStatus(self):
        return self._status

    def getFileRelPath(self, s3connection, band, resolution):
        if self.relfilerelpathbuffer is None:
            self.relfilerelpathbuffer = s3connection.get_band_filename(self, band, resolution).replace('_' + band + '_',
                                                                                                       '_@BAND@_')
        return self.relfilerelpathbuffer.replace('@BAND@', band)


class CreoCatalogClient(CatalogClientBase):

    MISSION_SENTINEL2 = 'Sentinel2'
    LEVEL1C = 'S2MSI1C'
    LEVEL2A = 'S2MSI2A'

    @staticmethod
    def _build_polygon(ulx, uly, brx, bry):
        return Polygon([(ulx, uly), (brx, uly), (brx, bry), (ulx, bry), (ulx, uly)])

    @staticmethod
    def _parse_product_ids(response) -> List[CreoCatalogEntry]:
        result = []
        for hit in response['features']:
            # https://creodias.eu/eo-data-finder-api-manual:
            # 31 means that product is orderable and waiting for download to our cache,
            # 32 means that product is ordered and processing is in progress,
            # 34 means that product is downloaded in cache,
            # 37 means that product is processed by our platform,
            # 0 means that already processed product is waiting in our platform

            # TODO: status is now "ONLINE" or "OFFLINE". Prefer using the Scala OpenSearchClient.
            if hit['properties']['status'] in {0, 34, 37}:
                result.append(
                    CreoCatalogEntry(hit['properties']['productIdentifier'].replace('.SAFE', ''), CatalogStatus.AVAILABLE))
            else:
                result.append(
                    CreoCatalogEntry(hit['properties']['productIdentifier'].replace('.SAFE', ''), CatalogStatus.ORDERABLE))
        return result

    def __init__(self, mission, level=None, product_type=None):
        super().__init__(mission, level, product_type)
        self.itemsperpage = 100
        self.maxpages = 100  # elasticsearch has a 10000 limit on the paged search

        status_forcelist = [502, 503, 504]
        MAX_RETRIES = 5
        retries = Retry(
            total=MAX_RETRIES,
            read=MAX_RETRIES,
            other=MAX_RETRIES,
            status=MAX_RETRIES,
            backoff_factor=0.1,
            status_forcelist=status_forcelist,
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )
        self._session = requests.Session()
        self._session.mount('https://', HTTPAdapter(max_retries=retries))
        self._session.mount('http://', HTTPAdapter(max_retries=retries))

    def __del__(self):
        del self._session
        self._session=None

    def catalogEntryFromProductId(self, product_id):
        return CreoCatalogEntry(product_id, CatalogStatus.AVAILABLE)

    def _query_page(self, start_date: datetime, end_date: datetime,
                    tile_id,
                    ulx, uly, brx, bry,
                    cldPrcnt,
                    from_index):

        query_params = [('processingLevel', self.level),
                        ('productType', self.product_type),
                        ('startDate', start_date.isoformat()),
                        ('cloudCover', '[0,' + str(int(cldPrcnt)) + ']'),
                        ('page', str(from_index)),
                        ('maxRecords', str(self.itemsperpage)),
                        ('sortParam', 'startDate'),
                        ('sortOrder', 'ascending'),
                        ('status', 'all'),
                        ('dataset', 'ESA-DATASET')]

        # optional parameters
        if end_date is not None:
            query_params.append(('completionDate', end_date.isoformat()))
        if tile_id is None:
            polygon = CreoCatalogClient._build_polygon(ulx, uly, brx, bry)
            query_params.append(('geometry', polygon.wkt))
        else:
            query_params.append(('productIdentifier', '%_T' + tile_id + '_%'))

        response = self._session.get('https://catalogue.dataspace.copernicus.eu/resto/api/collections/' + self.mission + '/search.json',
                                params=query_params)
        response.raise_for_status()

        response = response.json()

        self.logger.debug('Paged catalogs query returned %d results', response['properties']['itemsPerPage'])
        return response

    def _query_per_tile(self, start_date, end_date,
                        tile_id,
                        ulx, uly, brx, bry,
                        cldPrcnt) -> List[CreoCatalogEntry]:

        result = []

        for i in range(self.maxpages):
            response = self._query_page(start_date, end_date, tile_id, ulx, uly, brx, bry, cldPrcnt, i + 1)
            if i == 1:
                self.logger.debug(
                    f"Hits in catalogs: {response.get('properties', {}).get('totalResults')} exact: {response.get('properties', {}).get('exactCount')}"
                )
            chunk = CreoCatalogClient._parse_product_ids(response)
            if len(chunk) == 0: break
            result = result + chunk
        if len(result) >= self.itemsperpage * self.maxpages:
            raise Exception(
                "Total hits larger than 10000, which is not supported by paging: either split your job to multiple smaller or implement scroll or search_after.")

        return result

    def query(self, start_date, end_date,
              tile_ids=None,
              ulx=-180, uly=90, brx=180, bry=-90,
              cldPrcnt=100.) -> List[CreoCatalogEntry]:

        result = []
        if tile_ids is None:
            result = result + self._query_per_tile(start_date, end_date, None, ulx, uly, brx, bry, cldPrcnt)
        else:
            for itileid in tile_ids:
                result = result + self._query_per_tile(start_date, end_date, itileid, ulx, uly, brx, bry, cldPrcnt)

        self.logger.info('Number of products found: ' + str(len(result)))

        return result

    def query_offline(self, start_date, end_date, ulx=-180, uly=90, brx=180, bry=-90,cldPrcnt=100.) -> List[CreoCatalogEntry]:
        return [
            p for p in self.query(start_date=start_date, end_date=end_date, ulx=ulx, uly=uly, brx=brx, bry=bry,cldPrcnt=cldPrcnt)
            if p.getStatus() == CatalogStatus.ORDERABLE
        ]

    def query_product_paths(self, start_date, end_date, ulx, uly, brx, bry):
        products = self.query(start_date, datetime.combine(end_date, time.max), ulx=ulx, uly=uly, brx=brx, bry=bry)
        return [str(Path("/", p.getS3Bucket().lower(), p.getS3Key())) for p in products]

    def order(self, entries):
        tag = str(len(entries)) + 'products'
        if entries is not None:
            self.logger.info("Catalog found %d products (%d available, %d orderable, %d not-found)" % (
                len(entries),
                len(list(filter(lambda i: i.getStatus() == CatalogStatus.AVAILABLE, entries))),
                len(list(filter(lambda i: i.getStatus() == CatalogStatus.ORDERABLE, entries))),
                len(list(filter(lambda i: i.getStatus() == CatalogStatus.NOT_FOUND, entries)))
            ))
        for i in entries: self.logger.debug(str(i.getStatus()) + " -> " + i.getProductId())
        order = CreoOrder().order(entries, tag)
        return order

    def count(self, start_date, end_date,
              tile_ids=None,
              ulx=-180, uly=90, brx=180, bry=-90,
              cldPrcnt=100.):

        # ugly, but totalresults do not always return the exact number
        return len(self.query(start_date, end_date, tile_ids, ulx, uly, brx, bry, cldPrcnt))
