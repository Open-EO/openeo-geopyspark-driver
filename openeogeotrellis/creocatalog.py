from pathlib import Path
from datetime import datetime, timedelta

from dias_s2chain.catalogs.creo import CatalogClient


class CreoCatalog():

    def __init__(self, mission, level):
        self.catalog_client = CatalogClient(mission, level)

    def query_product_paths(self, start_date, end_date, ulx, uly, brx, bry):
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date) + timedelta(days=1)
        products = self.catalog_client.query(start, end, ulx=ulx, uly=uly, brx=brx, bry=bry)
        return [str(Path("/", p.getS3Bucket().lower(), p.getS3Key())) for p in products]
