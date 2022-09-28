import datetime

from openeogeotrellis.catalogs.creo import CreoCatalogClient


def test_creo_catalog():
    date = datetime.date(2018, 4, 1)
    l2a_catalog = CreoCatalogClient(mission="Sentinel2", level="LEVEL2A")
    results = l2a_catalog.query_product_paths(date, date, ulx=5, uly=51.1, brx=5.1, bry=51)
    filenames = [r.split("/")[-1] for r in results]
    assert "S2A_MSIL2A_20180401T105031_N0207_R051_T31UFS_20180401T144530.SAFE" in filenames
