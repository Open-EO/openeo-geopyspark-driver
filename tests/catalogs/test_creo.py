from openeogeotrellis.catalogs.base import CatalogStatus
from openeogeotrellis.catalogs.creo import CreoCatalogEntry


def test_entry():
    product_id = "S2B_MSIL2A_20191117T105229_N0213_R051_T31UET_20191117T134337"
    entry = CreoCatalogEntry(product_id, status=CatalogStatus.AVAILABLE)
    assert entry.getProductId() == product_id
    assert entry.getTileId() == "31UET"
    assert entry.getStatus() == CatalogStatus.AVAILABLE
    assert entry.getDateStr() == "20191117"
