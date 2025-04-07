from openeogeotrellis.catalogs.oscars import OscarsCatalogEntry


def test_entry():
    product_id = "S2A_20200913T110701_30TUM_TOC_V200"
    entry = OscarsCatalogEntry(product_id)
    assert entry.getProductId() == product_id
    assert entry.getTileId() == "30TUM"
    assert entry.getDateStr() == "20200913"
