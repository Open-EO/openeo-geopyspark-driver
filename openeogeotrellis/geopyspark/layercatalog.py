from typing import List
import geopyspark as gps
class LayerCatalog():
    """Catalog providing access to GeoPySpark layers"""
    def __init__(self):
        pass

    def layers(self) -> List:
        """Returns all available layers."""
        #TODO make this work with Kerberos authentication
        store = gps.AttributeStore("accumulo://driesj@epod6.vgt.vito.be:2181/hdp-accumulo-instance")
        layers = store.layers()
        print(layers)

