import unittest
from typing import List

import geopyspark as gps
import json
import os

class LayerCatalog():

    """Catalog providing access to GeoPySpark layers"""
    def __init__(self):
        if os.path.isfile("layercatalog.json"):
            with open("layercatalog.json","r") as f:
                self.catalog = {layer["data_id"]:layer for layer in json.load(f)}
        else:
            raise RuntimeError("layercatalog.json not found, please make sure that it is available in the working directory.")

    def layers(self) -> List:
        """Returns all available layers."""
        #TODO make this work with Kerberos authentication
        store = gps.AttributeStore("accumulo+kerberos://epod6.vgt.vito.be:2181/hdp-accumulo-instance")
        layers = store.layers()
        return list(self.catalog.items())

    def layer(self,product_id) -> List:
        """Returns all available layers."""
        if product_id in self.catalog:
            self.catalog[product_id]
        else:
            raise ValueError("Unknown collection id: " + product_id)



