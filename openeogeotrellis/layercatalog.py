import unittest
from typing import List
from unittest import TestCase

import geopyspark as gps


class LayerCatalog(TestCase):
    """Catalog providing access to GeoPySpark layers"""
    def __init__(self):
        pass

    @unittest.skip("Test depends on internal VITO infrastructure")
    def layers(self) -> List:
        """Returns all available layers."""
        #TODO make this work with Kerberos authentication
        store = gps.AttributeStore("accumulo://driesj@epod6.vgt.vito.be:2181/hdp-accumulo-instance")
        layers = store.layers()
        print(layers)

