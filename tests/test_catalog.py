from unittest import skip

from base_test_class import BaseTestClass
from openeogeotrellis.layercatalog import LayerCatalog


class TestLayerCatalog(BaseTestClass):

    @skip("Depends on VITO infrastructure")
    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)
