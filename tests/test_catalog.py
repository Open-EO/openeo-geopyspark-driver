from unittest import skip

from openeogeotrellis.layercatalog import LayerCatalog

from .base_test_class import BaseTestClass


class TestLayerCatalog(BaseTestClass):

    @skip("Depends on VITO infrastructure")
    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)
