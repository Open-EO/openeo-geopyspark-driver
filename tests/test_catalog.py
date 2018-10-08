from unittest import skip, TestCase

from .base_test_class import BaseTestClass
BaseTestClass.setup_local_spark()

from openeogeotrellis.layercatalog import LayerCatalog


class TestLayerCatalog(TestCase):

    @skip("Depends on VITO infrastructure")
    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)
