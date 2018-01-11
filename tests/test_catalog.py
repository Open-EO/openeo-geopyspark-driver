from unittest import TestCase

from openeogeotrellis.layercatalog import LayerCatalog


class TestLayerCatalog(TestCase):

    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)
