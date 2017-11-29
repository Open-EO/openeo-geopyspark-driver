from openeogeotrellis.geopyspark.layercatalog import  LayerCatalog
from base_test_class import BaseTestClass


class TestLayerCatalog(BaseTestClass):

    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)
