import re
from unittest import skip, TestCase

from openeogeotrellis.layercatalog import LayerCatalog
from .base_test_class import BaseTestClass

BaseTestClass.setup_local_spark()


class TestLayerCatalog(TestCase):

    @skip("Depends on VITO infrastructure")
    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)


def test_layercatalog_json():
    for layer in LayerCatalog().catalog.values():
        assert re.match(r'^[A-Za-z0-9_\-\.~\/]+$', layer['id'])
        # TODO enable these other checks too
        # assert 'stac_version' in layer
        # assert 'description' in layer
        # assert 'license' in layer
        # assert 'extent' in layer
