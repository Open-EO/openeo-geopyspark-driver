import mock
import pytest
from mock import MagicMock
from openeo_driver.errors import ProcessGraphComplexityException

from openeogeotrellis.layercatalog import get_layer_catalog


def test_load_collection_bands_no_extent():
    catalog = get_layer_catalog()
    with pytest.raises(ProcessGraphComplexityException):
        catalog.load_collection('TERRASCOPE_S2_TOC_V2',{'bands':['TOC-B03_10M']})

@mock.patch('openeogeotrellis.layercatalog.JavaGateway', autospec=True)
def test_load_collection_bands_with_extent(javagateway):
    catalog = get_layer_catalog()

    jvm_mock = MagicMock()
    javagateway.return_value.jvm = jvm_mock
    raster_layer = MagicMock()
    jvm_mock.geopyspark.geotrellis.TemporalTiledRasterLayer.return_value = raster_layer
    raster_layer.layerMetadata.return_value = '{' \
                                              '"crs":"EPSG:4326",\n' \
                                              '"cellType":"uint8",\n' \
                                              '"bounds":{"minKey":{"col":0,"row":0},"maxKey":{"col":1,"row":1}},\n' \
                                              '"extent":{"xmin":0,"ymin":0,"xmax":1,"ymax":1},\n' \
                                              '"layoutDefinition":{\n' \
                                              '"extent":{"xmin":0,"ymin":0,"xmax":1,"ymax":1},' \
                                              '"tileLayout":{"layoutCols":1, "layoutRows":1, "tileCols":256, "tileRows":256}' \
                                              '}' \
                                              '}'
    collection = catalog.load_collection('TERRASCOPE_S2_TOC_V2',{'from':'2019-01-01','to':'2019-01-01','bands':['TOC-B03_10M'],'left':4,'right':4.001,'top':52,'bottom':51.9999,'srs':4326})

    print(collection.metadata)
    assert len(collection.metadata.bands)==1
    assert collection.metadata.bands[0].name=='TOC-B03_10M'

    factory_mock = jvm_mock.org.openeo.geotrellis.file.Sentinel2PyramidFactory
    extent_mock = jvm_mock.geotrellis.vector.Extent.return_value

    jvm_mock.geotrellis.vector.Extent.assert_called_once_with(4.0, 51.9999, 4.001, 52.0)

    factory_mock.assert_called_once_with('urn:eop:VITO:TERRASCOPE_S2_TOC_V2', ['TOC-B03_10M'], '/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2')
    factory_mock.return_value.pyramid_seq.assert_called_once_with(extent_mock, "EPSG:4326", '2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00', {})

