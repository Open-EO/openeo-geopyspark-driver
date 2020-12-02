import mock
import pytest
from mock import MagicMock

from openeo_driver.backend import LoadParameters
from openeo_driver.errors import ProcessGraphComplexityException
from openeo_driver.utils import EvalEnv

from openeogeotrellis.layercatalog import get_layer_catalog


def test_load_collection_bands_missing_required_extent():
    catalog = get_layer_catalog()
    load_params = LoadParameters(bands=['TOC-B03_10M'])
    env = EvalEnv({'require_bounds': True})
    with pytest.raises(ProcessGraphComplexityException):
        catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=env)


@mock.patch('openeogeotrellis.layercatalog.JavaGateway', autospec=True)
def test_load_collection_bands_with_required_extent(javagateway):
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

    load_params = LoadParameters(
        temporal_extent=('2019-01-01', '2019-01-01'),
        bands=['TOC-B03_10M'],
        spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326}
    )
    env = EvalEnv({'require_bounds': True})
    collection = catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=env)

    print(collection.metadata)
    assert len(collection.metadata.bands)==1
    assert collection.metadata.bands[0].name=='TOC-B03_10M'

    factory_mock = jvm_mock.org.openeo.geotrellis.file.Sentinel2PyramidFactory
    extent_mock = jvm_mock.geotrellis.vector.Extent.return_value
    cellsize_mock = jvm_mock.geotrellis.raster.CellSize.return_value

    jvm_mock.geotrellis.vector.Extent.assert_called_once_with(4.0, 51.9999, 4.001, 52.0)

    factory_mock.assert_called_once_with("https://services.terrascope.be/catalogue", 'urn:eop:VITO:TERRASCOPE_S2_TOC_V2', ['TOC-B03_10M'], '/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2', cellsize_mock)
    factory_mock.return_value.pyramid_seq.assert_called_once_with(extent_mock, "EPSG:4326", '2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00', {}, '')

