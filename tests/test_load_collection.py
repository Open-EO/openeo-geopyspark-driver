import mock
import pytest
from mock import MagicMock

from openeo_driver.backend import LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import ProcessGraphComplexityException, OpenEOApiException
from openeo_driver.utils import EvalEnv
from py4j.java_gateway import JavaGateway

from openeogeotrellis.layercatalog import get_layer_catalog
import geopyspark as gps


def test_load_collection_bands_missing_required_extent():
    catalog = get_layer_catalog()
    load_params = LoadParameters(bands=['TOC-B03_10M'])
    env = EvalEnv({'require_bounds': True})
    with pytest.raises(ProcessGraphComplexityException):
        catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=env)


def test_load_collection_sar_backscatter_compatible():
    catalog = get_layer_catalog()
    load_params = LoadParameters(temporal_extent=("2021-02-08T10:36:00Z", "2021-02-08T10:36:00Z"),
                                 spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326},
                                 sar_backscatter=SarBackscatterArgs())
    catalog.load_collection('SENTINEL1_GAMMA0_SENTINELHUB', load_params=load_params, env=EvalEnv())


def test_load_collection_sar_backscatter_incompatible():
    catalog = get_layer_catalog()
    load_params = LoadParameters(sar_backscatter=SarBackscatterArgs())
    with pytest.raises(OpenEOApiException) as exc_info:
        catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=EvalEnv())

    assert exc_info.value.status_code == 400
    assert (exc_info.value.args[0] ==
            """Process "sar_backscatter" is not applicable for collection TERRASCOPE_S2_TOC_V2.""")


@mock.patch('openeogeotrellis.layercatalog.get_jvm')
def test_load_collection_old_and_new_band_names(get_jvm):
    catalog = get_layer_catalog()

    jvm_mock = get_jvm.return_value
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

    temporal_extent = ('2019-01-01', '2019-01-01')
    spatial_extent = {'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326}

    for bands in [['TOC-B03_10M'], ['B03']]:
        load_params = LoadParameters(temporal_extent=temporal_extent, bands=bands, spatial_extent=spatial_extent)
        collection = catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=EvalEnv())

        assert len(collection.metadata.bands) == 1
        assert collection.metadata.bands[0].name == bands[0]
        assert collection.metadata.bands[0].aliases == ['TOC-B03_10M']


def test_create_params():
    pysc = gps.get_spark_context()
    gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
    jvm = gateway.jvm
    datacubeParams = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
    datacubeParams.tileSize = 256
    assert datacubeParams.tileSize == 256

@mock.patch('openeogeotrellis.layercatalog.get_jvm')
def test_load_collection_bands_with_required_extent(get_jvm):
    catalog = get_layer_catalog()

    jvm_mock = get_jvm.return_value
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

    factory_mock.assert_called_once_with("https://services.terrascope.be/catalogue", 'urn:eop:VITO:TERRASCOPE_S2_TOC_V2', ['TOC-B03_10M'], '/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2', cellsize_mock,False)
    factory_mock.return_value.pyramid_seq.assert_called_once_with(extent_mock, "EPSG:4326", '2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00', {}, '')


@mock.patch('openeogeotrellis.layercatalog.get_jvm')
def test_load_collection_data_cube_params(get_jvm):
    catalog = get_layer_catalog()

    jvm_mock = get_jvm.return_value
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
        bands=['temperature-mean'],
        spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326}

    )
    load_params['featureflags'] = {
      "tilesize":1,
      "experimental":True
   }
    env = EvalEnv({'require_bounds': True, 'pyramid_levels':'highest'})
    collection = catalog.load_collection('AGERA5', load_params=load_params, env=env)

    print(collection.metadata)
    assert len(collection.metadata.bands)==1
    assert collection.metadata.bands[0].name=='temperature-mean'

    factory_mock = jvm_mock.org.openeo.geotrellis.file.AgEra5PyramidFactory
    projected_polys = jvm_mock.org.openeo.geotrellis.ProjectedPolygons.fromExtent.return_value
    datacubeParams = jvm_mock.org.openeo.geotrelliscommon.DataCubeParameters.return_value

    jvm_mock.geotrellis.vector.Extent.assert_called_once_with(4.0, 51.9999, 4.001, 52.0)

    factory_mock.assert_called_once_with('/data/MEP/ECMWF/AgERA5/*/*/AgERA5_dewpoint-temperature_*.tif', ['temperature-mean'], '.+_(\\d{4})(\\d{2})(\\d{2})\\.tif')
    factory_mock.return_value.datacube_seq.assert_called_once_with(projected_polys, '2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00', {}, '',datacubeParams)
    getattr(datacubeParams,'tileSize_$eq').assert_called_once_with(1)
    getattr(datacubeParams, 'layoutScheme_$eq').assert_called_once_with('FloatingLayoutScheme')

