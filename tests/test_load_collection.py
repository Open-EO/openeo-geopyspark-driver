import mock
import pytest
from mock import MagicMock, ANY

from openeo_driver.backend import LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import OpenEOApiException
from openeo_driver.utils import EvalEnv
from py4j.java_gateway import JavaGateway

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.layercatalog import get_layer_catalog
import geopyspark as gps

from .test_api_result import CreoApiMocker, TerrascopeApiMocker

@pytest.fixture
def jvm_mock():
    with mock.patch('openeogeotrellis.layercatalog.get_jvm') as get_jvm:
        jvm_mock = get_jvm.return_value
        raster_layer = MagicMock()
        jvm_mock.geopyspark.geotrellis.TemporalTiledRasterLayer.return_value = raster_layer
        raster_layer.layerMetadata.return_value = """{
            "crs": "EPSG:4326",
            "cellType": "uint8",
            "bounds": {"minKey": {"col":0, "row":0}, "maxKey": {"col": 1, "row": 1}},
            "extent": {"xmin": 0,"ymin": 0, "xmax": 1,"ymax": 1},
            "layoutDefinition": {
                "extent": {"xmin": 0, "ymin": 0,"xmax": 1,"ymax": 1},
                "tileLayout": {"layoutCols": 1, "layoutRows": 1, "tileCols": 256, "tileRows": 256}
            }
        }"""
        yield jvm_mock


def test_load_collection_bands_missing_required_extent():
    catalog = get_layer_catalog()
    load_params = LoadParameters(bands=['TOC-B03_10M'])
    env = EvalEnv({'require_bounds': True})
    with pytest.raises(OpenEOApiException):
        catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=env)


def test_load_collection_sar_backscatter_compatible(jvm_mock):
    catalog = get_layer_catalog()

    load_params = LoadParameters(temporal_extent=("2021-02-08T10:36:00Z", "2021-02-08T10:36:00Z"),
                                 spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326},
                                 sar_backscatter=SarBackscatterArgs())
    catalog.load_collection('SENTINEL1_GRD', load_params=load_params,
                            env=EvalEnv({'pyramid_levels': 'highest'}))

    factory_mock = jvm_mock.org.openeo.geotrellissentinelhub.PyramidFactory.withGuardedRateLimiting
    sample_type_mock = jvm_mock.org.openeo.geotrellissentinelhub.SampleType.withName.return_value
    cellsize_mock = jvm_mock.geotrellis.raster.CellSize(10, 10)
    projected_polys = jvm_mock.org.openeo.geotrellis.ProjectedPolygons.fromExtent.return_value

    reproject = (getattr(getattr(jvm_mock.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$")).reproject
    reproject.assert_called_once_with(projected_polys,32631)
    reprojected = reproject.return_value

    factory_mock.assert_called_once_with("https://services.sentinel-hub.com", "sentinel-1-grd", "sentinel-1-grd", "???", "!!!",
                                         {"backCoeff": "GAMMA0_TERRAIN", "orthorectify": True}, sample_type_mock,
                                         cellsize_mock, False)

    datacubeParams = jvm_mock.org.openeo.geotrelliscommon.DataCubeParameters.return_value
    jvm_mock.org.openeo.geotrellissentinelhub.SampleType.withName.assert_called_once_with("FLOAT32")
    factory_mock.return_value.datacube_seq.assert_called_once_with(reprojected.polygons(),
                                                 ANY, '2021-02-08T10:36:00+00:00', '2021-02-08T10:36:00+00:00',
                                                 ['VV', 'VH', 'HH', 'HV'], {},
                                                 datacubeParams)


def test_load_collection_sar_backscatter_incompatible():
    catalog = get_layer_catalog()
    load_params = LoadParameters(sar_backscatter=SarBackscatterArgs())
    with pytest.raises(OpenEOApiException) as exc_info:
        catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=EvalEnv())

    assert exc_info.value.status_code == 400
    assert (exc_info.value.args[0] ==
            """Process "sar_backscatter" is not applicable for collection TERRASCOPE_S2_TOC_V2.""")


def test_load_file_oscars(jvm_mock):
    catalog = get_layer_catalog()
    load_params = LoadParameters(temporal_extent=("2010-01-01T10:36:00Z", "2012-01-01T10:36:00Z"),
                                 spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326})
    env = EvalEnv()
    env = env.push({"pyramid_levels": "single"})
    collection = catalog.load_collection('COPERNICUS_30', load_params=load_params, env=env)
    assert(collection.metadata.spatial_dimensions[0].step == 0.002777777777777778)
    assert(collection.metadata.spatial_dimensions[1].step == 0.002777777777777778)
    cellsize_call_mock = jvm_mock.geotrellis.raster.CellSize

    cellsize_call_mock.assert_called_once_with(0.002777777777777778, 0.002777777777777778)


def test_load_file_oscars_resample(jvm_mock):
    catalog = get_layer_catalog()
    load_params = LoadParameters(temporal_extent=("2010-01-01T10:36:00Z", "2012-01-01T10:36:00Z"),
                                 spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326},
                                 target_resolution=[15,15],
                                 target_crs=3857,
                                 featureflags={"experimental":True}
                                 )
    env = EvalEnv()
    env = env.push({"pyramid_levels": "single"})

    factory_mock = jvm_mock.org.openeo.geotrellis.file.Sentinel2PyramidFactory
    extent_mock = jvm_mock.geotrellis.vector.Extent.return_value
    cellsize_call_mock = jvm_mock.geotrellis.raster.CellSize
    cellsize_mock = jvm_mock.geotrellis.raster.CellSize(15, 15)

    datacubeParams = jvm_mock.org.openeo.geotrelliscommon.DataCubeParameters.return_value


    collection = catalog.load_collection('COPERNICUS_30', load_params=load_params, env=env)
    assert(collection.metadata.spatial_dimensions[0].step == 0.002777777777777778)
    assert(collection.metadata.spatial_dimensions[1].step == 0.002777777777777778)

    jvm_mock.geotrellis.vector.Extent.assert_called_once_with(4.0, 51.9999, 4.001, 52.0)
    cellsize_call_mock.assert_called_with(15,15)

    factory_mock.assert_called_once_with('https://services.terrascope.be/catalogue', 'urn:eop:VITO:COP_DEM_GLO_30M_COG', ['DEM'], '/data/MTDA/DEM/COP_DEM_30M_COG', cellsize_mock, True)
    factory_mock.return_value.datacube_seq.assert_called_once_with(ANY, '2010-01-01T10:36:00+00:00', '2012-01-01T10:36:00+00:00', {}, '', datacubeParams)


def test_load_collection_old_and_new_band_names(jvm_mock):
    catalog = get_layer_catalog()

    temporal_extent = ('2019-01-01', '2019-01-01')
    spatial_extent = {'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326}

    for bands in [['TOC-B03_10M'], ['B03']]:
        load_params = LoadParameters(temporal_extent=temporal_extent, bands=bands, spatial_extent=spatial_extent)
        collection = catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=EvalEnv())

        cellsize_call_mock = jvm_mock.geotrellis.raster.CellSize

        cellsize_call_mock.assert_called_with(15, 15)

        assert len(collection.metadata.bands) == 1
        assert collection.metadata.bands[0].name == bands[0]
        assert collection.metadata.bands[0].aliases == ['TOC-B03_10M']
        assert collection.metadata.temporal_dimension.extent == ('2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00')


def test_create_params():
    pysc = gps.get_spark_context()
    gateway = JavaGateway(eager_load=True, gateway_parameters=pysc._gateway.gateway_parameters)
    jvm = gateway.jvm
    datacubeParams = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
    datacubeParams.tileSize = 256
    assert datacubeParams.tileSize == 256


def test_reprojection():
    """
    It is important that reprojection in Python and Geotrellis give the same result, for alignment of bounding boxes!
    @return:
    """
    reprojected = GeopysparkDataCube._reproject_extent("EPSG:4326","EPSG:32631",5.071, 51.21,5.1028,51.23)
    print(reprojected)
    assert reprojected.xmin == 644594.8230399278
    assert reprojected.ymin == 5675216.271413178
    assert reprojected.xmax == 646878.5028127492
    assert reprojected.ymax == 5677503.191395153


def test_load_collection_bands_with_required_extent(jvm_mock):
    catalog = get_layer_catalog()

    load_params = LoadParameters(
        temporal_extent=('2019-01-01', '2019-01-01'),
        bands=['TOC-B03_10M'],
        spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326}
    )
    env = EvalEnv({'require_bounds': True})
    collection = catalog.load_collection('TERRASCOPE_S2_TOC_V2', load_params=load_params, env=env)

    print(collection.metadata)
    assert len(collection.metadata.bands) == 1
    assert collection.metadata.bands[0].name == 'TOC-B03_10M'

    factory_mock = jvm_mock.org.openeo.geotrellis.file.Sentinel2PyramidFactory
    extent_mock = jvm_mock.geotrellis.vector.Extent.return_value

    cellsize_call_mock = jvm_mock.geotrellis.raster.CellSize

    cellsize_call_mock.assert_called_once_with(15, 15)
    jvm_mock.geotrellis.vector.Extent.assert_called_once_with(4.0, 51.9999, 4.001, 52.0)

    factory_mock.assert_called_once_with("https://services.terrascope.be/catalogue", 'urn:eop:VITO:TERRASCOPE_S2_TOC_V2', ['TOC-B03_10M'], '/data/MTDA/TERRASCOPE_Sentinel2/TOC_V2', cellsize_call_mock.return_value,False)
    factory_mock.return_value.pyramid_seq.assert_called_once_with(extent_mock, "EPSG:4326", '2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00', {}, '')


def test_load_collection_data_cube_params(jvm_mock):
    catalog = get_layer_catalog()

    load_params = LoadParameters(
        temporal_extent=('2019-01-01', '2019-01-01'),
        bands=['temperature-mean'],
        spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326}

    )
    load_params['featureflags'] = {
        "tilesize": 1,
        "experimental": True
    }
    env = EvalEnv({'require_bounds': True, 'pyramid_levels': 'highest'})
    collection = catalog.load_collection('AGERA5', load_params=load_params, env=env)

    print(collection.metadata)
    assert len(collection.metadata.bands) == 1
    assert collection.metadata.bands[0].name == 'temperature-mean'

    factory_mock = jvm_mock.org.openeo.geotrellis.file.AgEra5PyramidFactory
    projected_polys = jvm_mock.org.openeo.geotrellis.ProjectedPolygons.fromExtent.return_value
    datacubeParams = jvm_mock.org.openeo.geotrelliscommon.DataCubeParameters.return_value

    jvm_mock.geotrellis.vector.Extent.assert_called_once_with(4.0, 51.9999, 4.001, 52.0)

    factory_mock.assert_called_once_with('/data/MEP/ECMWF/AgERA5/*/*/AgERA5_dewpoint-temperature_*.tif', ['temperature-mean'], '.+_(\\d{4})(\\d{2})(\\d{2})\\.tif')
    factory_mock.return_value.datacube_seq.assert_called_once_with(projected_polys, '2019-01-01T00:00:00+00:00', '2019-01-01T00:00:00+00:00', {}, '',datacubeParams)
    getattr(datacubeParams,'tileSize_$eq').assert_called_once_with(1)
    getattr(datacubeParams, 'layoutScheme_$eq').assert_called_once_with('FloatingLayoutScheme')


def test_load_collection_common_name_user_selected(jvm_mock):
    catalog = get_layer_catalog()
    load_params = LoadParameters(
        temporal_extent=('2019-01-01', '2019-01-01'),
        spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326},
        bands=['B03'],
    )

    load_params.backend_provider = 'sentinelhub'
    collection = catalog.load_collection('SENTINEL2_L2A', load_params=load_params, env=EvalEnv())
    assert collection.metadata.get('id') == 'SENTINEL2_L2A_SENTINELHUB'

    load_params.backend_provider = 'terrascope'
    collection = catalog.load_collection('SENTINEL2_L2A', load_params=load_params, env=EvalEnv())
    assert collection.metadata.get('id') == 'TERRASCOPE_S2_TOC_V2'


@pytest.mark.parametrize(["missing_products", "expected_source"], [
    (False, "TERRASCOPE_S2_TOC_V2"),
    (True, "SENTINEL2_L2A_SENTINELHUB"),
])
def test_load_collection_common_name_by_missing_products(jvm_mock, requests_mock, missing_products, expected_source):
    catalog = get_layer_catalog()

    load_params = LoadParameters(
        temporal_extent=('2020-03-01', '2020-03-03'),
        spatial_extent={'west': 4, 'east': 4.001, 'north': 52, 'south': 51.9999, 'crs': 4326},
        bands=['B03'],
    )

    requests_mock.get(
        "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?processingLevel=LEVEL1C&startDate=2020-03-01T00%3A00%3A00&cloudCover=%5B0%2C100%5D&page=1&maxRecords=100&sortParam=startDate&sortOrder=ascending&status=all&dataset=ESA-DATASET&completionDate=2020-03-03T23%3A59%3A59.999999&geometry=POLYGON+%28%284+52%2C+4.001+52%2C+4.001+51.9999%2C+4+51.9999%2C+4+52%29%29",
        json=CreoApiMocker.feature_collection(features=[{"tile_id": "16WEA"}, {"tile_id": "16WDA"}])
    )
    requests_mock.get(
        "https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?processingLevel=LEVEL1C&startDate=2020-03-01T00%3A00%3A00&cloudCover=%5B0%2C100%5D&page=2&maxRecords=100&sortParam=startDate&sortOrder=ascending&status=all&dataset=ESA-DATASET&completionDate=2020-03-03T23%3A59%3A59.999999&geometry=POLYGON+%28%284+52%2C+4.001+52%2C+4.001+51.9999%2C+4+51.9999%2C+4+52%29%29",
        json=CreoApiMocker.feature_collection(features=[])
    )

    if missing_products:
        tfs = [{"tile_id": "16WEA"}]
    else:
        tfs = [{"tile_id": "16WEA"}, {"tile_id": "16WDA"}]
    from_index = 1
    requests_mock.get(
        f"https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=4%2C51.9999%2C4.001%2C52&sortKeys=title&startIndex={from_index}&start=2020-03-01T00%3A00%3A00&end=2020-03-03T23%3A59%3A59.999999&cloudCover=%5B0%2C100.0%5D",
        json=TerrascopeApiMocker.feature_collection(features=tfs),
    )
    from_index += len(tfs)
    requests_mock.get(
        f"https://services.terrascope.be/catalogue/products?collection=urn%3Aeop%3AVITO%3ATERRASCOPE_S2_TOC_V2&bbox=4%2C51.9999%2C4.001%2C52&sortKeys=title&startIndex={from_index}&start=2020-03-01T00%3A00%3A00&end=2020-03-03T23%3A59%3A59.999999&cloudCover=%5B0%2C100.0%5D",
        json=TerrascopeApiMocker.feature_collection(features=[]),
    )

    collection = catalog.load_collection('SENTINEL2_L2A', load_params=load_params, env=EvalEnv())
    assert collection.metadata.get('id') == expected_source


def test_load_disk_collection_pyramid(imagecollection_with_two_bands_and_three_dates,backend_implementation,tmp_path):
    out = imagecollection_with_two_bands_and_three_dates.write_assets(filename=tmp_path/"out.tif",format="GTiff",format_options=dict(batch_mode=True))
    #example output path: /tmp/pytest-of-driesj/pytest-1/test_load_disk_collection0/openEO_2017-10-25Z.tif
    cube = backend_implementation.load_disk_data(format="GTiff",glob_pattern=str(tmp_path/"openEO_*.tif"),options=dict(date_regex=".*\/openEO_(\d{4})-(\d{2})-(\d{2})Z.tif"),load_params=LoadParameters(),env=EvalEnv())
    cube = cube.add_dimension("bands","band1","bands")

    assert len(cube.metadata.spatial_dimensions) == 2
    assert len(cube.pyramid.levels) == 2


def test_load_disk_collection_batch(imagecollection_with_two_bands_and_three_dates,backend_implementation,tmp_path):
    out = imagecollection_with_two_bands_and_three_dates.write_assets(filename=tmp_path/"out.tif",format="GTiff",format_options=dict(batch_mode=True))
    #example output path: /tmp/pytest-of-driesj/pytest-1/test_load_disk_collection0/openEO_2017-10-25Z.tif
    load_params = LoadParameters()

    load_params.spatial_extent = dict(west=2,east=3,south=1,north=2)
    env = EvalEnv(dict(pyramid_levels="1"))

    cube = backend_implementation.load_disk_data(format="GTiff", glob_pattern=str(tmp_path/"openEO_*.tif"), options=dict(date_regex=".*\/openEO_(\d{4})-(\d{2})-(\d{2})Z.tif"), load_params=load_params, env=env)
    cube = cube.add_dimension("bands","band1","bands")

    assert len(cube.metadata.spatial_dimensions) == 2
    assert len(cube.pyramid.levels)==1
    print(cube.get_max_level().layer_metadata)