import json
import shutil
import textwrap
from mock import MagicMock
from pathlib import Path
from unittest import mock

import pytest
from pytest import approx
from openeo_driver.save_result import ImageCollectionResult
from shapely.geometry import shape
from osgeo import gdal

from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.utils import read_json
from openeogeotrellis.deploy.batch_job import (
    extract_result_metadata,
    run_job, _get_tracker,
    _convert_asset_outputs_to_s3_urls,
    _convert_job_metadatafile_outputs_to_s3_urls,
    read_projection_extension_metadata,
    parse_projection_extension_metadata,
    extract_crs_epsg_code_from_wkt_string,
    _get_projection_extension_metadata,
)
from openeogeotrellis.utils import get_jvm
from openeogeotrellis._version import __version__
from tests.data import get_test_data_file


def test_extract_result_metadata():
    tracer = DryRunDataTracer()
    cube = tracer.load_collection(collection_id="Sentinel2", arguments={
        "temporal_extent": ["2020-02-02", "2020-03-03"],
    })
    cube = cube.filter_bbox(west=4, south=51, east=5, north=52)

    metadata = extract_result_metadata(tracer)
    expected = {
        "bbox": [4, 51, 5, 52],
        "geometry": {"type": "Polygon", "coordinates": (((4.0, 51.0), (4.0, 52.0), (5.0, 52.0), (5.0, 51.0), (4.0, 51.0)),)},
        "area": {"value": approx(7725459381.443416, 0.01), "unit": "square meter"},
        "start_datetime": "2020-02-02T00:00:00Z",
        "end_datetime": "2020-03-03T00:00:00Z",
        "links":[]
    }
    assert metadata == expected


def test_extract_result_metadata_aggregate_spatial():
    tracer = DryRunDataTracer()
    cube = tracer.load_collection(collection_id="Sentinel2", arguments={
        "temporal_extent": ["2020-02-02", "2020-03-03"],
    })
    cube = cube.filter_bbox(west=4, south=51, east=5, north=52)

    geometries = shape(read_json(get_test_data_file("multipolygon01.geojson")))
    cube = cube.aggregate_spatial(geometries=geometries, reducer="mean")

    metadata = extract_result_metadata(tracer)
    expected = {
        "bbox": (5.0, 5.0, 45.0, 40.0),
        "geometry": {
            'type': 'MultiPolygon',
            'coordinates': [
                (((30.0, 20.0), (45.0, 40.0), (10.0, 40.0), (30.0, 20.0)),),
                (((15.0, 5.0), (40.0, 10.0), (10.0, 20.0), (5.0, 10.0), (15.0, 5.0)),)
            ],
        },
        "area": {"value": approx(6797677574525.158, 0.01), "unit": "square meter"},
        "start_datetime": "2020-02-02T00:00:00Z",
        "end_datetime": "2020-03-03T00:00:00Z",
        "links": []
    }
    assert metadata == expected


def test_extract_result_metadata_aggregate_spatial_delayed_vector():
    tracer = DryRunDataTracer()
    cube = tracer.load_collection(collection_id="Sentinel2", arguments={
        "temporal_extent": ["2020-02-02", "2020-03-03"],
    })
    cube = cube.filter_bbox(west=4, south=51, east=5, north=52)
    geometries = DelayedVector(str(get_test_data_file("multipolygon01.geojson")))
    cube = cube.aggregate_spatial(geometries=geometries, reducer="mean")

    metadata = extract_result_metadata(tracer)
    expected = {
        "bbox": (5.0, 5.0, 45.0, 40.0),
        "geometry": {
            'type': 'Polygon',
            'coordinates': (((5.0, 5.0), (5.0, 40.0), (45.0, 40.0), (45.0, 5.0), (5.0, 5.0)),),
        },
        "area": {"value": approx(6763173869883.0, 1.0), "unit": "square meter"},
        "start_datetime": "2020-02-02T00:00:00Z",
        "end_datetime": "2020-03-03T00:00:00Z",
        "links": []
    }
    assert metadata == expected

@mock.patch('openeo_driver.ProcessGraphDeserializer.evaluate')
def test_run_job(evaluate, tmp_path):
    cube_mock = MagicMock()
    asset_meta = {"openEO01-01.tif": {"href": "tmp/openEO01-01.tif", "roles": "data"},"openEO01-05.tif": {"href": "tmp/openEO01-05.tif", "roles": "data"}}
    cube_mock.write_assets.return_value = asset_meta
    evaluate.return_value = ImageCollectionResult(cube=cube_mock, format="GTiff", options={"multidate":True})
    t = _get_tracker()
    t.setGlobalTracking(True)
    t.clearGlobalTracker()
    #tracker reset, so get it again
    t = _get_tracker()
    PU_COUNTER = "Sentinelhub_Processing_Units"
    t.registerDoubleCounter(PU_COUNTER)
    t.add(PU_COUNTER, 1.4)
    t.addInputProducts("collectionName", ["http://myproduct1", "http://myproduct2"])
    t.addInputProducts("collectionName", ["http://myproduct3"])
    t.addInputProducts("other_collectionName", ["http://myproduct4"])
    t.add(PU_COUNTER, 0.4)

    run_job(
        job_specification={'process_graph': {'nop': {'process_id': 'discard_result', 'result': True}}},
        output_file=tmp_path /"out", metadata_file=tmp_path / "metadata.json", api_version="1.0.0", job_dir="./",
        dependencies={}, user_id="jenkins"
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(tmp_path/"metadata.json")
    assert metadata_result == {'assets': asset_meta,
                               'bbox': None,
                               'end_datetime': None,
                               'epsg': None,
                               'geometry': None,
                               'area': None,
                               'unique_process_ids': ['discard_result'],
                               'instruments': [],
                               'links': [{'href': 'http://myproduct4', 'rel': 'derived_from', 'title': 'Derived from http://myproduct4'},
                                         {'href': 'http://myproduct1', 'rel': 'derived_from', 'title': 'Derived from http://myproduct1'},
                                         {'href': 'http://myproduct2', 'rel': 'derived_from', 'title': 'Derived from http://myproduct2'},
                                         {'href': 'http://myproduct3', 'rel': 'derived_from', 'title': 'Derived from http://myproduct3'}],
                               'processing:facility': 'VITO - SPARK',
                               'processing:software': 'openeo-geotrellis-' + __version__,
                               'start_datetime': None,
                               'usage': {'sentinelhub': {'unit': 'sentinelhub_processing_unit',
                                                         'value': 1.7999999999999998}}
                               }
    t.setGlobalTracking(False)


@mock.patch("openeo_driver.ProcessGraphDeserializer.evaluate")
def test_run_job_get_projection_extension_metadata(evaluate, tmp_path):
    cube_mock = MagicMock()
    first_asset = str(
        get_test_data_file(
            "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
        )
    )
    asset_meta = {
        first_asset: {
            "href": first_asset,
            "roles": "data",
        },
        # The second file does not exist on the filesystem.
        # This triggers that the projection extension metadata is put on the
        # bands, for the remaining assets (Here there is only 1 other asset off course).
        "openEO01-05.tif": {"href": "tmp/openEO01-05.tif", "roles": "data"},
    }
    cube_mock.write_assets.return_value = asset_meta
    evaluate.return_value = ImageCollectionResult(
        cube=cube_mock, format="GTiff", options={"multidate": True}
    )
    t = _get_tracker()
    t.setGlobalTracking(True)
    t.clearGlobalTracker()
    # tracker reset, so get it again
    t = _get_tracker()
    PU_COUNTER = "Sentinelhub_Processing_Units"
    t.registerDoubleCounter(PU_COUNTER)
    t.add(PU_COUNTER, 1.4)
    t.addInputProducts("collectionName", ["http://myproduct1", "http://myproduct2"])
    t.addInputProducts("collectionName", ["http://myproduct3"])
    t.addInputProducts("other_collectionName", ["http://myproduct4"])
    t.add(PU_COUNTER, 0.4)

    run_job(
        job_specification={
            "process_graph": {"nop": {"process_id": "discard_result", "result": True}}
        },
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "metadata.json",
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(tmp_path / "metadata.json")
    assert metadata_result == {
        "assets": {
            first_asset: {
                "href": first_asset,
                "roles": "data",
                "proj:bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
                "proj:epsg": 4326,
                "proj:shape": [720, 1188],
            },
            "openEO01-05.tif": {"href": "tmp/openEO01-05.tif", "roles": "data"},
        },
        "bbox": None,
        "end_datetime": None,
        "epsg": None,
        "geometry": None,
        "area": None,
        "unique_process_ids": ["discard_result"],
        "instruments": [],
        "links": [
            {
                "href": "http://myproduct4",
                "rel": "derived_from",
                "title": "Derived from http://myproduct4",
            },
            {
                "href": "http://myproduct1",
                "rel": "derived_from",
                "title": "Derived from http://myproduct1",
            },
            {
                "href": "http://myproduct2",
                "rel": "derived_from",
                "title": "Derived from http://myproduct2",
            },
            {
                "href": "http://myproduct3",
                "rel": "derived_from",
                "title": "Derived from http://myproduct3",
            },
        ],
        "processing:facility": "VITO - SPARK",
        "processing:software": "openeo-geotrellis-" + __version__,
        "start_datetime": None,
        "usage": {
            "sentinelhub": {
                "unit": "sentinelhub_processing_unit",
                "value": 1.7999999999999998,
            }
        },
    }
    t.setGlobalTracking(False)


@mock.patch("openeo_driver.ProcessGraphDeserializer.evaluate")
def test_run_job_get_projection_extension_metadata_all_assets_same_epsg_and_bbox(
    evaluate, tmp_path
):
    """When there are two raster assets with the same projection metadata, it should put
    those metadata at the level of the item instead of the individual bands.
    """
    cube_mock = MagicMock()

    first_asset_path = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    first_asset = str(first_asset_path)
    # For the second file: use a copy of the first file  (in the temp dir) so we know
    # that GDAL will find exactly the same metadata under a different asset path.
    second_asset_path = tmp_path / first_asset_path.name
    second_asset = str(second_asset_path)
    shutil.copyfile(first_asset_path, second_asset_path)
    asset_meta = {
        first_asset: {
            "href": first_asset,
            "roles": "data",
        },
        # use same file twice to simulate the same CRS and bbox
        second_asset: {
            "href": second_asset,
            "roles": "data",
        },
    }

    cube_mock.write_assets.return_value = asset_meta
    evaluate.return_value = ImageCollectionResult(
        cube=cube_mock, format="GTiff", options={"multidate": True}
    )
    t = _get_tracker()
    t.setGlobalTracking(True)
    t.clearGlobalTracker()
    # tracker reset, so get it again
    t = _get_tracker()
    PU_COUNTER = "Sentinelhub_Processing_Units"
    t.registerDoubleCounter(PU_COUNTER)
    t.add(PU_COUNTER, 1.4)
    t.addInputProducts("collectionName", ["http://myproduct1", "http://myproduct2"])
    t.addInputProducts("collectionName", ["http://myproduct3"])
    t.addInputProducts("other_collectionName", ["http://myproduct4"])
    t.add(PU_COUNTER, 0.4)

    run_job(
        job_specification={
            "process_graph": {"nop": {"process_id": "discard_result", "result": True}}
        },
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "metadata.json",
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(tmp_path / "metadata.json")
    assert metadata_result == {
        "assets": {
            first_asset: {
                "href": first_asset,
                "roles": "data",
                # Projection extension metadata should not be here, but higher up.
            },
            second_asset: {
                "href": second_asset,
                "roles": "data",
                # Idem: projection extension metadata should not be here, but higher up.
            },
        },
        "bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
        "end_datetime": None,
        "epsg": 4326,
        "proj:shape": [720, 1188],
        "geometry": None,
        "area": None,
        "unique_process_ids": ["discard_result"],
        "instruments": [],
        "links": [
            {
                "href": "http://myproduct4",
                "rel": "derived_from",
                "title": "Derived from http://myproduct4",
            },
            {
                "href": "http://myproduct1",
                "rel": "derived_from",
                "title": "Derived from http://myproduct1",
            },
            {
                "href": "http://myproduct2",
                "rel": "derived_from",
                "title": "Derived from http://myproduct2",
            },
            {
                "href": "http://myproduct3",
                "rel": "derived_from",
                "title": "Derived from http://myproduct3",
            },
        ],
        "processing:facility": "VITO - SPARK",
        "processing:software": "openeo-geotrellis-" + __version__,
        "start_datetime": None,
        "usage": {
            "sentinelhub": {
                "unit": "sentinelhub_processing_unit",
                "value": 1.7999999999999998,
            }
        },
    }
    t.setGlobalTracking(False)


def reproject_raster_file(
    source_path: str, destination_path: str, dest_crs: str, width: int, height: int
):
    """Use the equavalent of the gdalwarp utility to reproject a raster file to a new CRS

    :param source_path: Path to the source raster file.
    :param destination_path: Path to the destination file.
    :param dest_crs: Which Coordinate Reference System to convert to.
    :param width:
        Width of the output raster in pixels.
        Reprojection may get a different size than the source raster had.
        In order to get the raster shape you need you have to give gdal.Warp the width and heigth.
    :param height:
        Height of the output raster in pixels.
        Same remark as for `width`: need to specify output raster's width and height.
    """
    opts = gdal.WarpOptions(dstSRS=dest_crs, width=width, height=height)
    gdal.Warp(
        destNameOrDestDS=destination_path, srcDSOrSrcDSTab=source_path, options=opts
    )


@mock.patch("openeo_driver.ProcessGraphDeserializer.evaluate")
def test_run_job_get_projection_extension_metadata_assets_with_different_epsg(
    evaluate, tmp_path
):
    """When there are two raster assets with the same projection metadata, it should put
    those metadata at the level of the item instead of the individual bands.
    """
    cube_mock = MagicMock()

    first_asset_path = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    first_asset = str(first_asset_path)

    # For the second file: use a copy of the first file  (in the temp dir) so we know
    # that GDAL will find exactly the same metadata under a different asset path.
    second_asset_path = tmp_path / first_asset_path.name
    (second_asset) = str(second_asset_path)
    reproject_raster_file(
        source_path=first_asset,
        destination_path=second_asset,
        dest_crs="EPSG:3812",
        width=720,
        height=1188,
    )

    asset_meta = {
        first_asset: {
            "href": first_asset,
            "roles": "data",
        },
        # use same file twice to simulate the same CRS and bbox
        second_asset: {
            "href": second_asset,
            "roles": "data",
        },
    }

    cube_mock.write_assets.return_value = asset_meta
    evaluate.return_value = ImageCollectionResult(
        cube=cube_mock, format="GTiff", options={"multidate": True}
    )
    t = _get_tracker()
    t.setGlobalTracking(True)
    t.clearGlobalTracker()
    # tracker reset, so get it again
    t = _get_tracker()
    PU_COUNTER = "Sentinelhub_Processing_Units"
    t.registerDoubleCounter(PU_COUNTER)
    t.add(PU_COUNTER, 1.4)
    t.addInputProducts("collectionName", ["http://myproduct1", "http://myproduct2"])
    t.addInputProducts("collectionName", ["http://myproduct3"])
    t.addInputProducts("other_collectionName", ["http://myproduct4"])
    t.add(PU_COUNTER, 0.4)

    run_job(
        job_specification={
            "process_graph": {"nop": {"process_id": "discard_result", "result": True}}
        },
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "metadata.json",
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(tmp_path / "metadata.json")
    assert metadata_result == {
        "assets": {
            first_asset: {
                "href": first_asset,
                "roles": "data",
                "proj:bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
                "proj:epsg": 4326,
                "proj:shape": [720, 1188],
            },
            second_asset: {
                "href": second_asset,
                "roles": "data",
                "proj:bbox": [723413.644, 577049.010, 745443.909, 614102.693],
                "proj:epsg": 3812,
                "proj:shape": [720, 1188],
            },
        },
        "bbox": None,
        "end_datetime": None,
        "epsg": None,
        "geometry": None,
        "area": None,
        "unique_process_ids": ["discard_result"],
        "instruments": [],
        "links": [
            {
                "href": "http://myproduct4",
                "rel": "derived_from",
                "title": "Derived from http://myproduct4",
            },
            {
                "href": "http://myproduct1",
                "rel": "derived_from",
                "title": "Derived from http://myproduct1",
            },
            {
                "href": "http://myproduct2",
                "rel": "derived_from",
                "title": "Derived from http://myproduct2",
            },
            {
                "href": "http://myproduct3",
                "rel": "derived_from",
                "title": "Derived from http://myproduct3",
            },
        ],
        "processing:facility": "VITO - SPARK",
        "processing:software": "openeo-geotrellis-" + __version__,
        "start_datetime": None,
        "usage": {
            "sentinelhub": {
                "unit": "sentinelhub_processing_unit",
                "value": 1.7999999999999998,
            }
        },
    }
    t.setGlobalTracking(False)


def test_extract_projection_extension_metadata():
    crs_wkt_string = textwrap.dedent(
        """
        PROJCRS["ETRS89 / Belgian Lambert 2008",
            BASEGEOGCRS["ETRS89",
                ENSEMBLE["European Terrestrial Reference System 1989 ensemble",
                    MEMBER["European Terrestrial Reference Frame 1989"],
                    MEMBER["European Terrestrial Reference Frame 1990"],
                    MEMBER["European Terrestrial Reference Frame 1991"],
                    MEMBER["European Terrestrial Reference Frame 1992"],
                    MEMBER["European Terrestrial Reference Frame 1993"],
                    MEMBER["European Terrestrial Reference Frame 1994"],
                    MEMBER["European Terrestrial Reference Frame 1996"],
                    MEMBER["European Terrestrial Reference Frame 1997"],
                    MEMBER["European Terrestrial Reference Frame 2000"],
                    MEMBER["European Terrestrial Reference Frame 2005"],
                    MEMBER["European Terrestrial Reference Frame 2014"],
                    ELLIPSOID["GRS 1980",6378137,298.257222101,
                        LENGTHUNIT["metre",1]],
                    ENSEMBLEACCURACY[0.1]],
                PRIMEM["Greenwich",0,
                    ANGLEUNIT["degree",0.0174532925199433]],
                ID["EPSG",4258]],
            CONVERSION["Belgian Lambert 2008",
                METHOD["Lambert Conic Conformal (2SP)",
                    ID["EPSG",9802]],
                PARAMETER["Latitude of false origin",50.797815,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8821]],
                PARAMETER["Longitude of false origin",4.35921583333333,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8822]],
                PARAMETER["Latitude of 1st standard parallel",49.8333333333333,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8823]],
                PARAMETER["Latitude of 2nd standard parallel",51.1666666666667,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8824]],
                PARAMETER["Easting at false origin",649328,
                    LENGTHUNIT["metre",1],
                    ID["EPSG",8826]],
                PARAMETER["Northing at false origin",665262,
                    LENGTHUNIT["metre",1],
                    ID["EPSG",8827]]],
            CS[Cartesian,2],
                AXIS["easting (X)",east,
                    ORDER[1],
                    LENGTHUNIT["metre",1]],
                AXIS["northing (Y)",north,
                    ORDER[2],
                    LENGTHUNIT["metre",1]],
            USAGE[
                SCOPE["Engineering survey, topographic mapping."],
                AREA["Belgium - onshore."],
                BBOX[49.5,2.5,51.51,6.4]],
            ID["EPSG",3812]]
        """
    )

    crs_id = extract_crs_epsg_code_from_wkt_string(crs_wkt_string)
    assert crs_id == 3812


@pytest.mark.parametrize(
    ["json_file", "expected_metadata"],
    [
        (
            "gdalinfo-output/c_gls_LC100-COV-GRASSLAND_201501010000_AFRI_PROBAV_1.0.1.nc.json",
            {
                "proj:bbox": [-30.000496, -34.999504, 59.999504, 45.000496],
                "proj:shape": [90720, 80640],
            },
        ),
        (
            "gdalinfo-output/z_cams_c_ecmf_20230308120000_prod_fc_sfc_021_aod550.nc.json",
            {"proj:bbox": [-0.2, -90.2, 359.8, 90.2], "proj:shape": [900, 451]},
        ),
    ],
)
def test_get_projection_extension_metadata(json_file, expected_metadata):
    json_path = get_test_data_file(json_file)

    with open(json_path, "rt") as f_in:
        gdal_info = json.load(f_in)

    proj_metadata = _get_projection_extension_metadata(gdal_info)

    assert proj_metadata == expected_metadata


@mock.patch("openeogeotrellis.deploy.batch_job.read_gdal_info")
def test_parse_projection_extension_metadata(mock_read_gdal_info):
    json_dir = get_test_data_file(
        "gdalinfo-output/SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23"
    )
    netcdf_path = json_dir / "SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23.nc"
    nc_json_path = (
        json_dir / "SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23.nc.json"
    )

    def read_json_file(netcdf_uri: str) -> dict:
        if netcdf_uri == str(netcdf_path):
            # json_path = netcdf_uri + ".json"
            json_path = nc_json_path
        else:
            parts = netcdf_uri.split(":")
            band = parts[-1]
            # strip off the surrounding double quotes from the filename
            filename = parts[1][1:-1]
            json_path = json_dir / f"{filename}.{band}.json"
        with open(json_path, "rt") as f:
            return json.load(f)

    mock_read_gdal_info.side_effect = read_json_file
    with open(nc_json_path, "rt") as f_in:
        gdal_info = json.load(f_in)

    proj_metadata = parse_projection_extension_metadata(gdal_info)

    expected_metadata = {
        "proj:epsg": 32631,
        "proj:bbox": [643120.0, 5675170.0, 646690.0, 5677500.0],
        "proj:shape": [357, 233],
    }
    assert proj_metadata == expected_metadata


@mock.patch("openeogeotrellis.deploy.batch_job.read_gdal_info")
def test_read_projection_extension_metadata(mock_read_gdal_info):
    json_dir = get_test_data_file(
        "gdalinfo-output/SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23"
    )
    netcdf_path = json_dir / "SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23.nc"

    def read_json_file(netcdf_uri: str) -> dict:
        if netcdf_uri == str(netcdf_path):
            json_path = netcdf_uri + ".json"
        else:
            parts = netcdf_uri.split(":")
            band = parts[-1]
            # strip off the surrounding double quotes from the filename
            filename = parts[1][1:-1]
            json_path = json_dir / f"{filename}.{band}.json"
        with open(json_path, "rt") as f:
            return json.load(f)

    mock_read_gdal_info.side_effect = read_json_file

    proj_metadata = read_projection_extension_metadata(str(netcdf_path))

    expected_metadata = {
        "proj:epsg": 32631,
        "proj:bbox": [643120.0, 5675170.0, 646690.0, 5677500.0],
        "proj:shape": [357, 233],
    }
    assert proj_metadata == expected_metadata


def get_job_metadata_without_s3(job_dir: Path) -> dict:
    """Helper function to create test data."""

    return {
        'assets': {
            'openEO_2017-11-21Z.tif': {
                'output_dir': str(job_dir),
                'bands': [{'common_name': None,
                            'name': 'ndvi',
                            'wavelength_um': None}],
                'href': f'{job_dir / "openEO_2017-11-21Z.tif"}',
                'nodata': 255,
                'roles': ['data'],
                'type': 'image/tiff; '
                        'application=geotiff'},
            'a-second-asset-file.tif': {
                'output_dir': str(job_dir),
                'bands': [{'common_name': None,
                            'name': 'ndvi',
                            'wavelength_um': None}],
                'href': f'{job_dir / "openEO_2017-11-21Z.tif"}',
                'nodata': 255,
                'roles': ['data'],
                'type': 'image/tiff; '
                        'application=geotiff'}
            },
        'bbox': [2, 51, 3, 52],
        'end_datetime': '2017-11-21T00:00:00Z',
        'epsg': 4326,
        'geometry': {'coordinates': [[[2.0, 51.0],
                                    [2.0, 52.0],
                                    [3.0, 52.0],
                                    [3.0, 51.0],
                                    [2.0, 51.0]]],
                    'type': 'Polygon'},
        'instruments': [],
        'links': [],
        'processing:facility': 'VITO - SPARK',
        'processing:software': 'openeo-geotrellis-0.3.3a1',
        'start_datetime': '2017-11-21T00:00:00Z'
    }


def test_convert_asset_outputs_to_s3_urls():
    """Test that it converts a metadata dict, translating each output_dir to a URL with the s3:// scheme."""

    metadata = get_job_metadata_without_s3(Path("/data/projects/OpenEO/6d11e901-bb5d-4589-b600-8dfb50524740/"))
    _convert_asset_outputs_to_s3_urls(metadata)

    assert metadata['assets']['openEO_2017-11-21Z.tif']["href"].startswith("s3://")
    assert metadata['assets']['a-second-asset-file.tif']["href"].startswith("s3://")


def test_convert_job_metadatafile_outputs_to_s3_urls(tmp_path):
    """Test that it processes the file on disk, converting each output_dir to a URL with the s3:// scheme."""

    job_id = "6d11e901-bb5d-4589-b600-8dfb50524740"
    job_dir = (tmp_path / job_id)
    metadata_path = job_dir / "whatever.json"
    job_dir.mkdir(parents=True)
    metadata = get_job_metadata_without_s3(job_dir)

    with open(metadata_path, "wt") as md_file:
        json.dump(metadata, md_file)

    _convert_job_metadatafile_outputs_to_s3_urls(metadata_path)

    with open(metadata_path, "rt") as md_file:
        converted_metadata = json.load(md_file)

    assert converted_metadata['assets']['openEO_2017-11-21Z.tif']["href"].startswith("s3://")
    assert converted_metadata['assets']['a-second-asset-file.tif']["href"].startswith("s3://")
