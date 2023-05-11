import json
import shutil
import tempfile
from mock import MagicMock
from pathlib import Path
from unittest import mock

import pytest
from pytest import approx
from openeo_driver.save_result import ImageCollectionResult
from shapely.geometry import box, mapping, shape
from osgeo import gdal

from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.testing import DictSubSet
from openeo_driver.utils import read_json
from openeogeotrellis.deploy.batch_job import (
    extract_result_metadata,
    run_job, _convert_asset_outputs_to_s3_urls,
    _convert_job_metadatafile_outputs_to_s3_urls,
    _get_projection_extension_metadata,
    read_gdal_raster_metadata,
    parse_gdal_raster_metadata,
    AssetRasterMetadata,
    BandStatistics,
)
from openeogeotrellis.utils import get_jvm, to_s3_url
from openeogeotrellis.deploy.batch_job import _get_tracker
from openeogeotrellis._version import __version__

EXPECTED_GRAPH = [{"expression": {"nop": {"process_id": "discard_result",
                                               "result": True}},
                        "format": "openeo"}]

EXPECTED_PROVIDERS = [[{'description': 'This data was processed on an openEO backend ' \
                               'maintained by VITO.',
                'name': 'VITO',
                'processing:facility': 'openEO Geotrellis backend',
                'processing:software': {'Geotrellis backend': __version__},
                'roles': ['processor']}]]
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
        "bbox": [5.0, 5.0, 45.0, 40.0],
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
        "bbox": [5.0, 5.0, 45.0, 40.0],
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


@pytest.mark.parametrize(
    ["crs_epsg", "west", "south", "east", "north"],
    [
        # BBoxes below correspond to lat-long from 4E 51N to 5E 52N.
        # Belgian Lambert 2008
        (3812, 624112.728540544, 687814.368911342, 693347.444114525, 799212.044310798),
        # ETRS89 / UTM zone 31N (N-E)
        (3043, 570168.861511006, 5650300.78652147, 637294.365895774, 5762926.81279022),
        # Netherlands, Amersfoort / RD New
        (28992, 57624.6287650174, 335406.866285557, 128410.08537081, 445806.50883315),
    ],
)
def test_extract_result_metadata_reprojects_bbox_when_bbox_crs_not_epsg4326(
    crs_epsg, west, south, east, north
):
    """When the raster has a different CRS than EPSG:4326 (WGS), then extract_result_metadata
    should convert the bounding box to WGS.

    We give the test a few data cubes in a projected CRS with a bounding box that should
    correspond to lat long coordinates from 4E 51N to 5E 52N, give or take a small
    margin, which is needed for floating point rounding errors and some small
    differences you can get with CRS conversions.
    """
    tracer = DryRunDataTracer()
    cube = tracer.load_collection(
        collection_id="Sentinel2",
        arguments={
            "temporal_extent": ["2020-02-02", "2020-03-03"],
        },
    )
    cube = cube.filter_bbox(
        west=west, south=south, east=east, north=north, crs=crs_epsg
    )
    cube.resample_spatial(resolution=0, projection=crs_epsg)

    metadata = extract_result_metadata(tracer)

    # Allow a 1% margin in the approximate comparison of the BBox
    expected_bbox = [
        approx(4, 0.01),
        approx(51, 0.01),
        approx(5, 0.01),
        approx(52, 0.01),
    ]
    assert metadata["bbox"] == expected_bbox

    assert metadata["area"] == {
        "value": approx(6763173869883.0, 1.0),
        "unit": "square meter",
    }


@pytest.mark.parametrize(
    ["crs_epsg", "west", "south", "east", "north"],
    [
        # BBoxes below correspond to lat-long from 4E 51N to 5E 52N.
        # Belgian Lambert 2008
        (3812, 624112.728540544, 687814.368911342, 693347.444114525, 799212.044310798),
        # ETRS89 / UTM zone 31N (N-E)
        (3043, 570168.861511006, 5650300.78652147, 637294.365895774, 5762926.81279022),
        # Netherlands, Amersfoort / RD New
        (28992, 57624.6287650174, 335406.866285557, 128410.08537081, 445806.50883315),
    ],
)
def test_extract_result_metadata_aggregate_spatial_when_bbox_crs_not_epsg4326(
    crs_epsg, west, south, east, north
):
    """When the raster has a different CRS than EPSG:4326 (WGS), and also
    a spatial aggregation is being done, then extract_result_metadata
    should return the bounding box of the spatial aggregation expressed in EPSG:4326.
    """
    tracer = DryRunDataTracer()
    cube = tracer.load_collection(
        collection_id="Sentinel2",
        arguments={
            "temporal_extent": ["2020-02-02", "2020-03-03"],
        },
    )
    cube = cube.filter_bbox(
        west=west, south=south, east=east, north=north, crs=crs_epsg
    )
    cube.resample_spatial(resolution=0, projection=crs_epsg)

    #
    # Create a BaseGeometry (in memory) for the spatial aggregation.
    #
    # We only accept EPSG:4326 for an aggregate_spatial with a BaseGeometry,
    # i.e. a geometry from shapely. Therefore we don't convert this geometry
    # to crs_epsg.
    #
    # To keep the bbox numbers sensible we create a rectangular polygon that is
    # slightly smaller than the BBox in filter_bbox above, so that they will stay
    # within the valid bounds of the (projected) CRS of the cube.
    # That way, we should not get large and/or inaccurate coordinates if any
    # CRS conversion has happened along the way.
    #
    # This smaller rectangle is also the BBox that we expect in the metadata.
    aggrgeo_west_latlon = 4.2
    aggrgeo_east_latlon = 4.8
    aggrgeo_south_latlon = 51.2
    aggrgeo_north_latlon = 51.8
    geometries_lat_lon = box(
        aggrgeo_west_latlon,
        aggrgeo_south_latlon,
        aggrgeo_east_latlon,
        aggrgeo_north_latlon,
    )
    cube = cube.aggregate_spatial(geometries=geometries_lat_lon, reducer="mean")

    metadata = extract_result_metadata(tracer)
    # Allow a 1% margin in the approximate comparison of the BBox
    expected_bbox = [
        approx(aggrgeo_west_latlon, 0.01),
        approx(aggrgeo_south_latlon, 0.01),
        approx(aggrgeo_east_latlon, 0.01),
        approx(aggrgeo_north_latlon, 0.01),
    ]
    assert metadata["bbox"] == expected_bbox
    assert metadata["area"] == {
        "value": approx(6763173869883.0, 1.0),
        "unit": "square meter",
    }


@pytest.mark.parametrize(
    ["crs_epsg", "west", "south", "east", "north"],
    [
        # BBoxes below correspond to lat-long from 4E 51N to 5E 52N.
        # Belgian Lambert 2008
        (3812, 624112.728540544, 687814.368911342, 693347.444114525, 799212.044310798),
        # ETRS89 / UTM zone 31N (N-E)
        (3043, 570168.861511006, 5650300.78652147, 637294.365895774, 5762926.81279022),
        # Netherlands, Amersfoort / RD New
        (28992, 57624.6287650174, 335406.866285557, 128410.08537081, 445806.50883315),
    ],
)
def test_extract_result_metadata_aggregate_spatial_delayed_vector_when_bbox_crs_not_epsg4326(
    tmp_path, crs_epsg, west, south, east, north
):
    tracer = DryRunDataTracer()
    cube = tracer.load_collection(
        collection_id="Sentinel2",
        arguments={
            "temporal_extent": ["2020-02-02", "2020-03-03"],
        },
    )
    cube = cube.filter_bbox(
        west=west, south=south, east=east, north=north, crs=crs_epsg
    )
    cube.resample_spatial(resolution=0, projection=crs_epsg)

    #
    # Create a geojson file for the spatial aggregation with a delayed vector file.
    # GeoJSON is always in lat-long, so we don't reproject this geometry.
    #
    # To keep the bbox numbers sensible we create a rectangular polygon that is
    # slightly smaller than the BBox in filter_bbox above, so that they will stay
    # within the valid bounds of the (projected) CRS of the cube.
    # That way, we should not get large and/or inaccurate coordinates if any
    # CRS conversion has happened along the way.
    #
    # This smaller rectangle is also the BBox that we expect in the metadata.
    aggrgeo_west_latlon = 4.2
    aggrgeo_east_latlon = 4.8
    aggrgeo_south_latlon = 51.2
    aggrgeo_north_latlon = 51.8
    geometries_lat_lon = box(
        aggrgeo_west_latlon,
        aggrgeo_south_latlon,
        aggrgeo_east_latlon,
        aggrgeo_north_latlon,
    )
    # Geojson should always be lat-long, so we don't reproject the the delayed vector file.
    delayed_vector_file = tmp_path / "delayed_vector_polygon.geojson"
    geo_json_data = mapping(geometries_lat_lon)
    delayed_vector_file.write_text(json.dumps(geo_json_data))

    geometries = DelayedVector(str(delayed_vector_file))
    cube = cube.aggregate_spatial(geometries=geometries, reducer="mean")

    metadata = extract_result_metadata(tracer)
    # Allow a 1% margin in the approximate comparison of the BBox
    expected_bbox = [
        approx(aggrgeo_west_latlon, 0.01),
        approx(aggrgeo_south_latlon, 0.01),
        approx(aggrgeo_east_latlon, 0.01),
        approx(aggrgeo_north_latlon, 0.01),
    ]
    assert metadata["bbox"] == expected_bbox
    assert metadata["area"] == {
        "value": approx(6763173869883.0, 1.0),
        "unit": "square meter",
    }


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
    ProductIdAndUrl = get_jvm().org.openeo.geotrelliscommon.BatchJobMetadataTracker.ProductIdAndUrl
    t.addInputProductsWithUrls("other_collectionName", [ProductIdAndUrl("p4", "http://myproduct4")])
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
                               'links': [{'href': 'http://myproduct4', 'rel': 'derived_from', 'title': 'Derived from p4'},
                                         {'href': 'http://myproduct1', 'rel': 'derived_from', 'title': 'Derived from http://myproduct1'},
                                         {'href': 'http://myproduct2', 'rel': 'derived_from', 'title': 'Derived from http://myproduct2'},
                                         {'href': 'http://myproduct3', 'rel': 'derived_from', 'title': 'Derived from http://myproduct3'}],
                               'processing:expression': [{'expression': {'nop': {'process_id': 'discard_result',
                                                                                 'result': True}},
                                                          'format': 'openeo'}],
                               'processing:facility': 'VITO - SPARK',
                               'processing:software': 'openeo-geotrellis-' + __version__,
                               'start_datetime': None,
                               'providers': EXPECTED_PROVIDERS,
                               'usage': {'sentinelhub': {'unit': 'sentinelhub_processing_unit',
                                                         'value': 1.7999999999999998}}
                               }
    t.setGlobalTracking(False)


@mock.patch("openeo_driver.ProcessGraphDeserializer.evaluate")
def test_run_job_get_projection_extension_metadata(evaluate, tmp_path):
    cube_mock = MagicMock()

    job_dir = tmp_path / "job-test-proj-metadata"
    job_dir.mkdir()
    output_file = job_dir / "out"
    metadata_file = job_dir / "metadata.json"

    first_asset_source = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    first_asset_name = first_asset_source.name
    first_asset_dest = job_dir / first_asset_name
    shutil.copy(first_asset_source, first_asset_dest)

    asset_meta = {
        first_asset_name: {
            "href": first_asset_name,  # A path relative to the job dir must work.
            "roles": "data",
        },
        # The second file does not exist on the filesystem.
        # This triggers that the projection extension metadata is put on the
        # bands, for the remaining assets (Here there is only 1 other asset off course).
        "openEO01-05.tif": {"href": "openEO01-05.tif", "roles": "data"},
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
        output_file=output_file,
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=job_dir,
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(metadata_file)
    assert metadata_result == {
        "assets": {
            first_asset_name: {
                "href": first_asset_name,
                "roles": "data",
                "proj:bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
                "proj:epsg": 4326,
                "proj:shape": [720, 1188],
                "raster:bands": [
                    {
                        "name": "1",
                        "statistics": {
                            "maximum": approx(641.22131347656),
                            "mean": approx(403.31786404988),
                            "minimum": approx(149.76655578613),
                            "stddev": approx(98.389307981699),
                            "valid_percent": 100.0,
                        },
                    }
                ],
            },
            "openEO01-05.tif": {"href": "openEO01-05.tif", "roles": "data"},
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
        "processing:expression": EXPECTED_GRAPH,
        "providers": EXPECTED_PROVIDERS,
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

    job_dir = tmp_path / "job-test-proj-metadata-same_epsg_and_bbox"
    job_dir.mkdir()
    output_file = job_dir / "out"
    metadata_file = job_dir / "metadata.json"

    first_asset_source = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    first_asset_name = first_asset_source.name
    first_asset_dest = job_dir / first_asset_name
    shutil.copy(first_asset_source, first_asset_dest)

    # For the second file: use a copy of the first file so we know that GDAL
    # will find exactly the same metadata under a different asset path.
    second_asset_path = job_dir / f"second_{first_asset_name}"
    second_asset_name = second_asset_path.name
    shutil.copy(first_asset_source, second_asset_path)

    asset_meta = {
        first_asset_name: {
            "href": first_asset_name,
            "roles": "data",
        },
        # Use same file twice to simulate the same CRS and bbox.
        second_asset_name: {
            "href": second_asset_name,
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
        output_file=output_file,
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(metadata_file)
    assert metadata_result == {
        "assets": {
            first_asset_name: {
                "href": first_asset_name,
                "roles": "data",
                # Projection extension metadata should not be here, but higher up.
                "raster:bands": [
                    {
                        "name": "1",
                        "statistics": {
                            "maximum": approx(641.22131347656),
                            "minimum": approx(149.76655578613),
                            "mean": approx(403.31786404988),
                            "stddev": approx(98.389307981699),
                            "valid_percent": 100.0,
                        },
                    }
                ],
            },
            second_asset_name: {
                "href": second_asset_name,
                "roles": "data",
                # Idem: projection extension metadata should not be here, but higher up.
                "raster:bands": [
                    {
                        "name": "1",
                        "statistics": {
                            "maximum": approx(641.22131347656),
                            "minimum": approx(149.76655578613),
                            "mean": approx(403.31786404988),
                            "stddev": approx(98.389307981699),
                            "valid_percent": 100.0,
                        },
                    }
                ],
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
        "processing:expression": EXPECTED_GRAPH,
        "providers": EXPECTED_PROVIDERS,
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

    job_dir = tmp_path / "job-test-proj-metadata-different_epsg"
    job_dir.mkdir()
    output_file = job_dir / "out"
    metadata_file = job_dir / "metadata.json"

    first_asset_source = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    first_asset_name = first_asset_source.name
    first_asset_dest = job_dir / first_asset_name
    shutil.copy(first_asset_source, first_asset_dest)

    # For the second file: use a copy of the first file so we know that GDAL
    # will find exactly the same metadata under a different asset path.
    second_asset_path = job_dir / f"second_{first_asset_name}"
    second_asset_name = second_asset_path.name
    reproject_raster_file(
        source_path=str(first_asset_source),
        destination_path=str(second_asset_path),
        dest_crs="EPSG:3812",
        width=720,
        height=1188,
    )

    asset_meta = {
        first_asset_name: {
            "href": first_asset_name,
            "roles": "data",
        },
        # use same file twice to simulate the same CRS and bbox
        second_asset_name: {
            "href": second_asset_name,
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
        output_file=output_file,
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()
    metadata_result = read_json(metadata_file)
    assert metadata_result == {
        "assets": {
            first_asset_name: {
                "href": first_asset_name,
                "roles": "data",
                "proj:bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
                "proj:epsg": 4326,
                "proj:shape": [720, 1188],
                "raster:bands": [
                    {
                        "name": "1",
                        "statistics": {
                            "maximum": approx(641.22131347656),
                            "mean": approx(403.31786404988),
                            "minimum": approx(149.76655578613),
                            "stddev": approx(98.389307981699),
                            "valid_percent": 100.0,
                        },
                    }
                ],
            },
            second_asset_name: {
                "href": second_asset_name,
                "roles": "data",
                "proj:bbox": [723413.644, 577049.010, 745443.909, 614102.693],
                "proj:epsg": 3812,
                "proj:shape": [720, 1188],
                "raster:bands": [
                    {
                        "name": "1",
                        "statistics": {
                            "maximum": approx(641.22131347656),
                            "minimum": approx(0.0),
                            "mean": approx(388.80473522222),
                            "stddev": approx(122.48474568907),
                            "valid_percent": 100.0,
                        },
                    }
                ],
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
        "processing:expression": EXPECTED_GRAPH,
        "providers": EXPECTED_PROVIDERS,
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
def test_run_job_get_projection_extension_metadata_job_dir_is_relative_path(evaluate):
    cube_mock = MagicMock()
    # job dir should be a relative path,
    # We still want the test data to be cleaned up though, so we need to use
    # tempfile instead of pytest's tmp_path.
    with tempfile.TemporaryDirectory(
        dir=".", suffix="job-test-proj-metadata"
    ) as job_dir:
        job_dir = Path(job_dir)
        assert not job_dir.is_absolute()

        # Note that batch_job's main() interprets its output_file and metadata_file
        # arguments as relative to job_dir, but run_job does not!
        output_file = job_dir / "out"
        metadata_file = job_dir / "metadata.json"

        first_asset_source = get_test_data_file(
            "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
        )
        first_asset_name = first_asset_source.name
        first_asset_dest = job_dir / first_asset_name
        shutil.copy(first_asset_source, first_asset_dest)

        asset_meta = {
            first_asset_name: {
                "href": first_asset_name,  # A path relative to the job dir must work.
                "roles": "data",
            },
            # The second file does not exist on the filesystem.
            # This triggers that the projection extension metadata is put on the
            # bands, for the remaining assets (Here there is only 1 other asset off course).
            "openEO01-05.tif": {"href": "openEO01-05.tif", "roles": "data"},
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
                "process_graph": {
                    "nop": {"process_id": "discard_result", "result": True}
                }
            },
            output_file=output_file,
            metadata_file=metadata_file,
            api_version="1.0.0",
            job_dir=job_dir,
            dependencies={},
            user_id="jenkins",
        )

        cube_mock.write_assets.assert_called_once()
        metadata_result = read_json(metadata_file)
        assert metadata_result == {
            "assets": {
                first_asset_name: {
                    "href": first_asset_name,
                    "roles": "data",
                    "proj:bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
                    "proj:epsg": 4326,
                    "proj:shape": [720, 1188],
                    "raster:bands": [
                        {
                            "name": "1",
                            "statistics": {
                                "maximum": approx(641.22131347656),
                                "mean": approx(403.31786404988),
                                "minimum": approx(149.76655578613),
                                "stddev": approx(98.389307981699),
                                "valid_percent": 100.0,
                            },
                        }
                    ],
                },
                "openEO01-05.tif": {"href": "openEO01-05.tif", "roles": "data"},
            },
            "bbox": None,
            "epsg": None,
            "end_datetime": None,
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
            "processing:expression": EXPECTED_GRAPH,
            "providers": EXPECTED_PROVIDERS,
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


@mock.patch(
    "openeogeotrellis.configparams.ConfigParams.use_object_storage",
    new_callable=mock.PropertyMock,
)
@mock.patch("openeo_driver.ProcessGraphDeserializer.evaluate")
def test_run_job_get_projection_extension_metadata_assets_in_s3(
    evaluate, mock_config_use_object_storage, tmp_path, mock_s3_bucket
):
    mock_config_use_object_storage.return_value = True
    cube_mock = MagicMock()

    job_id = "j-123546"
    job_dir = tmp_path / "job-test-proj-metadata"
    job_dir.mkdir()
    output_file = job_dir / "out"
    metadata_file = job_dir / "metadata.json"

    single_asset_source = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    single_asset_name = single_asset_source.name
    asset_s3_key = f"{job_id}/{single_asset_name}"
    single_asset_href = to_s3_url(asset_s3_key)
    mock_s3_bucket.put_object(Key=asset_s3_key, Body=single_asset_source.read_bytes())

    asset_meta = {
        single_asset_name: {
            "href": single_asset_href,
            "roles": "data",
        }
    }

    cube_mock.write_assets.return_value = asset_meta
    evaluate.return_value = ImageCollectionResult(cube=cube_mock, format="GTiff", options={"multidate": True})

    # The asset file should not be available in the local job_dir before
    # starting the job. It will be downloaded when gdalinfo needs it.
    first_asset_dest = job_dir / single_asset_name
    assert not first_asset_dest.exists()

    run_job(
        job_specification={"process_graph": {"nop": {"process_id": "discard_result", "result": True}}},
        output_file=output_file,
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()

    # After run_job the asset should have been downloaded to be accessible to gdalinfo.
    assert first_asset_dest.exists()

    metadata_result = read_json(metadata_file)
    assert metadata_result == DictSubSet(
        {
            "assets": {
                single_asset_name: {
                    "href": single_asset_href,
                    "roles": "data",
                    # Projection extension metadata should not be here, but higher up.
                },
            },
            "bbox": [5.3997917, 50.0001389, 5.6997917, 50.3301389],
            "epsg": 4326,
            "proj:shape": [720, 1188],
        }
    )


@mock.patch(
    "openeogeotrellis.configparams.ConfigParams.use_object_storage",
    new_callable=mock.PropertyMock,
)
@mock.patch("openeo_driver.ProcessGraphDeserializer.evaluate")
def test_run_job_get_projection_extension_metadata_assets_in_s3_multiple_assets(
    evaluate, mock_config_use_object_storage, tmp_path, mock_s3_bucket
):
    mock_config_use_object_storage.return_value = True
    cube_mock = MagicMock()

    job_id = "j-123546"
    job_dir = tmp_path / "job-test-proj-metadata"
    job_dir.mkdir()
    output_file = job_dir / "out"
    metadata_file = job_dir / "metadata.json"

    first_asset_source = get_test_data_file(
        "binary/s1backscatter_orfeo/copernicus-dem-30m/Copernicus_DSM_COG_10_N50_00_E005_00_DEM/Copernicus_DSM_COG_10_N50_00_E005_00_DEM.tif"
    )
    first_asset_name = first_asset_source.name

    # For the second file: use a copy of the first file so we know that GDAL
    # will find exactly the same metadata under a different asset path.
    second_asset_path: Path = tmp_path / f"second_{first_asset_name}"
    second_asset_name = second_asset_path.name
    reproject_raster_file(
        source_path=str(first_asset_source),
        destination_path=str(second_asset_path),
        dest_crs="EPSG:3812",
        width=720,
        height=1188,
    )

    first_asset_s3_key = f"{job_id}/{first_asset_name}"
    second_asset_s3_key = f"{job_id}/{second_asset_name}"
    first_asset_href = to_s3_url(first_asset_s3_key)
    second_asset_href = to_s3_url(second_asset_s3_key)
    mock_s3_bucket.put_object(Key=first_asset_s3_key, Body=first_asset_source.read_bytes())
    mock_s3_bucket.put_object(Key=second_asset_s3_key, Body=second_asset_path.read_bytes())

    asset_meta = {
        first_asset_name: {
            "href": first_asset_href,
            "roles": "data",
        },
        second_asset_name: {
            "href": second_asset_href,
            "roles": "data",
        },
    }

    cube_mock.write_assets.return_value = asset_meta
    evaluate.return_value = ImageCollectionResult(cube=cube_mock, format="GTiff", options={"multidate": True})

    # The asset files should not be available in the local job_dir before
    # starting the job. They will be downloaded when gdalinfo needs them.
    assert len(list(job_dir.iterdir())) == 0

    run_job(
        job_specification={"process_graph": {"nop": {"process_id": "discard_result", "result": True}}},
        output_file=output_file,
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir="./",
        dependencies={},
        user_id="jenkins",
    )

    cube_mock.write_assets.assert_called_once()

    # After run_job the assets should have been downloaded to be accessible to gdalinfo
    first_asset_dest = job_dir / first_asset_name
    second_asset_dest = job_dir / second_asset_name
    assert first_asset_dest.exists()
    assert second_asset_dest.exists()

    metadata_result = read_json(metadata_file)
    assert metadata_result == DictSubSet(
        {
            "assets": {
                first_asset_name: DictSubSet(
                    {
                        "href": first_asset_href,
                        "roles": "data",
                        "proj:bbox": pytest.approx([5.3997917, 50.0001389, 5.6997917, 50.3301389]),
                        "proj:epsg": 4326,
                        "proj:shape": [720, 1188],
                    }
                ),
                second_asset_name: DictSubSet(
                    {
                        "href": second_asset_href,
                        "roles": "data",
                        "proj:bbox": pytest.approx([723413.644, 577049.010, 745443.909, 614102.693]),
                        "proj:epsg": 3812,
                        "proj:shape": [720, 1188],
                    }
                ),
            },
            "bbox": None,
            "epsg": None,
        }
    )


# TODO: Update this test to include statistics or not? Would need to update the json file.
@pytest.mark.parametrize(
    ["json_file", "expected_metadata"],
    [
        (
            "gdalinfo-output/c_gls_LC100-COV-GRASSLAND_201501010000_AFRI_PROBAV_1.0.1.nc.json",
            {
                "proj:epsg": 4326,
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
def test_parse_gdal_raster_metadata(mock_read_gdal_info):
    json_dir = get_test_data_file("gdalinfo-output/SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23")
    netcdf_path = json_dir / "SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23.nc"
    nc_json_path = json_dir / "SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23.nc.json"

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

    raster_metadata: AssetRasterMetadata = parse_gdal_raster_metadata(gdal_info)

    expected_metadata = {
        "proj:epsg": 32631,
        "proj:bbox": [643120.0, 5675170.0, 646690.0, 5677500.0],
        "proj:shape": [357, 233],
    }
    assert raster_metadata.projection == expected_metadata


def test_read_gdal_raster_metadata_from_multiband_netcdf_file():
    netcdf_path = get_test_data_file(
        "binary/stac_proj_extension/netcdf/SENTINEL2_L1C_SENTINELHUB_E5_05_N51_21-E5_10_N51_23.nc"
    )

    raster_metadata = read_gdal_raster_metadata(str(netcdf_path))

    assert raster_metadata.to_dict() == {
        "proj:epsg": 32631,
        "proj:bbox": [643120.0, 5675170.0, 646690.0, 5677500.0],
        "proj:shape": [357, 233],
        "raster:bands": [
            {
                "name": "B01",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(1651.0),
                        "maximum": approx(4203.0),
                        "mean": approx(2260.1994),
                        "stddev": approx(535.1263),
                    }
                ),
            },
            {
                "name": "B02",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(1230.0),
                        "maximum": approx(4262.0),
                        "mean": approx(1877.3180),
                        "stddev": approx(531.5487),
                    }
                ),
            },
            {
                "name": "B03",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(848),
                        "maximum": approx(3683),
                        "mean": approx(1479.7089),
                        "stddev": approx(506.3976),
                    }
                ),
            },
            {
                "name": "B04",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(715),
                        "maximum": approx(4007),
                        "mean": approx(1451.2542),
                        "stddev": approx(593.3228),
                    }
                ),
            },
            {
                "name": "B05",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(746),
                        "maximum": approx(4243),
                        "mean": approx(1596.4995),
                        "stddev": approx(643.2813),
                    }
                ),
            },
            {
                "name": "B06",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(738),
                        "maximum": approx(4678),
                        "mean": approx(1822.6124),
                        "stddev": approx(700.7382),
                    }
                ),
            },
            {
                "name": "B07",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(753),
                        "maximum": approx(4931),
                        "mean": approx(1926.1757),
                        "stddev": approx(729.2182),
                    }
                ),
            },
            {
                "name": "B08",
                "statistics": DictSubSet(
                    {
                        "minimum": approx(670),
                        "maximum": approx(4834),
                        "mean": approx(1875.9574),
                        "stddev": approx(706.7410),
                    }
                ),
            },
        ],
    }


def test_read_gdal_raster_metadata_from_singleband_netcdf_file():
    netcdf_path = get_test_data_file(
        "binary/stac_proj_extension/netcdf/z_cams_c_ecmf_20230308120000_prod_fc_sfc_021_aod550.nc"
    )

    raster_metadata = read_gdal_raster_metadata(str(netcdf_path))

    # Check it via the BandStatistics object before we check the dict that
    # contains the entire result.
    actual_band_stats: BandStatistics = raster_metadata.statistics["Total Aerosol Optical Depth at 550nm"]
    assert actual_band_stats.minimum == approx(0.008209, abs=0.000001)
    assert actual_band_stats.maximum == approx(3.0, abs=0.000001)
    assert actual_band_stats.mean == approx(0.124908, abs=0.000001)
    assert actual_band_stats.stddev == approx(0.12537779432458, abs=0.000001)
    # Turns out this result changes after the first run of the test, because
    # gdalinfo generates an *.aux.xml file that caches the statistics.
    # assert actual_band_stats.valid_percent is None

    assert raster_metadata.to_dict() == DictSubSet(
        {
            "proj:bbox": [-0.2, -90.2, 359.8, 90.2],
            "proj:shape": [900, 451],
            "raster:bands": [
                {
                    "name": "Total Aerosol Optical Depth at 550nm",
                    "statistics": DictSubSet(
                        {
                            "minimum": approx(0.008209, abs=0.000001),
                            "maximum": approx(3.0, abs=0.000001),
                            "mean": approx(0.1249084, abs=0.000001),
                            "stddev": approx(0.1253777, abs=0.000001),
                        }
                    ),
                }
            ],
        }
    )


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
