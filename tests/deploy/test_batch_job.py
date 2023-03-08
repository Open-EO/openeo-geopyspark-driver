import json
from mock import MagicMock
from pathlib import Path
from unittest import mock

import pytest
from pytest import approx
from openeo_driver.save_result import ImageCollectionResult
from shapely.geometry import mapping, shape, Polygon

from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.utils import read_json
from openeo_driver.util.geometry import reproject_geometry
from openeogeotrellis.deploy.batch_job import (
    extract_result_metadata,
    run_job, _get_tracker,
    _convert_asset_outputs_to_s3_urls,
    _convert_job_metadatafile_outputs_to_s3_urls,
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
        "bbox": (4, 51, 5, 52),
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
    expected_bbox = (
        approx(4, 0.01),
        approx(51, 0.01),
        approx(5, 0.01),
        approx(52, 0.01),
    )
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
    geometries_lat_lon = Polygon(
        (
            (aggrgeo_west_latlon, aggrgeo_south_latlon),
            (aggrgeo_west_latlon, aggrgeo_north_latlon),
            (aggrgeo_east_latlon, aggrgeo_north_latlon),
            (aggrgeo_east_latlon, aggrgeo_south_latlon),
            (aggrgeo_west_latlon, aggrgeo_south_latlon),
        )
    )
    cube = cube.aggregate_spatial(geometries=geometries_lat_lon, reducer="mean")

    metadata = extract_result_metadata(tracer)
    # Allow a 1% margin in the approximate comparison of the BBox
    expected_bbox = (
        approx(aggrgeo_west_latlon, 0.01),
        approx(aggrgeo_south_latlon, 0.01),
        approx(aggrgeo_east_latlon, 0.01),
        approx(aggrgeo_north_latlon, 0.01),
    )
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
    geometries_lat_lon = Polygon(
        (
            (aggrgeo_west_latlon, aggrgeo_south_latlon),
            (aggrgeo_west_latlon, aggrgeo_north_latlon),
            (aggrgeo_east_latlon, aggrgeo_north_latlon),
            (aggrgeo_east_latlon, aggrgeo_south_latlon),
            (aggrgeo_west_latlon, aggrgeo_south_latlon),
        )
    )
    # Geojson should always be lat-long, so we don't reproject the the delayed vector file.
    delayed_vector_file = tmp_path / "delayed_vector_polygon.geojson"
    geo_json_data = mapping(geometries_lat_lon)
    with open(delayed_vector_file, "wt") as f:
        f.writelines(json.dumps(geo_json_data))

    geometries = DelayedVector(str(delayed_vector_file))
    cube = cube.aggregate_spatial(geometries=geometries, reducer="mean")

    metadata = extract_result_metadata(tracer)
    # Allow a 1% margin in the approximate comparison of the BBox
    expected_bbox = (
        approx(aggrgeo_west_latlon, 0.01),
        approx(aggrgeo_south_latlon, 0.01),
        approx(aggrgeo_east_latlon, 0.01),
        approx(aggrgeo_north_latlon, 0.01),
    )
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
