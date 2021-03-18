from shapely.geometry import shape

from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.utils import read_json
from openeogeotrellis.deploy.batch_job import extract_result_metadata
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
        "start_datetime": "2020-02-02T00:00:00Z",
        "end_datetime": "2020-03-03T00:00:00Z",
        "links": []
    }
    assert metadata == expected
