from openeo_driver.dry_run import DryRunDataTracer
from openeogeotrellis.deploy.batch_job import extract_result_metadata


def test_extract_result_metadata():
    tracer = DryRunDataTracer()
    tracer.load_collection("Sentinel2", {
        "temporal_extent": ["2020-02-02", "2020-03-03"],
    })
    tracer.process_traces(
        traces=list(tracer.get_trace_leaves()),
        operation="spatial_extent", arguments={"west": 4, "south": 51, "east": 5, "north": 52}
    )

    metadata = extract_result_metadata(tracer)
    expected = {
        "bbox": [4, 51, 5, 52],
        "geometry": {"type": "Polygon", "coordinates": (((4, 51), (4, 52), (5, 52), (5, 51), (4, 51)),)},
        "start_datetime": "2020-02-02T00:00:00Z",
        "end_datetime": "2020-03-03T00:00:00Z",
    }
    assert metadata == expected
