import json

from openeogeotrellis.deploy.batch_job import extract_result_metadata,run_job

def test_ep3874_filter_spatial(tmp_path):

    job_spec = {"process_graph":{
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-04", "2021-01-06"],
                "bands": ["Flat:2"]
            },
        },
        "filterspatial1": {
            "process_id": "filter_spatial",
            "arguments": {
                "data": {"from_node": "lc"},
                "geometries":  {
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
                    },
                        {
                            "type": "Feature",
                            "properties": {},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [
                                            0.72,
                                            -0.516
                                        ],
                                        [
                                            2.99,
                                            -1.29
                                        ],
                                        [
                                            2.279,
                                            1.724
                                        ],
                                        [
                                            0.725,
                                            -0.18
                                        ],
                                        [
                                            0.725,
                                            -0.516
                                        ]
                                    ]
                                ]
                            }
                        }
                    ]
                },
            }
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "filterspatial1"}, "format": "netCDF","options":{
                "sample_by_feature":True
            }},
            "result": True,
        }
    }}
    metadata_file = tmp_path / "metadata.json"
    run_job(job_spec, output_file=tmp_path /"out", metadata_file=metadata_file,
            api_version="1.0.0", job_dir="./", dependencies={}, user_id="jenkins")
    with metadata_file.open() as f:
        metadata = json.load(f)
    assert metadata["start_datetime"] == "2021-01-04T00:00:00Z"
    assert len(metadata["assets"]) == 2
