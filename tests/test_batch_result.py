import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Point
import xarray
from openeo.metadata import Band

from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER, evaluate
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.utils import EvalEnv
from openeogeotrellis.deploy.batch_job import run_job, extract_result_metadata


def test_png_export(tmp_path):

    job_spec = {
        "title": "my job",
        "description": "*minimum band*",
        "process_graph":{
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"]
            },
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "lc"}, "format": "PNG"},
            "result": True,
        }
    }}
    metadata_file = tmp_path / "metadata.json"
    run_job(job_spec, output_file= "/tmp/out.png" , metadata_file=metadata_file,
            api_version="1.0.0", job_dir="./", dependencies={}, user_id="jenkins")
    with metadata_file.open() as f:
        metadata = json.load(f)
    assert metadata["start_datetime"] == "2021-01-05T00:00:00Z"
    assets = metadata["assets"]
    assert len(assets) == 1
    assert assets["out.png"]
    for asset in assets:
        theAsset = assets[asset]

        assert 'image/png' == theAsset['type']
        href = theAsset['href']
        from osgeo.gdal import Info
        info = Info(href, format='json')
        print(info)
        assert info['driverShortName'] == 'PNG'


def test_simple_math(tmp_path):
    simple_compute = {
        'subtract1': {'process_id': 'subtract', 'arguments': {'x': 50, 'y': 32}},
           'divide1': {'process_id': 'divide', 'arguments': {'x': {'from_node': 'subtract1'}, 'y': 1.8},
                       'result': True}
    }
    job_spec = {
        "title": "my job",
        "description": "*minimum band*",
        "process_graph":simple_compute
    }
    metadata_file = tmp_path / "metadata.json"
    run_job(job_spec, output_file= Path("/tmp/out.json") , metadata_file=metadata_file,
            api_version="1.0.0", job_dir="./", dependencies={}, user_id="jenkins")
    with metadata_file.open() as f:
        metadata = json.load(f)

    assets = metadata["assets"]
    assert len(assets) == 1
    for asset in assets:
        theAsset = assets[asset]

        assert 'application/json' == theAsset['type']
        href = theAsset['href']
        assert href.endswith(".json")
        with open(href,'r') as f:
            theJSON = json.load(f)
            assert theJSON == 10.0



def test_ep3899_netcdf_no_bands(tmp_path):

    job_spec = {
        "title": "my job",
        "description": "*minimum band*",
        "process_graph":{
        "lc": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"]
            },
        },
        'reducedimension1': {
            'process_id': 'reduce_dimension',
            'arguments': {
                'data': {'from_node': 'lc'},
                'dimension': 'bands',
                'reducer': {'process_graph': {
                    'mean1': {
                        'process_id': 'min',
                        'arguments': {'data': {'from_parameter': 'data'}},
                        'result': True
                    }
                }}
            },
            'result': False
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "reducedimension1"}, "format": "netCDF"},
            "result": True,
        }
    }}
    metadata_file = tmp_path / "metadata.json"
    run_job(job_spec, output_file= "/tmp/out.nc" , metadata_file=metadata_file,
            api_version="1.0.0", job_dir="./", dependencies={}, user_id="jenkins")
    with metadata_file.open() as f:
        metadata = json.load(f)
    assert metadata["start_datetime"] == "2021-01-01T00:00:00Z"
    assets = metadata["assets"]
    assert len(assets) == 1
    for asset in assets:
        theAsset = assets[asset]

        assert 'application/x-netcdf' == theAsset['type']
        href = theAsset['href']
        from osgeo.gdal import Info
        info = Info("NETCDF:\""+href+"\":var", format='json')
        print(info)
        assert info['driverShortName'] == 'netCDF'
        da = xarray.open_dataset(href, engine='h5netcdf')
        print(da)


@pytest.mark.parametrize("prefix", [None, "prefixTest"])
def test_ep3874_filter_spatial(prefix, tmp_path):
    print("tmp_path: ", tmp_path)
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
                        "properties": {},
                        "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
                    },
                        {
                            "type": "Feature",
                            "properties": {},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [[[0.725, -0.516],[2.99,-1.29],[2.279,1.724],[0.725,-0.18],[0.725,-0.516]]]
                            }
                        }
                    ]
                },
            }
        },
        "save": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "filterspatial1"}, "format": "netCDF","options":{
                "filename_prefix": prefix,
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
    assets = metadata["assets"]
    assert len(assets) == 2
    if prefix:
        assert assets[prefix + "_0.nc"]
        assert assets[prefix + "_1.nc"]
    else:
        assert assets["openEO_0.nc"]
        assert assets["openEO_1.nc"]

    for asset in assets:
        theAsset = assets[asset]
        bands = [Band(**b) for b in theAsset["bands"]]
        assert len(bands) == 1
        da = xarray.open_dataset(theAsset['href'], engine='h5netcdf')
        assert 'Flat:2' in da
        print(da['Flat:2'])


def test_aggregate_spatial_area_result(tmp_path):
    pg = {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-04", "2021-01-06"],
                    "bands": ["Flat:2"]
                },
            },
            "aggregatespatial1": {
                "arguments": {
                    "data": {
                        "from_node": "loadcollection1"
                    },
                    "geometries": {
                        "crs": {
                            "properties": {
                                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                            },
                            "type": "name"
                        },
                        "features": [{
                            "geometry": {
                                "coordinates": [
                                    [[5.075427149289014, 51.19258173530002], [5.076317642681958, 51.19305912348515],
                                        [5.075430319510139, 51.19388497600461], [5.074314520559944, 51.193407596375614],
                                        [5.075427149289014, 51.19258173530002]]],
                                "type": "Polygon"
                            },
                            "properties": {
                                "Name": "Polygon",
                                "description": None,
                                "tessellate": 1
                            },
                            "type": "Feature"
                        }],
                        "name": "Daft Logic Google Maps Area Calculator Tool",
                        "type": "FeatureCollection"
                    },
                    "reducer": {
                        "process_graph": {
                            "mean1": {
                                "arguments": {
                                    "data": {
                                        "from_parameter": "data"
                                    }
                                },
                                "process_id": "mean",
                                "result": True
                            }
                        }
                    }
                },
                "process_id": "aggregate_spatial",
            },
            "saveresult1": {
              "arguments": {
                "data": {
                  "from_node": "aggregatespatial1"
                },
                "format": "JSON",
                "options": {}
              },
              "process_id": "save_result",
              "result": True
            }
        }
    }
    metadata_file = tmp_path / "metadata.json"
    run_job(pg, output_file=tmp_path / "out", metadata_file=metadata_file,
            api_version="1.0.0", job_dir="./", dependencies={}, user_id="jenkins")
    with metadata_file.open() as f:
        metadata = json.load(f)
    assert metadata["area"]["value"] == 10155.607958197594
    assert metadata["area"]["unit"] == "square meter"


def test_aggregate_spatial_area_result_delayed_vector(backend_implementation):
    dry_run_tracer = DryRunDataTracer()
    dry_run_env = EvalEnv({
        ENV_DRY_RUN_TRACER: dry_run_tracer,
        "backend_implementation": backend_implementation,
        "version": "1.0.0"
    })
    pg = {
        'loadcollection1': {
            'process_id': 'load_collection',
            'arguments': {
                'bands': ['B04'],
                'id': 'TERRASCOPE_S2_TOC_V2',
                'spatial_extent': None,
                'temporal_extent': ['2020-05-01', '2020-06-01']
            }
        },
        'readvector1': {
            'process_id': 'read_vector',
            'arguments': {
                'filename': 'https://artifactory.vgt.vito.be/artifactory/testdata-public/parcels/test_10.geojson'
            }
        },
        'aggregatespatial1': {
            'process_id': 'aggregate_spatial',
            'arguments': {
                'data': {
                    'from_node': 'loadcollection1'
                },
                'geometries': {
                    'from_node': 'readvector1'
                },
                'reducer': {
                    'process_graph': {
                        'mean1': {
                            'process_id': 'mean',
                            'arguments': {
                                'data': {
                                    'from_parameter': 'data'
                                }
                            },
                            'result': True
                        }
                    }
                }
            },
            'result': True
        }
    }
    evaluate(pg, env = dry_run_env)
    metadata = extract_result_metadata(dry_run_tracer)
    assert metadata["area"]["value"] == pytest.approx(187056.07523286293, abs=0.001)
    assert metadata["area"]["unit"] == "square meter"


def test_spatial_geoparquet(tmp_path):
    job_specification = {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "temporal_extent": ["2021-01-05", "2021-01-06"],
                    "bands": ["Flat:1", "Flat:2"]
                }
            },
            "reducedimension1": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "dimension": "t",
                    "reducer": {
                        "process_graph": {
                            "mean1": {
                                "arguments": {
                                    "data": {
                                        "from_parameter": "data"
                                    }
                                },
                                "process_id": "mean",
                                "result": True
                            }
                        }
                    }
                }
            },
            "aggregatespatial1": {
                "process_id": "aggregate_spatial",
                "arguments": {
                    "data": {"from_node": "reducedimension1"},
                    "geometries": {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "geometry": {
                                    "coordinates": [4.834132470464912, 51.14651864980539],
                                    "type": "Point"
                                },
                                "id": "0",
                                "properties": {"name": "maize"},
                                "type": "Feature"
                            },
                            {
                                "geometry": {
                                    "coordinates": [4.826795583109673, 51.154775560357045],
                                    "type": "Point"
                                },
                                "id": "1",
                                "properties": {"name": "maize"},
                                "type": "Feature"
                            }
                        ]
                    },
                    "reducer": {
                        "process_graph": {
                            "mean1": {
                                "arguments": {
                                    "data": {
                                        "from_parameter": "data"
                                    }
                                },
                                "process_id": "mean",
                                "result": True
                            }
                        }
                    }
                }
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "aggregatespatial1"},
                    "format": "Parquet"
                },
                "result": True
            }
        }
    }

    run_job(job_specification, output_file=tmp_path / "out", metadata_file=tmp_path / "metadata.json",
            api_version="1.0.0", job_dir="./", dependencies=[], user_id="jenkins")

    assert gpd.read_parquet(tmp_path / "timeseries.parquet").to_dict('list') == {
        'geometry': [Point(4.834132470464912, 51.14651864980539), Point(4.826795583109673, 51.154775560357045)],
        'feature_index': [0, 1],
        'name': ['maize', 'maize'],
        'avg_band_0': [1.0, 1.0],
        'avg_band_1': [2.0, 2.0],
    }
