import json
import os
import shutil
import uuid
from pathlib import Path

import geopandas as gpd
import pystac
import pytest

from openeo.util import ensure_dir
from openeo_driver.testing import DictSubSet
from shapely.geometry import Point, Polygon, mapping, shape
import xarray

from openeo.metadata import Band

from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER, evaluate
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.testing import ephemeral_fileserver
from openeo_driver.utils import EvalEnv
from openeogeotrellis.deploy.batch_job import run_job, extract_result_metadata
from .data import TEST_DATA_ROOT


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
    run_job(
        job_spec,
        output_file=tmp_path / "out.png",
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies={},
        user_id="jenkins",
    )
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
    run_job(
        job_spec,
        output_file=tmp_path / "out.json",
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies={},
        user_id="jenkins",
    )
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
    run_job(
        job_spec,
        output_file=tmp_path / "out.nc",
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies={},
        user_id="jenkins",
    )
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
def test_ep3874_sample_by_feature_filter_spatial_inline_geojson(prefix, tmp_path):
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
                        "properties": {"id":22},
                        "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
                    },
                        {
                            "type": "Feature",
                            "properties": {"id":"myTextId"},
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
                "sample_by_feature":True,
                "feature_id_property": "id"
            }},
            "result": True,
        }
    }}
    metadata_file = tmp_path / "metadata.json"
    run_job(
        job_spec,
        output_file=tmp_path / "out",
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies={},
        user_id="jenkins",
    )
    with metadata_file.open() as f:
        metadata = json.load(f)
    assert metadata["start_datetime"] == "2021-01-04T00:00:00Z"
    assets = metadata["assets"]
    assert len(assets) == 2
    if prefix:
        assert assets[prefix + "_22.nc"]
        assert assets[prefix + "_myTextId.nc"]
    else:
        assert assets["openEO_22.nc"]
        assert assets["openEO_myTextId.nc"]

    for asset in assets:
        theAsset = assets[asset]
        bands = [Band(**b) for b in theAsset["bands"]]
        assert len(bands) == 1
        da = xarray.open_dataset(theAsset['href'], engine='h5netcdf')
        assert 'Flat:2' in da
        print(da['Flat:2'])


def test_sample_by_feature_filter_spatial_vector_cube_from_load_url(tmp_path):
    """
    sample_by_feature with vector cube loaded through load_url
    https://github.com/Open-EO/openeo-geopyspark-driver/issues/700
    """
    with ephemeral_fileserver(TEST_DATA_ROOT) as fileserver_root:
        job_spec = {
            "process_graph": {
                "lc": {
                    "process_id": "load_collection",
                    "arguments": {
                        "id": "TestCollection-LonLat4x4",
                        "temporal_extent": ["2021-01-04", "2021-01-06"],
                        "bands": ["Longitude"],
                    },
                },
                "geometry": {
                    "process_id": "load_url",
                    "arguments": {
                        "url": f"{fileserver_root}/geometries/FeatureCollection03.geoparquet",
                        "format": "Parquet",
                    },
                },
                "filterspatial1": {
                    "process_id": "filter_spatial",
                    "arguments": {
                        "data": {"from_node": "lc"},
                        "geometries": {"from_node": "geometry"},
                    },
                },
                "save": {
                    "process_id": "save_result",
                    "arguments": {
                        "data": {"from_node": "filterspatial1"},
                        "format": "netCDF",
                        "options": {"sample_by_feature": True},
                    },
                    "result": True,
                },
            }
        }
        metadata_file = tmp_path / "metadata.json"
        run_job(
            job_spec,
            output_file=tmp_path / "out",
            metadata_file=metadata_file,
            api_version="1.0.0",
            job_dir=ensure_dir(tmp_path / "job_dir"),
            dependencies={},
            user_id="jenkins",
        )

    # Check result metadata
    with metadata_file.open() as f:
        result_metadata = json.load(f)
    assets = result_metadata["assets"]
    assert len(assets) == 4

    # Check asset contents
    asset_minima = {}
    for name, asset_metadata in assets.items():
        assert asset_metadata["bands"] == [{"name": "Longitude"}]
        ds = xarray.open_dataset(asset_metadata["href"])
        asset_minima[name] = ds["Longitude"].min().item()

    assert asset_minima == {
        "openEO_0.nc": 1.0,
        "openEO_1.nc": 4.0,
        "openEO_2.nc": 2.0,
        "openEO_3.nc": 5.0,
    }


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
                                "name": "EPSG:4326"
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
    run_job(
        pg,
        output_file=tmp_path / "out",
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies={},
        user_id="jenkins",
    )
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

    run_job(
        job_specification,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "metadata.json",
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies=[],
        user_id="jenkins",
    )

    assert gpd.read_parquet(tmp_path / "timeseries.parquet").to_dict('list') == {
        'geometry': [Point(4.834132470464912, 51.14651864980539), Point(4.826795583109673, 51.154775560357045)],
        'feature_index': [0, 1],
        'name': ['maize', 'maize'],
        'avg_band_0_': [1.0, 1.0],
        'avg_band_1_': [2.0, 2.0],
    }


def test_spatial_cube_to_netcdf_sample_by_feature(tmp_path):
    job_spec = {"process_graph": {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-04", "2021-01-06"],
                "bands": ["Flat:2"]
            },
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
        "filterspatial1": {
            "process_id": "filter_spatial",
            "arguments": {
                "data": {"from_node": "reducedimension1"},
                "geometries":  {
                    "type": "FeatureCollection",
                    "features": [{
                        "type": "Feature",
                        "properties": {},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0.1, 0.1], [1.8, 0.1], [1.1, 1.8], [0.1, 0.1]]]},
                    }, {
                        "type": "Feature",
                        "properties": {},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [[[0.725, -0.516], [2.99, -1.29], [2.279, 1.724], [0.725, -0.18],
                                             [0.725, -0.516]]]
                        }
                    }]
                }
            }
        },
        "save": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "filterspatial1"},
                "format": "netCDF",
                "options": {"sample_by_feature": True}
            },
            "result": True
        }
    }}

    metadata_file = tmp_path / "metadata.json"
    run_job(
        job_spec,
        output_file=tmp_path / "out",
        metadata_file=metadata_file,
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies=[],
        user_id="jenkins",
    )

    with metadata_file.open() as f:
        metadata = json.load(f)

    # the envelope of the input features
    assert metadata["bbox"] == [0.1, -1.29, 2.99, 1.8]

    # analogous to GTiff
    assert metadata["start_datetime"] == "2021-01-04T00:00:00Z"
    assert metadata["end_datetime"] == "2021-01-06T00:00:00Z"

    # expected: 2 assets with bboxes that correspond to the input features
    assets = metadata["assets"]
    assert len(assets) == 2

    assert assets["openEO_0.nc"]["bbox"] == [0.1, 0.1, 1.8, 1.8]
    assert (shape(assets["openEO_0.nc"]["geometry"]).normalize()
            .almost_equals(Polygon.from_bounds(0.1, 0.1, 1.8, 1.8).normalize()))

    assert assets["openEO_1.nc"]["bbox"] == [0.725, -1.29, 2.99, 1.724]
    assert (shape(assets["openEO_1.nc"]["geometry"]).normalize()
            .almost_equals(Polygon.from_bounds(0.725, -1.29, 2.99, 1.724).normalize()))


def test_multiple_time_series_results(tmp_path):
    job_spec = {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "spatial_extent": {"west": 0.0, "south": 50.0, "east": 5.0, "north": 55.0},
                    "temporal_extent": ["2021-01-04", "2021-01-06"],
                    "bands": ["Flat:2"]
                },
            },
            "aggregatespatial1": {
                "process_id": "aggregate_spatial",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "geometries": {
                        "type": "Point",
                        "coordinates": [2.5, 52.5],
                    },
                    "reducer": {
                        "process_graph": {
                            "mean1": {
                                "process_id": "mean",
                                "arguments": {
                                    "data": {"from_parameter": "data"}
                                },
                                "result": True,
                            }
                        }
                    }
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "aggregatespatial1"},
                    "format": "JSON",
                },
            },
            "saveresult2": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "saveresult1"},
                    "format": "CSV",
                },
                "result": True,
            },
        }
    }

    run_job(
        job_spec,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies=[],
        user_id="jenkins",
    )

    output_files = os.listdir(tmp_path)

    assert "timeseries.json" in output_files
    assert "timeseries.csv" in output_files


def test_multiple_image_collection_results(tmp_path):
    job_spec = {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat4x4",
                    "spatial_extent": {"west": 0.0, "south": 50.0, "east": 5.0, "north": 55.0},
                    "temporal_extent": ["2021-01-04", "2021-01-06"],
                    "bands": ["Flat:2"]
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "format": "GTiff",
                },
            },
            "saveresult2": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "saveresult1"},
                    "format": "netCDF",
                },
                "result": True,
            },
        }
    }

    run_job(
        job_spec,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="1.0.0",
        job_dir=ensure_dir(tmp_path / "job_dir"),
        dependencies=[],
        user_id="jenkins",
    )

    output_files = os.listdir(tmp_path)

    assert "openEO_2021-01-05Z.tif" in output_files
    assert "openEO.nc" in output_files


def test_export_workspace(tmp_path):
    workspace_id = "tmp"
    merge = f"OpenEO-workspace-{uuid.uuid4()}"

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"]
            }
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff"
            },
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": workspace_id,
                "merge": merge,
            },
            "result": True
        }
    }

    process = {"process_graph": process_graph}

    # TODO: avoid depending on `/tmp` for test output, make sure to leverage `tmp_path` fixture
    workspace_dir = Path(f"/tmp/{merge}")
    workspace_dir.mkdir()
    try:
        run_job(
            process,
            output_file=tmp_path / "out.tif",
            metadata_file=tmp_path / "job_metadata.json",
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )

        output_file = tmp_path / "openEO_2021-01-05Z.tif"
        assert output_file.exists()

        workspace_files = os.listdir(workspace_dir)
        assert set(workspace_files) == {
            "collection.json",
            "openEO_2021-01-05Z.tif",
            "openEO_2021-01-05Z.tif.json"
        }

        stac_collection = pystac.Collection.from_file(str(workspace_dir / "collection.json"))
        stac_collection.validate_all()

        item_links = [item_link for item_link in stac_collection.links if item_link.rel == "item"]
        assert len(item_links) == 1
        item_link = item_links[0]

        assert item_link.media_type == "application/geo+json"
        assert item_link.href == "./openEO_2021-01-05Z.tif.json"

        items = list(stac_collection.get_items())
        assert len(items) == 1

        item = items[0]
        assert item.id == "openEO_2021-01-05Z.tif"
        assert item.bbox == [0.0, 0.0, 1.0, 2.0]
        assert (shape(item.geometry).normalize()
                .almost_equals(Polygon.from_bounds(0.0, 0.0, 1.0, 2.0).normalize()))

        geotiff_asset = item.get_assets()["openEO_2021-01-05Z.tif"]
        assert "data" in geotiff_asset.roles
        assert geotiff_asset.href == "./openEO_2021-01-05Z.tif"
        assert geotiff_asset.media_type == "image/tiff; application=geotiff"
        assert geotiff_asset.extra_fields["eo:bands"] == [DictSubSet({"name": "Flat:2"})]

        geotiff_asset_file = tmp_path / "openEO_2021-01-05Z_copy.tif"
        geotiff_asset.copy(str(geotiff_asset_file))  # downloads the asset file
        assert geotiff_asset_file.exists()

        # TODO: check other things e.g. proj:
    finally:
        shutil.rmtree(workspace_dir)


def test_discard_result(tmp_path):
    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"]
            }
        },
        "discardresult1": {
            "process_id": "discard_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
            },
            "result": True,
        },
    }

    process = {"process_graph": process_graph}

    run_job(
        process,
        output_file=tmp_path / "out.tif",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    # runs to completion without output assets
    assert set(os.listdir(tmp_path)) == {"job_metadata.json", "collection.json"}
