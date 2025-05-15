import datetime as dt
import json
import os
import shutil
import tempfile
import textwrap
import uuid
from pathlib import Path, PurePath
from typing import Set
from unittest import mock

import geopandas as gpd
import pystac
import pytest
import rasterio
import xarray
from openeo.metadata import Band
from openeo.util import ensure_dir
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.errors import OpenEOApiException
from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER, evaluate
from openeo_driver.testing import DictSubSet, ephemeral_fileserver, ListSubSet
from openeo_driver.util.geometry import validate_geojson_coordinates
from openeo_driver.utils import EvalEnv
from openeo_driver.workspace import DiskWorkspace
from osgeo import gdal
from shapely.geometry import Point, Polygon, shape

from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.workspace import StacApiWorkspace
from openeogeotrellis._version import __version__
from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.deploy.batch_job import run_job
from openeogeotrellis.deploy.batch_job_metadata import extract_result_metadata
from openeogeotrellis.utils import s3_client, GDALINFO_SUFFIX
from openeogeotrellis.workspace import ObjectStorageWorkspace
from openeogeotrellis.workspace.custom_stac_io import CustomStacIO
from . import assert_cog
from .conftest import force_stop_spark_context, _setup_local_spark, TEST_AWS_REGION_NAME

from .data import TEST_DATA_ROOT, get_test_data_file


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


@pytest.mark.parametrize(
    ["from_node", "expected_names"],
    [
        (
            "loadcollection_sentinel2",
            {
                "openEO_2021-06-05Z_TileRow.tif",
                "openEO_2021-06-05Z_TileCol.tif",
                "openEO_2021-06-15Z_TileRow.tif",
                "openEO_2021-06-15Z_TileCol.tif",
            },
        ),
        ("reducedimension_temporal", {"openEO_TileRow.tif", "openEO_TileCol.tif"}),
    ],
)
def test_separate_asset_per_band(tmp_path, from_node, expected_names):
    job_spec = {
        "process_graph": {
            "loadcollection_sentinel2": {
                "process_id": "load_collection",
                "arguments": {
                    "bands": ["TileRow", "TileCol"],
                    "id": "TestCollection-LonLat16x16",
                    "properties": {},
                    "spatial_extent": {"west": 0.0, "south": 50.0, "east": 5.0, "north": 55.0},
                    "temporal_extent": ["2021-06-01", "2021-06-16"],
                },
            },
            "reducedimension_temporal": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {"from_node": "loadcollection_sentinel2"},
                    "dimension": "t",
                    "reducer": {
                        "process_graph": {
                            "min1": {
                                "process_id": "min",
                                "arguments": {"data": {"from_parameter": "data"}},
                                "result": True,
                            }
                        }
                    },
                },
            },
            "save1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": from_node},
                    "format": "GTIFF",
                    "options": {"separate_asset_per_band": True},
                },
                "result": True,
            },
        },
        "parameters": [],
    }
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
    assert metadata["start_datetime"] == "2021-06-01T00:00:00Z"
    assets = metadata["assets"]
    # get file names as set:
    asset_names = set(assets.keys())
    assert asset_names == expected_names

    for asset_key in assets:
        asset = assets[asset_key]
        assert len(asset["bands"]) == 1
        assert len(asset["raster:bands"]) == 1
        assert asset["bands"][0]["name"] == asset["raster:bands"][0]["name"]


def test_separate_asset_per_band_throw(tmp_path):
    job_spec = {
        "process_graph": {
            "loadcollection_sentinel2": {
                "process_id": "load_collection",
                "arguments": {
                    "bands": ["Longitude", "Day"],
                    "id": "TestCollection-LonLat4x4",
                    "properties": {},
                    "spatial_extent": {"east": 5.08, "north": 51.22, "south": 51.215, "west": 5.07},
                    "temporal_extent": ["2023-06-01", "2023-06-06"],
                },
            },
            "save1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "loadcollection_sentinel2"},
                    "format": "NETCDF",
                    "options": {"separate_asset_per_band": True},
                },
                "result": True,
            },
        },
        "parameters": [],
    }
    metadata_file = tmp_path / "metadata.json"
    with pytest.raises(OpenEOApiException):
        run_job(
            job_spec,
            output_file=tmp_path / "out",
            metadata_file=metadata_file,
            api_version="1.0.0",
            job_dir=ensure_dir(tmp_path / "job_dir"),
            dependencies=[],
            user_id="jenkins",
        )


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
        'Flat_1': [1.0, 1.0],
        'Flat_2': [2.0, 2.0],
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
                    "id": "TestCollection-LonLat16x16",
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


@pytest.mark.parametrize("remove_original", [False, True])
@pytest.mark.parametrize("attach_gdalinfo_assets", [False, True])
def test_export_workspace(tmp_path, remove_original, attach_gdalinfo_assets):
    workspace_id = "tmp"
    merge = _random_merge()

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-16"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"]
            }
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "options": {
                    "attach_gdalinfo_assets": attach_gdalinfo_assets,
                },
                "format": "GTiff"
            },
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": workspace_id,
                "merge": str(merge),
            },
            "result": True
        }
    }

    process = {
        "process_graph": process_graph,
        "job_options": {
            "remove-exported-assets": remove_original,
        },
    }

    # TODO: avoid depending on `/tmp` for test output, make sure to leverage `tmp_path` fixture (https://github.com/Open-EO/openeo-python-driver/issues/265)
    workspace: DiskWorkspace = get_backend_config().workspaces[workspace_id]
    workspace_dir = workspace.root_directory / merge

    try:
        metadata_file = tmp_path / "job_metadata.json"

        run_job(
            process,
            output_file=tmp_path / "out.tif",
            metadata_file=metadata_file,
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )

        job_dir_files = set(os.listdir(tmp_path))
        assert len(job_dir_files) > 0

        if remove_original:
            assert "openEO_2021-01-05Z.tif" not in job_dir_files
            assert "openEO_2021-01-15Z.tif" not in job_dir_files
        else:
            assert "openEO_2021-01-05Z.tif" in job_dir_files
            assert "openEO_2021-01-15Z.tif" in job_dir_files

        expected_paths = {
            Path("collection.json"),
            Path("openEO_2021-01-05Z.tif"),
            Path("openEO_2021-01-05Z.tif.json"),
            Path("openEO_2021-01-15Z.tif"),
            Path("openEO_2021-01-15Z.tif.json"),
        }
        if attach_gdalinfo_assets:
            expected_paths |= {
                Path(f"openEO_2021-01-05Z.tif{GDALINFO_SUFFIX}"),
                Path(f"openEO_2021-01-05Z.tif{GDALINFO_SUFFIX}.json"),
                Path(f"openEO_2021-01-15Z.tif{GDALINFO_SUFFIX}"),
                Path(f"openEO_2021-01-15Z.tif{GDALINFO_SUFFIX}.json"),
            }
        assert _paths_relative_to(workspace_dir) == expected_paths

        stac_collection = pystac.Collection.from_file(str(workspace_dir / "collection.json"))
        stac_collection.validate_all()

        assert stac_collection.extent.spatial.bboxes == [[0.0, 0.0, 1.0, 2.0]]
        assert stac_collection.extent.temporal.intervals == [
            [dt.datetime(2021, 1, 5, tzinfo=dt.timezone.utc), dt.datetime(2021, 1, 16, tzinfo=dt.timezone.utc)]
        ]

        item_links = [item_link for item_link in stac_collection.links if item_link.rel == "item"]
        assert len(item_links) == 4 if attach_gdalinfo_assets else 2
        item_link = [item_link for item_link in item_links if "openEO_2021-01-05Z.tif" in item_link.href][0]

        assert item_link.media_type == "application/geo+json"
        assert item_link.href == "./openEO_2021-01-05Z.tif.json"

        items = list(stac_collection.get_items())
        items = list(filter(lambda x: "data" in x.assets[x.id].roles, items))
        assert len(items) == 2

        item = [item for item in items if item.id == "openEO_2021-01-05Z.tif"][0]
        assert item.bbox == [0.0, 0.0, 1.0, 2.0]
        assert (shape(item.geometry).normalize()
                .almost_equals(Polygon.from_bounds(0.0, 0.0, 1.0, 2.0).normalize()))

        geotiff_asset = item.get_assets()["openEO_2021-01-05Z.tif"]
        assert "data" in geotiff_asset.roles
        assert geotiff_asset.href == "./openEO_2021-01-05Z.tif"
        assert geotiff_asset.media_type == "image/tiff; application=geotiff"
        assert geotiff_asset.extra_fields["eo:bands"] == [DictSubSet({"name": "Flat:2"})]
        assert geotiff_asset.extra_fields["raster:bands"] == [
            {
                "name": "Flat:2",
                "statistics": {"minimum": 2.0, "maximum": 2.0, "mean": 2.0, "stddev": 0.0, "valid_percent": 100.0},
            }
        ]

        geotiff_asset_copy_path = tmp_path / "openEO_2021-01-05Z.tif.copy"
        geotiff_asset.copy(str(geotiff_asset_copy_path))  # downloads the asset file
        with rasterio.open(geotiff_asset_copy_path) as dataset:
            assert dataset.driver == "GTiff"

        # TODO: check other things e.g. proj:
        with open(metadata_file) as f:
            job_metadata = json.load(f)

        assert job_metadata["assets"]["openEO_2021-01-05Z.tif"]["href"] == str(tmp_path / "openEO_2021-01-05Z.tif")

        assert not remove_original or (
            job_metadata["assets"]["openEO_2021-01-05Z.tif"]["public_href"]
            == f"file:{workspace_dir / 'openEO_2021-01-05Z.tif'}"
        )
    finally:
        shutil.rmtree(workspace_dir)


def test_export_workspace_with_asset_per_band(tmp_path):
    workspace_id = "tmp"
    merge = _random_merge()

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Latitude"],
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {"separate_asset_per_band": "true"},
            },
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": workspace_id,
                "merge": str(merge),
            },
            "result": True,
        },
    }

    process = {
        "process_graph": process_graph,
    }

    # TODO: avoid depending on `/tmp` for test output, make sure to leverage `tmp_path` fixture (https://github.com/Open-EO/openeo-python-driver/issues/265)
    workspace: DiskWorkspace = get_backend_config().workspaces[workspace_id]
    workspace_dir = workspace.root_directory / merge

    try:
        run_job(
            process,
            output_file=tmp_path / "out",
            metadata_file=tmp_path / JOB_METADATA_FILENAME,
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )

        job_dir_files = set(os.listdir(tmp_path))
        assert len(job_dir_files) > 0
        assert "openEO_2021-01-05Z_Longitude.tif" in job_dir_files
        assert "openEO_2021-01-05Z_Latitude.tif" in job_dir_files

        assert _paths_relative_to(workspace_dir) == {
            Path("collection.json"),
            Path("openEO_2021-01-05Z_Longitude.tif"),
            Path("openEO_2021-01-05Z_Longitude.tif.json"),
            Path("openEO_2021-01-05Z_Latitude.tif"),
            Path("openEO_2021-01-05Z_Latitude.tif.json"),
        }

        stac_collection = pystac.Collection.from_file(str(workspace_dir / "collection.json"))
        stac_collection.validate_all()

        item_links = [item_link for item_link in stac_collection.links if item_link.rel == "item"]
        assert len(item_links) == 2
        item_link = item_links[0]

        assert item_link.media_type == "application/geo+json"
        assert item_link.href == "./openEO_2021-01-05Z_Latitude.tif.json"

        items = list(stac_collection.get_items())
        assert len(items) == 2

        item = items[0]
        assert item.id == "openEO_2021-01-05Z_Latitude.tif"
        assert item.bbox == [0.0, 0.0, 1.0, 2.0]
        assert shape(item.geometry).normalize().almost_equals(Polygon.from_bounds(0.0, 0.0, 1.0, 2.0).normalize())

        geotiff_asset = item.get_assets()["openEO_2021-01-05Z_Latitude.tif"]
        assert "data" in geotiff_asset.roles
        assert geotiff_asset.href == "./openEO_2021-01-05Z_Latitude.tif"
        assert geotiff_asset.media_type == "image/tiff; application=geotiff"
        assert geotiff_asset.extra_fields["eo:bands"] == [DictSubSet({"name": "Latitude"})]
        assert geotiff_asset.extra_fields["raster:bands"] == [
            {
                "name": "Latitude",
                "statistics": {
                    "maximum": 1.9375,
                    "mean": 0.96875,
                    "minimum": 0.0,
                    "stddev": 0.57706829101936,
                    "valid_percent": 100.0,
                },
            }
        ]

        geotiff_asset_copy_path = tmp_path / "openEO_2021-01-05Z_Latitude.copy"
        geotiff_asset.copy(str(geotiff_asset_copy_path))  # downloads the asset file
        with rasterio.open(geotiff_asset_copy_path) as dataset:
            assert dataset.driver == "GTiff"
    finally:
        shutil.rmtree(workspace_dir, ignore_errors=True)


@pytest.mark.parametrize("use_s3", [False])  # use_s3 is only for debugging locally. Does not work on Jenkins
def test_filepath_per_band(
    tmp_path,
    use_s3,
    mock_s3_bucket,
    moto_server,
    monkeypatch,
):
    if use_s3:
        # TODO: the location where executors write result assets to (either disk or object storage as determined by
        #  the FUSE_MOUNT_BATCHJOB_S3_BUCKET envar) is not related to the type of workspace (DiskWorkspace or
        #  ObjectStorageWorkspace) that will be used.
        #  Case in point: FUSE_MOUNT_BATCHJOB_S3_BUCKET is typically set on CDSE, but its configuration currently
        #  only defines workspaces of type ObjectStorageWorkspace.
        workspace_id = "s3_workspace"
    else:
        workspace_id = "tmp_workspace"

    workspace = get_backend_config().workspaces[workspace_id]
    s3_instance = s3_client()

    merge = _random_merge()
    attach_gdalinfo_assets = True

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Latitude"],
            },
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {
                        "first1": {
                            "process_id": "first",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        }
                    }
                },
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "reducedimension1"},
                "format": "GTiff",
                "options": {
                    "separate_asset_per_band": "true",
                    "attach_gdalinfo_assets": attach_gdalinfo_assets,
                    "filepath_per_band": ["folder1/lon.tif", "lat.tif"],
                },
            },
            "result": False,
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": workspace_id,
                "merge": str(merge),
            },
            "result": True,
        },
    }

    if use_s3:
        monkeypatch.setenv("KUBE", "TRUE")
        force_stop_spark_context()  # only use this when running a single test

        class TerminalReporterMock:
            @staticmethod
            def write_line(message):
                print(message)

        _setup_local_spark(TerminalReporterMock(), 0)
    try:
        process = {
            "process_graph": process_graph,
        }
        run_job(
            process,
            output_file=tmp_path / "out",
            metadata_file=tmp_path / JOB_METADATA_FILENAME,
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )

        if use_s3:
            job_dir_files = {
                o["Key"] for o in s3_instance.list_objects(Bucket=get_backend_config().s3_bucket_name)["Contents"]
            }
            print(job_dir_files)
        job_dir_files = set(os.listdir(tmp_path))

        assert len(job_dir_files) > 0
        assert "lat.tif" in job_dir_files
        assert any(f.startswith("folder1") for f in job_dir_files)

        stac_collection = pystac.Collection.from_file(str(tmp_path / "collection.json"))
        stac_collection.validate_all()
        item_links = [item_link for item_link in stac_collection.links if item_link.rel == "item"]
        assert len(item_links) == 4 if attach_gdalinfo_assets else 2
        for item in item_links:
            assert os.path.exists(tmp_path / item.href)
        item_link = item_links[0]

        assert item_link.media_type == "application/geo+json"
        assert item_link.href == "./folder1/lon.tif.json"

        items_all = list(stac_collection.get_items())

        for item in items_all:
            assert os.path.exists(tmp_path / item.self_href)
            for asset in item.assets:
                assert (Path(item.self_href).parent / item.assets[asset].href).exists()

        items = list(filter(lambda x: "data" in x.assets[x.id].roles, items_all))
        assert len(items) == 2

        item = items[0]
        assert item.id == "folder1/lon.tif"

        geotiff_asset = item.get_assets()["folder1/lon.tif"]
        assert "data" in geotiff_asset.roles
        assert geotiff_asset.href == "./lon.tif"  # relative to the json file
        assert geotiff_asset.media_type == "image/tiff; application=geotiff"
        assert geotiff_asset.extra_fields["eo:bands"] == [DictSubSet({"name": "Longitude"})]
        if not use_s3:
            assert geotiff_asset.extra_fields["raster:bands"] == [
                {
                    "name": "Longitude",
                    "statistics": {
                        "maximum": 0.75,
                        "mean": 0.375,
                        "minimum": 0.0,
                        "stddev": 0.27950849718747,
                        "valid_percent": 100.0,
                    },
                }
            ]

            geotiff_asset_copy_path = tmp_path / "file.copy"
            geotiff_asset.copy(str(geotiff_asset_copy_path))  # downloads the asset file
            with rasterio.open(geotiff_asset_copy_path) as dataset:
                assert dataset.driver == "GTiff"

        if use_s3:
            # job bucket and workspace bucket are the same
            job_dir_files_s3 = [
                o["Key"] for o in s3_instance.list_objects(Bucket=get_backend_config().s3_bucket_name)["Contents"]
            ]
            for prefix in [merge, tmp_path.relative_to("/")]:
                assert job_dir_files_s3 == ListSubSet(
                    [
                        f"{prefix}/collection.json",
                        f"{prefix}/folder1/lon.tif",
                        f"{prefix}/folder1/lon.tif.json",
                        f"{prefix}/lat.tif",
                        f"{prefix}/lat.tif.json",
                    ]
                )

        else:
            assert isinstance(workspace, DiskWorkspace)
            workspace_dir = Path(f"{workspace.root_directory}/{merge}")
            assert workspace_dir.exists()
            assert (workspace_dir / "lat.tif").exists()
            assert (workspace_dir / "folder1/lon.tif").exists()
            stac_collection_exported = pystac.Collection.from_file(str(workspace_dir / "collection.json"))
            stac_collection_exported.validate_all()
    finally:
        if not use_s3:
            assert isinstance(workspace, DiskWorkspace)
            workspace_dir = Path(f"{workspace.root_directory}/{merge}")
            shutil.rmtree(workspace_dir, ignore_errors=True)


def test_export_workspace_merge_into_existing(tmp_path, mock_s3_bucket):
    object_workspace_id = "s3_workspace"

    enable_merge = True
    merge = _random_merge(is_actual_collection_document=enable_merge)

    object_workspace = get_backend_config().workspaces[object_workspace_id]
    assert isinstance(object_workspace, ObjectStorageWorkspace)
    assert object_workspace.bucket == get_backend_config().s3_bucket_name

    def run_merge_job(job_dir: Path, temporal_extent, expected_asset_filename: str):
        job_dir.mkdir()

        process_graph = {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TestCollection-LonLat16x16",
                    "temporal_extent": temporal_extent,
                    "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                    "bands": ["Flat:0", "Flat:1"],
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "format": "GTiff",
                },
            },
            "exportworkspace1": {
                "process_id": "export_workspace",
                "arguments": {
                    "data": {"from_node": "saveresult1"},
                    "workspace": object_workspace_id,
                    "merge": str(merge),
                },
                "result": True,
            },
        }

        process = {
            "process_graph": process_graph,
            "job_options": {
                "export-workspace-enable-merge": enable_merge,
            },
        }

        metadata_file = job_dir / "job_metadata.json"

        run_job(
            process,
            output_file=job_dir / "out",
            metadata_file=metadata_file,
            api_version="2.0.0",
            job_dir=job_dir,
            dependencies=[],
        )

        with open(metadata_file) as f:
            job_metadata = json.load(f)

        (asset_alternate,) = job_metadata["assets"][expected_asset_filename]["alternate"].values()
        # noinspection PyUnresolvedReferences
        assert asset_alternate["href"] == f"s3://{object_workspace.bucket}/{merge}/{expected_asset_filename}"

    run_merge_job(
        job_dir=tmp_path / "first",
        temporal_extent=["2021-01-05", "2021-01-06"],
        expected_asset_filename="openEO_2021-01-05Z.tif",
    )

    run_merge_job(
        job_dir=tmp_path / "second",
        temporal_extent=["2021-01-15", "2021-01-16"],
        expected_asset_filename="openEO_2021-01-15Z.tif",
    )

    object_workspace_keys = [PurePath(obj.key) for obj in mock_s3_bucket.objects.all()]

    assert object_workspace_keys == ListSubSet([
        merge,  # the Collection itself
        merge / "openEO_2021-01-05Z.tif",
        merge / "openEO_2021-01-05Z.tif.json",
        merge / "openEO_2021-01-15Z.tif",
        merge / "openEO_2021-01-15Z.tif.json",
    ])


def test_export_workspace_merge_filepath_per_band(tmp_path, mock_s3_bucket):
    job_dir = tmp_path

    object_workspace_id = "s3_workspace"  # most common scenario: assets on disk to workspace in object storage
    disk_workspace_id = "tmp"  # assets on disk to workspace on disk

    enable_merge = True
    merge = _random_merge(is_actual_collection_document=enable_merge)

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Latitude"],
            },
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {
                        "first1": {
                            "process_id": "first",
                            "arguments": {
                                "data": {"from_parameter": "data"}
                            },
                            "result": True,
                        }
                    }
                },
            }
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "reducedimension1"},
                "format": "GTiff",
                "options": {
                    "separate_asset_per_band": "true",
                    "filepath_per_band": ["some/deeply/nested/folder/lon.tif", "lat.tif"],
                },
            },
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": object_workspace_id,
                "merge": str(merge),
            },
            "result": True,
        },
        "exportworkspace2": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": disk_workspace_id,
                "merge": str(merge),
            },
        },
    }

    process = {
        "process_graph": process_graph,
        "job_options": {
            "export-workspace-enable-merge": enable_merge,
            "concurrent-save-results": 4,  # TODO: Make this the default
        },
    }

    disk_workspace = get_backend_config().workspaces[disk_workspace_id]
    assert isinstance(disk_workspace, DiskWorkspace)
    workspace_dir = (disk_workspace.root_directory / merge).parent

    try:
        run_job(
            process,
            output_file=job_dir / "out",
            metadata_file=job_dir / "job_metadata.json",
            api_version="2.0.0",
            job_dir=job_dir,
            dependencies=[],
        )

        assert list(_paths_relative_to(job_dir)) == ListSubSet([
            Path("some/deeply/nested/folder/lon.tif"),
            Path("lat.tif"),
        ])

        object_workspace = get_backend_config().workspaces[object_workspace_id]
        assert isinstance(object_workspace, ObjectStorageWorkspace)
        assert object_workspace.bucket == get_backend_config().s3_bucket_name

        object_workspace_keys = [PurePath(obj.key) for obj in mock_s3_bucket.objects.all()]

        assert object_workspace_keys == ListSubSet([
            merge,  # the Collection itself
            merge / "some/deeply/nested/folder/lon.tif",
            merge / "some/deeply/nested/folder/lon.tif.json",
            merge / "lat.tif",
            merge / "lat.tif.json",
        ])

        def load_exported_collection(collection_href: str):
            assets_in_object_storage = collection_href.startswith("s3://")

            stac_collection = pystac.Collection.from_file(collection_href, stac_io=CustomStacIO(TEST_AWS_REGION_NAME))
            assert stac_collection.validate_all() == 2

            assets = [
                asset for item in stac_collection.get_items(recursive=True) for asset in item.get_assets().values()
            ]
            assert len(assets) == 2

            for asset in assets:
                with tempfile.NamedTemporaryFile() as f:
                    if assets_in_object_storage:
                        object_key = _object_key(asset.get_absolute_href())
                        mock_s3_bucket.download_file(object_key, f.name)
                    else:  # the asset is on disk
                        asset.copy(f.name)

                    with rasterio.open(f.name) as dataset:
                        assert dataset.driver == "GTiff"

        load_exported_collection(f"s3://{object_workspace.bucket}/{merge}")
        load_exported_collection(str(disk_workspace.root_directory / merge))
    finally:
        if os.path.exists(workspace_dir):
            shutil.rmtree(workspace_dir)


@pytest.mark.parametrize("kube", [False])  # kube==True will not run on Jenkins
@pytest.mark.parametrize("merge", [
    PurePath("collection1"),
    PurePath("path/to/assets/collection1")
])
def test_export_workspace_merge_into_stac_api(
    tmp_path,
    mock_s3_bucket,
    requests_mock,
    kube,
    merge,
    moto_server,
    monkeypatch,
):
    if kube:
        assert not get_backend_config().fuse_mount_batchjob_s3_bucket

        monkeypatch.setenv("KUBE", "TRUE")
        force_stop_spark_context()

        class TerminalReporterMock:
            @staticmethod
            def write_line(message):
                print(message)

        _setup_local_spark(TerminalReporterMock(), 0)

    job_dir = tmp_path

    stac_api_workspace_id = "stac_api_workspace"
    stac_api_workspace = get_backend_config().workspaces[stac_api_workspace_id]
    assert isinstance(stac_api_workspace, StacApiWorkspace)

    enable_merge = True
    collection_id = merge.name

    # the root Catalog
    requests_mock.get(stac_api_workspace.root_url, json={
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "stac.test",
        "description": "stac.test",
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0/collections",
            "https://api.stacspec.org/v1.0.0/collections/extensions/transaction",
            "https://api.stacspec.org/v1.0.0/ogcapi-features/extensions/transaction",
        ],
        "links": [],
    })

    # does the Collection already exist?
    requests_mock.get(f"{stac_api_workspace.root_url}/collections/{collection_id}", status_code=404, text="Not Found")

    # create STAC objects
    create_collection = requests_mock.post(f"{stac_api_workspace.root_url}/collections")
    create_item = requests_mock.post(f"{stac_api_workspace.root_url}/collections/{collection_id}/items")

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Latitude"],
            },
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {
                        "first1": {
                            "process_id": "first",
                            "arguments": {
                                "data": {"from_parameter": "data"}
                            },
                            "result": True,
                        }
                    }
                },
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "reducedimension1"},
                "format": "GTiff",
                "options": {
                    "separate_asset_per_band": "true",
                    "filepath_per_band": ["some/deeply/nested/folder/lon.tif", "lat.tif"],
                },
            },
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult1"},
                "workspace": stac_api_workspace_id,
                "merge": str(merge),
            },
            "result": True,
        },
    }

    process = {
        "process_graph": process_graph,
        "job_options": {
            "export-workspace-enable-merge": enable_merge,
        },
    }

    run_job(
        process,
        output_file=job_dir / "out",
        metadata_file=job_dir / "job_metadata.json",
        api_version="2.0.0",
        job_dir=job_dir,
        dependencies=[],
    )

    assert create_collection.called_once
    assert create_item.call_count == 2

    assert create_item.request_history[0].json()["assets"] == {
        "lat.tif": DictSubSet({
            "href": f"s3://openeo-fake-bucketname/{merge}/lat.tif"
        })
    }

    assert create_item.request_history[1].json()["assets"] == {
        "some/deeply/nested/folder/lon.tif": DictSubSet({
            "href": f"s3://openeo-fake-bucketname/{merge}/some/deeply/nested/folder/lon.tif"
        })
    }

    exported_asset_keys = [PurePath(obj.key) for obj in mock_s3_bucket.objects.all()]

    assert exported_asset_keys == ListSubSet([
        merge / "some/deeply/nested/folder/lon.tif",
        merge / "lat.tif",
    ])


def _object_key(s3_uri: str) -> str:
    from urllib.parse import urlparse

    uri_parts = urlparse(s3_uri)

    if uri_parts.scheme != "s3":
        raise ValueError(s3_uri)

    return uri_parts.path.lstrip("/")


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
    assert os.listdir(tmp_path) == ["job_metadata.json"]


def test_multiple_top_level_side_effects(tmp_path, caplog):
    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "spatial_extent": {"west": 5, "south": 50, "east": 5.1, "north": 50.1},
                "temporal_extent": ["2024-07-11", "2024-07-21"],
                "bands": ["Flat:1"]
            }
        },
        "loadcollection2": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat4x4",
                "spatial_extent": {"west": 5, "south": 50, "east": 5.1, "north": 50.1},
                "temporal_extent": ["2024-07-11", "2024-07-21"],
                "bands": ["Flat:2"]
            }
        },
        "inspect1": {
            "process_id": "inspect",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "message": "intermediate result",
                "level": "warning"
            }
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {"filename_prefix": "intermediate"},
            }
        },
        "mergecubes1": {
            "process_id": "merge_cubes",
            "arguments": {
                "cube1": {"from_node": "loadcollection1"},
                "cube2": {"from_node": "loadcollection2"},
            },
            "result": True
        },
        "saveresult2": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "mergecubes1"},
                "format": "GTiff",
                "options": {"filename_prefix": "final"},
            }
        },
    }

    process = {"process_graph": process_graph}

    run_job(
        process,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    assert "intermediate result" in caplog.messages

    with rasterio.open(tmp_path / "intermediate_2024-07-15Z.tif") as dataset:
        assert dataset.count == 1

    with rasterio.open(tmp_path / "final_2024-07-15Z.tif") as dataset:
        assert dataset.count == 2


@pytest.mark.parametrize(["process_graph_file", "output_file_predicates"], [
    ("pg01.json", {
        "intermediate.tif": lambda dataset: dataset.res == (10, 10),
        "final.tif": lambda dataset: dataset.res == (80, 80)
    }),
    ("pg02.json", {
        "B04.tif": lambda dataset: (dataset.descriptions[0] or dataset.tags(1)["DESCRIPTION"]) == "B04",
        "B11.tif": lambda dataset: (dataset.descriptions[0] or dataset.tags(1)["DESCRIPTION"]) == "B11",
    }),
])
def test_multiple_save_results(tmp_path, process_graph_file, output_file_predicates):
    with open(get_test_data_file(f"multiple_save_results/{process_graph_file}")) as f:
        process = json.load(f)

    run_job(
        process,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    for output_file, predicate in output_file_predicates.items():
        with rasterio.open(tmp_path / output_file) as dataset:
            assert predicate(dataset)


def test_results_geometry_from_load_collection_with_crs_not_wgs84(tmp_path):
    process = {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "TERRASCOPE_S2_TOC_V2",
                    "spatial_extent": {
                        "west": 3962799.4509550678,
                        "south": 2999475.969536712,
                        "east": 3966745.556060158,
                        "north": 3005269.06681928,
                        "crs": 3035,
                    },
                    "temporal_extent": ["2021-01-04", "2021-01-06"],
                },
            },
            "saveresult1": {
                "process_id": "save_result",
                "arguments": {
                    "data": {"from_node": "loadcollection1"},
                    "format": "GTiff",
                },
                "result": True,
            },
        }
    }

    run_job(
        process,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    with open(tmp_path / "job_metadata.json") as f:
        results_geometry = json.load(f)["geometry"]

    validate_geojson_coordinates(results_geometry)


def test_load_ml_model_via_jobid(tmp_path):
    job_spec = {
      "process_graph": {
        "loadmlmodel1": {
          "process_id": "load_ml_model",
          "arguments": {
            "id": "j-2409091a32614623a5338083d040db83"
          }
        },
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-01", "2021-02-01"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["TileRow", "TileCol"]
            },
        },
        "reducedimension1": {
          "process_id": "reduce_dimension",
          "arguments": {
            "data": {
              "from_node": "loadcollection1"
            },
            "dimension": "t",
            "reducer": {
              "process_graph": {
                "mean1": {
                  "process_id": "mean",
                  "arguments": {
                    "data": {
                      "from_parameter": "data"
                    }
                  },
                  "result": True
                }
              }
            }
          }
        },
        "reducedimension2": {
          "process_id": "reduce_dimension",
          "arguments": {
            "context": {
              "from_node": "loadmlmodel1"
            },
            "data": {
              "from_node": "reducedimension1"
            },
            "dimension": "bands",
            "reducer": {
              "process_graph": {
                "predictrandomforest1": {
                  "process_id": "predict_random_forest",
                  "arguments": {
                    "data": {
                      "from_parameter": "data"
                    },
                    "model": {
                      "from_parameter": "context"
                    }
                  },
                  "result": True
                }
              }
            }
          }
        },
        "saveresult1": {
          "process_id": "save_result",
          "arguments": {
            "data": {
              "from_node": "reducedimension2"
            },
            "format": "GTiff",
            "options": {}
          },
          "result": True
        }
      }
    }
    metadata_file = tmp_path / "metadata.json"
    with mock.patch("openeogeotrellis.backend.GpsBatchJobs.get_job_output_dir") as mock_get_job_output_dir:
        mock_get_job_output_dir.return_value = Path(TEST_DATA_ROOT) / "mlmodel"
        run_job(
            job_spec,
            output_file=tmp_path / "out.tiff",
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
        assert assets["out.tiff"]


def test_load_stac_temporal_extent_in_result_metadata(tmp_path, requests_mock):
    with open(get_test_data_file("binary/load_stac/issue852-temporal-extent/process_graph.json")) as f:
        process = json.load(f)

    geoparquet_url = "http://foo.test/32736-random-points.geoparquet"

    process["process_graph"]["loadstac1"]["arguments"]["url"] = str(
        get_test_data_file("binary/load_stac/issue852-temporal-extent/s1/collection.json").absolute()
    )
    process["process_graph"]["loadstac2"]["arguments"]["url"] = str(
        get_test_data_file("binary/load_stac/issue852-temporal-extent/s2/collection.json").absolute()
    )
    process["process_graph"]["loadurl1"]["arguments"]["url"] = geoparquet_url

    with open(
        get_test_data_file("binary/load_stac/issue852-temporal-extent/32736-random-points.geoparquet"), "rb"
    ) as f:
        geoparquet_content = f.read()

    requests_mock.get(geoparquet_url, content=geoparquet_content)

    run_job(
        process,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    with open(tmp_path / "job_metadata.json") as f:
        job_metadata = json.load(f)

    # metadata checks
    expected_start_datetime = "2016-10-30T00:00:00+00:00"
    expected_end_datetime = "2018-05-03T00:00:00+00:00"

    time_series_asset = job_metadata["assets"]["timeseries.parquet"]
    assert time_series_asset.get("start_datetime") == expected_start_datetime
    assert time_series_asset.get("end_datetime") == expected_end_datetime

    # asset checks
    gdf = gpd.read_parquet(tmp_path / "timeseries.parquet")

    band_columns = [column_name for column_name in gdf.columns.tolist() if column_name.startswith("S")]
    assert len(band_columns) == 17

    timestamps = gdf["date"].tolist()
    assert len(timestamps) > 0
    assert all(expected_start_datetime <= timestamp <= expected_end_datetime for timestamp in timestamps)


def test_multiple_save_result_single_export_workspace(tmp_path):
    workspace_id = "tmp"
    merge = _random_merge()

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"],
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {"data": {"from_node": "loadcollection1"}, "format": "NetCDF", "options": {}},
        },
        "dropdimension1": {
            "process_id": "drop_dimension",
            "arguments": {"data": {"from_node": "loadcollection1"}, "name": "t"},
        },
        "saveresult2": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "dropdimension1"},
                "format": "GTiff",
            },
        },
        "exportworkspace1": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "saveresult2"},
                "workspace": workspace_id,
                "merge": str(merge),
            },
            "result": True,
        },
    }

    process = {
        "process_graph": process_graph,
    }

    # TODO: avoid depending on `/tmp` for test output, make sure to leverage `tmp_path` fixture (https://github.com/Open-EO/openeo-python-driver/issues/265)
    workspace: DiskWorkspace = get_backend_config().workspaces[workspace_id]
    workspace_dir = workspace.root_directory / merge

    try:
        run_job(
            process,
            output_file=tmp_path / "out",
            metadata_file=tmp_path / "job_metadata.json",
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )

        job_dir_files = set(os.listdir(tmp_path))
        assert len(job_dir_files) > 0
        assert "openEO.nc" in job_dir_files
        assert "openEO.tif" in job_dir_files

        assert _paths_relative_to(workspace_dir) == {
            Path("collection.json"),
            Path("openEO.tif"),
            Path("openEO.tif.json"),
        }

        stac_collection = pystac.Collection.from_file(str(workspace_dir / "collection.json"))
        stac_collection.validate_all()

        items = list(stac_collection.get_items())
        assert len(items) == 1

        item = items[0]
        geotiff_asset = item.get_assets()["openEO.tif"]
        assert geotiff_asset.extra_fields["raster:bands"] == [
            {
                "name": "Flat:2",
                "statistics": {"minimum": 2.0, "maximum": 2.0, "mean": 2.0, "stddev": 0.0, "valid_percent": 100.0},
            }
        ]

        geotiff_asset_copy_path = tmp_path / "openEO.tif.copy"
        geotiff_asset.copy(str(geotiff_asset_copy_path))  # downloads the asset file
        with rasterio.open(geotiff_asset_copy_path) as dataset:
            assert dataset.driver == "GTiff"
    finally:
        shutil.rmtree(workspace_dir)


def test_vectorcube_write_assets(tmp_path):
    with ephemeral_fileserver(TEST_DATA_ROOT) as fileserver_root:
        job_spec = {
            "title": "my job",
            "description": "vectorcube write_assets",
            "process_graph":{
                "geometry": {
                    "process_id": "load_url",
                    "arguments": {
                        "url": f"{fileserver_root}/geometries/FeatureCollection03.geoparquet",
                        "format": "Parquet",
                    },
                },
                "save": {
                    "process_id": "save_result",
                    "arguments": {"data": {"from_node": "geometry"}, "format": "geojson"},
                    "result": True,
                }
            }
        }
        metadata_file = tmp_path / "metadata.json"
        run_job(
            job_spec,
            output_file=tmp_path / "out.geojson",
            metadata_file=metadata_file,
            api_version="1.0.0",
            job_dir=ensure_dir(tmp_path / "job_dir"),
            dependencies={},
            user_id="jenkins",
        )


def test_custom_geotiff_tags(tmp_path):
    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 50.0, "east": 5.0, "north": 55.0},
                "bands": ["Flat:2"],
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "format": "GTiff",
                "options": {
                    "bands_metadata": {
                        "Flat:2": {
                            "SCALE": 1.23,
                            "OFFSET": 4.56,
                            "ARBITRARY": "value",
                        },
                    },
                    "file_metadata": {
                        "product_tile": "29TNE",
                        "product_type": "LSF monthly median composite for band B02",
                    },
                },
            },
            "result": True,
        },
    }

    process = {
        "process_graph": process_graph,
        "description": "some description",
    }

    run_job(
        process,
        output_file=tmp_path / "out.tif",
        metadata_file=tmp_path / "job_metadata.json",
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    # metadata should be embedded in the tiff, not in a sidecar file
    aux_files = [tmp_path / aux_file for aux_file in os.listdir(tmp_path) if aux_file.endswith(".tif.aux.xml")]
    for aux_file in aux_files:
        aux_file.unlink()

    output_tiffs = [tmp_path / tiff_file for tiff_file in os.listdir(tmp_path) if tiff_file.endswith(".tif")]
    assert len(output_tiffs) == 1
    output_tiff = output_tiffs[0]

    raster = gdal.Open(str(output_tiff))

    assert raster.GetMetadata() == DictSubSet({
        "AREA_OR_POINT": "Area",
        "PROCESSING_SOFTWARE": __version__,
        "ImageDescription": "some description",
        "product_tile": "29TNE",
        "product_type": "LSF monthly median composite for band B02",
    })

    band_count = raster.RasterCount
    assert band_count == 1
    band = raster.GetRasterBand(1)
    assert band.GetDescription() == "Flat:2"
    assert band.GetScale() == 1.23
    assert band.GetOffset() == 4.56
    band_metadata = band.GetMetadata()
    assert band_metadata["ARBITRARY"] == "value"

    assert_cog(output_tiff)


@pytest.mark.parametrize("remove_original", [False, True])
def test_export_to_multiple_workspaces(tmp_path, remove_original):
    workspace_id = "tmp"

    merge1 = _random_merge()
    merge2 = _random_merge()

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Flat:2"],
            },
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
                "workspace": "tmp",
                "merge": str(merge1),
            },
        },
        "exportworkspace2": {
            "process_id": "export_workspace",
            "arguments": {
                "data": {"from_node": "exportworkspace1"},
                "workspace": "tmp",
                "merge": str(merge2),
            },
            "result": True,
        }
    }

    process = {
        "process_graph": process_graph,
        "job_options": {
            "remove-exported-assets": remove_original,
        },
    }

    workspace: DiskWorkspace = get_backend_config().workspaces[workspace_id]

    try:
        metadata_file = tmp_path / "job_metadata.json"

        run_job(
            process,
            output_file=tmp_path / "out.tif",
            metadata_file=metadata_file,
            api_version="2.0.0",
            job_dir=tmp_path,
            dependencies=[],
        )

        with open(metadata_file) as f:
            job_metadata = json.load(f)

        asset = job_metadata["assets"]["openEO_2021-01-05Z.tif"]

        assert asset["href"] == str(tmp_path / "openEO_2021-01-05Z.tif")

        first_workspace_uri = f"file:{workspace.root_directory / max(merge1, merge2)}/openEO_2021-01-05Z.tif"
        second_workspace_uri = f"file:{workspace.root_directory / min(merge1, merge2)}/openEO_2021-01-05Z.tif"

        if remove_original:
            assert asset["public_href"] == first_workspace_uri
            assert asset["alternate"] == {
                f"{workspace_id}/{min(merge1, merge2)}": {
                    "href": second_workspace_uri,
                },
            }
        else:
            assert asset["alternate"] == {
                f"{workspace_id}/{max(merge1, merge2)}": {
                    "href": first_workspace_uri,
                },
                f"{workspace_id}/{min(merge1, merge2)}": {
                    "href": second_workspace_uri,
                },
            }
    finally:
        shutil.rmtree(workspace.root_directory / merge1)
        shutil.rmtree(workspace.root_directory / merge2)


def test_reduce_bands_to_geotiff(tmp_path):
    process = {
        "process_graph": {
            "loadcollection1": {
                "process_id": "load_collection",
                "arguments": {
                    "bands": [
                        "Flat:0"
                    ],
                    "id": "TestCollection-LonLat16x16",
                    "spatial_extent": {
                        "west": 4.906082,
                        "south": 51.024594,
                        "east": 4.928398,
                        "north": 51.034499
                    },
                    "temporal_extent": [
                        "2024-10-01",
                        "2024-10-10"
                    ]
                }
            },
            "reducedimension1": {
                "process_id": "reduce_dimension",
                "arguments": {
                    "data": {
                        "from_node": "loadcollection1"
                    },
                    "dimension": "bands",
                    "reducer": {
                        "process_graph": {
                            "arrayelement1": {
                                "process_id": "array_element",
                                "arguments": {
                                    "data": {
                                        "from_parameter": "data"
                                    },
                                    "index": 0
                                },
                                "result": True
                            }
                        }
                    }
                },
                "result": True
            }
        }
    }

    run_job(
        process,
        output_file=tmp_path / "out",
        metadata_file=tmp_path / JOB_METADATA_FILENAME,
        api_version="2.0.0",
        job_dir=tmp_path,
        dependencies=[],
    )

    output_tiffs = {filename for filename in os.listdir(tmp_path) if filename.endswith(".tif")}
    assert output_tiffs == {"openEO_2024-10-05Z.tif"}

    raster = gdal.Open(str(tmp_path / output_tiffs.pop()))
    assert raster.RasterCount == 1

    only_band = raster.GetRasterBand(1)
    assert not only_band.GetDescription()


def _random_merge(is_actual_collection_document: bool = False) -> PurePath:
    subdirectory = PurePath(f"OpenEO-workspace-{uuid.uuid4()}")
    return subdirectory / "collection.json" if is_actual_collection_document else subdirectory


def _paths_relative_to(base: Path) -> Set[Path]:
    return {
        (Path(dirpath) / filename).relative_to(base)
        for dirpath, dirnames, filenames in os.walk(base)
        for filename in filenames
    }


def test_spatial_geotiff_metadata(tmp_path):
    job_dir = tmp_path

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": ["Longitude", "Latitude"],
            },
        },
        "reducedimension1": {
            "process_id": "reduce_dimension",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "dimension": "t",
                "reducer": {
                    "process_graph": {
                        "first1": {
                            "process_id": "first",
                            "arguments": {"data": {"from_parameter": "data"}},
                            "result": True,
                        }
                    }
                },
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "reducedimension1"},
                "format": "GTiff",
            },
            "result": True,
        },
    }

    process = {
        "process_graph": process_graph,
    }

    metadata_file = job_dir / "job_metadata.json"

    run_job(
        process,
        output_file=job_dir / "out",
        metadata_file=metadata_file,
        api_version="2.0.0",
        job_dir=job_dir,
        dependencies=[],
    )

    with open(metadata_file) as f:
        assets = json.load(f)["assets"]

    assert set(assets.keys()) == {"openEO.tif"}
    assert assets["openEO.tif"]["bbox"] == [0.0, 0.0, 1.0, 2.0]
    assert (
        shape(assets["openEO.tif"]["geometry"])
        .normalize()
        .equals_exact(Polygon.from_bounds(0.0, 0.0, 1.0, 2.0).normalize(), tolerance=0.001)
    )


@pytest.mark.parametrize(
    ["window_size", "default_tile_size", "requested_tile_size", "expected_tile_size"],
    [
        (32, None, None, 32),  # keep valid
        (32, None, 64, 64),  # replace valid with requested
        (81, None, None, 256),  # replace invalid with default
        (81, 128, None, 128),  # replace invalid with overridden default
        (81, None, 64, 64),  # replace invalid with requested
    ],
)
def test_geotiff_tile_size(tmp_path, window_size, default_tile_size, requested_tile_size, expected_tile_size):
    job_dir = tmp_path

    bands = ["Longitude", "Latitude"]

    udf_code = """
        from openeo.udf import XarrayDataCube

        def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
            return cube
    """

    process_graph = {
        "loadcollection1": {
            "process_id": "load_collection",
            "arguments": {
                "id": "TestCollection-LonLat16x16",
                "temporal_extent": ["2021-01-05", "2021-01-06"],
                "spatial_extent": {"west": 0.0, "south": 0.0, "east": 1.0, "north": 2.0},
                "bands": bands,
            },
        },
        "applyneighborhood1": {
            "process_id": "apply_neighborhood",
            "arguments": {
                "data": {"from_node": "loadcollection1"},
                "size": [
                    {"dimension": "x", "value": window_size, "unit": "px"},
                    {"dimension": "y", "value": window_size, "unit": "px"},
                ],
                "overlap": [
                    {"dimension": "x", "value": 0, "unit": "px"},
                    {"dimension": "y", "value": 0, "unit": "px"}
                ],
                "process": {
                    "process_graph": {
                        "runudf1": {
                            "process_id": "run_udf",
                            "arguments": {
                                "data": {"from_parameter": "data"},
                                "runtime": "Python",
                                "udf": textwrap.dedent(udf_code),
                            },
                            "result": True,
                        }
                    }
                },
            },
        },
        "saveresult1": {
            "process_id": "save_result",
            "arguments": {
                "data": {"from_node": "applyneighborhood1"},
                "format": "GTiff",
                "options": {
                    "tile_size": requested_tile_size,
                },
            },
            "result": True,
        },
    }

    process = {
        "process_graph": process_graph,
    }

    metadata_file = job_dir / "job_metadata.json"

    with gps_config_overrides(default_tile_size=default_tile_size):
        run_job(
            process,
            output_file=job_dir / "out",
            metadata_file=metadata_file,
            api_version="2.0.0",
            job_dir=job_dir,
            dependencies=[],
        )

    output_tiff = job_dir / "openEO_2021-01-05Z.tif"

    with rasterio.open(output_tiff) as dataset:
        assert dataset.crs.to_epsg() == 4326
        assert dataset.count == len(bands)
        for block_shape in dataset.block_shapes:
            assert block_shape == (expected_tile_size, expected_tile_size)

    assert_cog(output_tiff)
