import json
import xarray

from openeo.metadata import Band
from openeogeotrellis.deploy.batch_job import extract_result_metadata,run_job


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
    for asset in assets:
        theAsset = assets[asset]

        assert 'image/png' == theAsset['type']
        href = theAsset['href']
        from osgeo.gdal import Info
        info = Info(href, format='json')
        print(info)
        assert info['driverShortName'] == 'PNG'


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
                                "coordinates": [[[0.72, -0.516],[2.99,-1.29],[2.279,1.724],[0.725,-0.18],[0.725,-0.516]]]
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
    assets = metadata["assets"]
    assert len(assets) == 2
    for asset in assets:
        theAsset = assets[asset]
        bands = [Band(**b) for b in theAsset["bands"]]
        assert len(bands) == 1
        da = xarray.open_dataset(theAsset['href'], engine='h5netcdf')
        assert 'Flat:2' in da
        print(da['Flat:2'])


