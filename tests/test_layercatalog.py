import logging
import unittest.mock as mock
from pathlib import Path
from typing import List, Tuple, Union

import dirty_equals
import pytest
import schema
from openeo.util import deep_get
from openeo.utils.version import ComparableVersion
from openeo_driver.backend import LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.processes import ProcessRegistry
from openeo_driver.specs import read_spec
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv, read_json
from openeo_driver.views import OPENEO_API_VERSION_DEFAULT

from openeogeotrellis.backend import GpsProcessing
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata
from openeogeotrellis.layercatalog import (
    _get_sar_backscatter_arguments,
    _merge_layers_with_common_name,
    get_layer_catalog,
)
from openeogeotrellis.vault import Vault


def _get_layers() -> List[Tuple[str, dict]]:
    vault = Vault("http://example.org")
    catalog = get_layer_catalog(vault)
    layers = catalog.get_all_metadata()
    return [(layer["id"], layer) for layer in layers]


def test_opensearch_enrich_default():
    assert get_backend_config().opensearch_enrich is False


@pytest.mark.parametrize(["id", "layer"], _get_layers())
def test_layer_metadata(id, layer):
    # TODO: move/copy to openeo-deploy project?
    assert "bands" not in layer
    assert deep_get(layer, "properties", "cube:dimensions", default=None) is None
    assert deep_get(layer, "properties", "eo:bands", default=None) is None
    eo_bands = [b["name"] for b in deep_get(layer, "summaries", 'eo:bands', default=[])]
    cube_dimension_bands = []
    for cube_dim in layer.get("cube:dimensions", {}).values():
        if cube_dim["type"] == "bands":
            cube_dimension_bands = cube_dim["values"]
    if eo_bands:
        assert eo_bands == cube_dimension_bands

    def valid_bbox(bbox):
        return len(bbox) == 4 and bbox[0] <= bbox[2] and bbox[1] <= bbox[3]

    assert schema.Schema({
        "spatial": {
            "bbox": [
                schema.And([schema.Or(int, float)], valid_bbox)
            ]
        },
        "temporal": {"interval": [[schema.Or(str, None)]]}
    }).validate(layer["extent"])

    gps_metadata = GeopysparkCubeMetadata(layer)
    gps_metadata = gps_metadata.filter_bands([ cube_dimension_bands[0] ])
    titles = gps_metadata.opensearch_link_titles
    if gps_metadata.band_dimension.band_aliases[0] is not None and len(gps_metadata.band_dimension.band_aliases[0])>0:
        assert titles[0] == gps_metadata.band_dimension.band_aliases[0][0]
    else:
        assert titles[0] == cube_dimension_bands[0]




def test_get_layer_catalog_with_updates(vault):
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            "tests/data/layercatalog01.json",
            "tests/data/layercatalog02.json",
        ]
        catalog = get_layer_catalog(vault)
        assert sorted(l["id"] for l in catalog.get_all_metadata()) == ["BAR", "BZZ", "FOO", "QUU"]
        foo = catalog.get_collection_metadata("FOO")
        assert foo["license"] == "apache"
        assert foo["links"] == ["example.com/foo"]
        bar = catalog.get_collection_metadata("BAR")
        assert bar["description"] == "The BAR layer"
        assert bar["links"] == ["example.com/bar"]


# skip because test depends on external config
def skip_sentinelhub_layer(vault):
    catalog = get_layer_catalog(vault)
    viewingParameters = {}
    viewingParameters["from"] = "2018-01-01"
    viewingParameters["to"] = "2018-01-02"

    viewingParameters["left"] = 4
    viewingParameters["right"] = 4.0001
    viewingParameters["top"] = 50.00001
    viewingParameters["bottom"] = 50.0
    viewingParameters["srs"] = "EPSG:4326"
    datacube = catalog.load_collection("SENTINEL1_GAMMA0_SENTINELHUB", viewingParameters)


def test_get_layer_catalog_opensearch_enrich_oscars(requests_mock, vault):
    test_root = Path(__file__).parent / "data"
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            test_root / "layercatalog01.json",
            test_root / "layercatalog02.json",
            test_root / "layercatalog03_oscars.json"
        ]


        collections_response = read_json(test_root / "collections_oscars01.json")
        requests_mock.get("https://services.terrascope.test/catalogue/collections", json=collections_response)

        all_metadata = get_layer_catalog(vault, opensearch_enrich=True).get_all_metadata()

    assert all_metadata == [
        {
            "id": "XIP",
            "_vito": {
                "data_source": {
                    "opensearch_endpoint": "https://services.terrascope.test/catalogue",
                    "opensearch_collection_id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1"
                }
            },
            "title": "Sentinel 1 GRD Sigma0 product, VH, VV and angle.",
            "description": "The Sigma0 product describes how much of the radar signal that was sent out by Sentinel-1 "
                           "is reflected back to the sensor...",
            "extent": {
                "spatial": {"bbox": [[-1.05893, 47.66031, 11.6781, 53.67487]]},
                "temporal": {"interval": [["2014-10-23", None]]}
            },
            'keywords': ['VITO',
                         'C-SAR',
                         'Orthoimagery',
                         'SENTINEL-1A',
                         'SENTINEL-1',
                         'SENTINEL-1B',
                         'RADAR BACKSCATTER',
                         'RADAR'],
            "links": [
                {
                    "rel": "alternate",
                    "href": "https://docs.terrascope.be/#/DataProducts/Sentinel-1/ProductsOverview",
                    "title": "Online User Documentation"
                },
                {
                    "rel": "alternate",
                    "href": "https://www.vito-eodata.be/collections/srv/eng/main.home?uuid=urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1"
                },
                {
                    "rel": "alternate",
                    "href": "https://services.terrascope.be/catalogue/description.geojson?collection=urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
                    "title": "OpenSearch entry point"
                }
            ],
            "cube:dimensions": {
                "x": {"type": "spatial", "axis": "x"},
                "y": {"type": "spatial", "axis": "y"},
                "t": {"type": "temporal"},
                "bands": {
                    "type": "bands",
                    "values": ["VH"]
                }
            },
            "summaries": {
                "eo:bands": [
                    {
                        "description": "Calibrated radar backscattering coefficient (unitless), describing the returned radar signal strength in the cross-polarized channel (V transmit, H receive). Values are stored as floats.",
                        "type": "VH",
                        "title": "VH",
                        "resolution": 10,
                        "bitPerValue": 32,
                        "name": "VH"
                    }
                ],
                "platform": [],
                "instruments": ["MSI"]
            },
            "assets":{}
        },
        {
            "id": "FOO",
            "license": "apache",
            "links": [
                "example.com/foo"
            ]
        },
        {
            "id": "BAR",
            "description": "The BAR layer",
            "links": [
                "example.com/bar"
            ]
        },
        {
            "id": "BZZ"
        },
        {
            "id": "QUU"
        }
    ]


def test_get_layer_catalog_opensearch_enrich_creodias(requests_mock, vault):
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            "tests/data/layercatalog01.json",
            "tests/data/layercatalog04_creodias.json"
        ]
        collections_response = read_json("tests/data/collections_creodias01.json")
        requests_mock.get("https://finder.creodias.test/resto/collections.json", json=collections_response)

        all_metadata = get_layer_catalog(vault, opensearch_enrich=True).get_all_metadata()

    assert all_metadata == [
        {
            "id": "WUQ",
            "title": "Sentinel-1 Collection",
            "description": "Sentinel-1 Collection",
            "keywords": ["esa", "sentinel", "sentinel1", "s1", "radar"],
            "_vito": {
                "data_source": {
                    "opensearch_collection_id": "Sentinel1",
                    "opensearch_endpoint": "https://finder.creodias.test"
                }
            },
            "cube:dimensions": {
                "t": {"type": "temporal"},
                "x": {"axis": "x", "type": "spatial"},
                "y": {"axis": "y", "type": "spatial"}
            },
        },
        {"id": "FOO", "license": "mit"},
        {"id": "BAR", "description": "bar",  "links": ["example.com/bar"]},
        {"id": "BZZ"}
    ]


def test_layer_catalog_step_resolution(vault):
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            str(Path(__file__).parent / "layercatalog.json"),
        ]
        catalog = get_layer_catalog(vault, opensearch_enrich=True)
        all_metadata = catalog.get_all_metadata()

    warnings = ""
    for layer in all_metadata:
        metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata(collection_id=layer["id"]))
        warn_str = f"\n{layer['id']=}\n"

        gsd_in_meter = metadata.get_GSD_in_meters()
        warn_str += f"{gsd_in_meter=}\n"
        if gsd_in_meter is None:
            continue
        if isinstance(gsd_in_meter, tuple):
            gsd_in_meter = {"general": gsd_in_meter}

        for band, resolution in gsd_in_meter.items():
            # Example layer with low resolution: SEA_ICE_INDEX (25km)
            if (not 0.1 < resolution[0] < 50000) or (not 0.1 < resolution[1] < 50000):
                warn_str += "WARNING: gsd is not in expected range: " + str(resolution[0]) + "m\n"
                warnings += warn_str + "\n"
                break
    assert warnings == ""


def test_get_layer_native_extent_specific(vault):
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            str(Path(__file__).parent / "layercatalog.json"),
        ]
        catalog = get_layer_catalog(vault, opensearch_enrich=True)
        metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata(collection_id="SENTINEL1_CARD4L"))
        assert metadata.get_layer_native_extent() == BoundingBox(
            west=-26.15, south=-48, east=60.42, north=39, crs="EPSG:4326"
        )


def test_get_layer_native_extent_all(vault):
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            str(Path(__file__).parent / "layercatalog.json"),
        ]
        catalog = get_layer_catalog(vault, opensearch_enrich=True)
    all_metadata = catalog.get_all_metadata()
    for layer in all_metadata:
        print(layer["id"])
        metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata(collection_id=layer["id"]))
        metadata.get_layer_native_extent()


def test_get_layer_native_extent_empty(vault):
    metadata = GeopysparkCubeMetadata({})
    assert metadata.get_layer_native_extent() is None


def test_merge_layers_with_common_name_nothing():
    metadata = {"FOO": {"id": "FOO"}, "BAR": {"id": "BAR"}}
    _merge_layers_with_common_name(metadata)
    assert metadata == {"FOO": {"id": "FOO"}, "BAR": {"id": "BAR"}}


def test_merge_layers_with_common_name_simple():
    metadata = {
        "FOO": {"id": "FOO", "common_name": "S2"},
        "BAR": {"id": "BAR", "common_name": "S2"},
    }
    _merge_layers_with_common_name(metadata)
    assert metadata == {
        "BAR": {"common_name": "S2", "id": "BAR"},
        "FOO": {"common_name": "S2", "id": "FOO"},
        "S2": {
            "id": "S2",
            "_vito": {
                "data_source": {
                    "common_name": "S2",
                    "merged_collections": ["FOO", "BAR"],
                    "type": "merged_by_common_name",
                }
            },
            "extent": {"spatial": {"bbox": []}, "temporal": {"interval": []}},
            "links": [],
            "providers": [],
        },
    }


def test_merge_layers_with_common_name_band_order():
    metadata = {
        "FOO": {
            "id": "FOO",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04"]}},
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
        },
        "BAR": {
            "id": "BAR",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B03", "B02", "B01", "B06"]}},
            "summaries": {"eo:bands": [{"name": "B03"}, {"name": "B02"}, {"name": "B01"}, {"name": "B06"}]},
        },
    }
    _merge_layers_with_common_name(metadata)
    assert metadata == {
        "BAR": {
            "id": "BAR",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B03", "B02", "B01", "B06"]}},
            "summaries": {"eo:bands": [{"name": "B03"}, {"name": "B02"}, {"name": "B01"}, {"name": "B06"}]},
        },
        "FOO": {
            "id": "FOO",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04"]}},
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
        },
        "S2": {
            "_vito": {
                "data_source": {
                    "common_name": "S2",
                    "merged_collections": ["FOO", "BAR"],
                    "type": "merged_by_common_name",
                }
            },
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04", "B06"]}},
            "extent": {"spatial": {"bbox": []}, "temporal": {"interval": []}},
            "id": "S2",
            "links": [],
            "providers": [],
            "summaries": {
                "eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}, {"name": "B06"}]
            },
        },
    }


def test_merge_layers_with_common_name_band_order_override():
    metadata = {
        "FOO": {
            "id": "FOO",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04"]}},
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
        },
        "BAR": {
            "id": "BAR",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B03", "B02", "B01", "B06"]}},
            "summaries": {"eo:bands": [{"name": "B03"}, {"name": "B02"}, {"name": "B01"}, {"name": "B06"}]},
        },
        "S2": {
            "id": "S2",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B02", "B04", "B06", "B01", "B03"]}},
            "_vito": {"data_source": {"type": "virtual:merge-by-common-name"}},
        },
    }
    _merge_layers_with_common_name(metadata)
    assert metadata == {
        "BAR": {
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B03", "B02", "B01", "B06"]}},
            "id": "BAR",
            "summaries": {"eo:bands": [{"name": "B03"}, {"name": "B02"}, {"name": "B01"}, {"name": "B06"}]},
        },
        "FOO": {
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04"]}},
            "id": "FOO",
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
        },
        "S2": {
            "_vito": {
                "data_source": {
                    "common_name": "S2",
                    "merged_collections": ["FOO", "BAR"],
                    "type": "merged_by_common_name",
                }
            },
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B02", "B04", "B06", "B01", "B03"]}},
            "extent": {"spatial": {"bbox": []}, "temporal": {"interval": []}},
            "id": "S2",
            "links": [],
            "providers": [],
            "summaries": {
                "eo:bands": [{"name": "B02"}, {"name": "B04"}, {"name": "B06"}, {"name": "B01"}, {"name": "B03"}]
            },
        },
    }


def test_merge_layers_with_common_name_cube_dimensions_merging():
    metadata = {
        "FOO": {
            "id": "FOO",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04"]}},
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
        },
        "BAR": {
            "id": "BAR",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B03", "B02", "B01", "B06"]}},
            "summaries": {"eo:bands": [{"name": "B03"}, {"name": "B02"}, {"name": "B01"}, {"name": "B06"}]},
        },
        "S2": {
            "id": "S2",
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B02", "B04", "B06", "B01", "B03"]}},
            "_vito": {"data_source": {"type": "virtual:merge-by-common-name"}},
        },
    }
    _merge_layers_with_common_name(metadata)
    assert metadata == {
        "BAR": {
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B03", "B02", "B01", "B06"]}},
            "id": "BAR",
            "summaries": {"eo:bands": [{"name": "B03"}, {"name": "B02"}, {"name": "B01"}, {"name": "B06"}]},
        },
        "FOO": {
            "common_name": "S2",
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04"]}},
            "id": "FOO",
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}, {"name": "B03"}, {"name": "B04"}]},
        },
        "S2": {
            "_vito": {
                "data_source": {
                    "common_name": "S2",
                    "merged_collections": ["FOO", "BAR"],
                    "type": "merged_by_common_name",
                }
            },
            "cube:dimensions": {"bands": {"type": "bands", "values": ["B02", "B04", "B06", "B01", "B03"]}},
            "extent": {"spatial": {"bbox": []}, "temporal": {"interval": []}},
            "id": "S2",
            "links": [],
            "providers": [],
            "summaries": {
                "eo:bands": [{"name": "B02"}, {"name": "B04"}, {"name": "B06"}, {"name": "B01"}, {"name": "B03"}]
            },
        },
    }


@pytest.mark.parametrize(
    ["load_params", "sar_backscatter_spec", "expected"],
    [
        (
            # No user-specified sar_backscatter, no backend sar_backscatter spec override
            LoadParameters(),
            None,
            {"coefficient": "gamma0-terrain"},
        ),
        (
            # No user-specified sar_backscatter, no backend sar_backscatter spec override
            LoadParameters(),
            read_spec("openeo-processes/2.x/proposals/sar_backscatter.json"),
            {"coefficient": "gamma0-terrain"},
        ),
        (
            # User specified sar_backscatter arguments, no backend sar_backscatter spec override
            LoadParameters(sar_backscatter=SarBackscatterArgs(coefficient="omega3")),
            None,
            {"coefficient": "omega3"},
        ),
        (
            # Backend-defined default coefficient, no user specified coefficient
            LoadParameters(),
            {"id": "sar_backscatter", "parameters": [{"name": "coefficient", "default": "omega666"}]},
            {"coefficient": "omega666"},
        ),
        (
            # Backend-defined default coefficient and user specified coefficient
            LoadParameters(sar_backscatter=SarBackscatterArgs(coefficient="omega3")),
            {"id": "sar_backscatter", "parameters": [{"name": "coefficient", "default": "omega666"}]},
            {"coefficient": "omega3"},
        ),
    ],
)
def test_get_sar_backscatter_arguments(load_params: LoadParameters, sar_backscatter_spec: dict, expected, caplog):
    caplog.set_level(level=logging.WARNING)

    class MyProcessing(GpsProcessing):
        def __init__(self):
            self.process_registry = ProcessRegistry()

        def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
            return self.process_registry

    processing_impl = MyProcessing()
    if sar_backscatter_spec:
        processing_impl.process_registry.add_spec(sar_backscatter_spec)
    env = EvalEnv({"openeo_api_version": OPENEO_API_VERSION_DEFAULT})

    # TODO: unfortunately we have to use mocking here because proper injection
    #       through env.backend_implementation is ruined by WhiteListEvalEnv caching business.
    with mock.patch("openeogeotrellis.backend.GpsProcessing", return_value=processing_impl):
        sar_backscatter_arguments = _get_sar_backscatter_arguments(load_params=load_params, env=env)
    assert isinstance(sar_backscatter_arguments, SarBackscatterArgs)
    assert sar_backscatter_arguments._asdict() == dirty_equals.IsPartialDict(expected)
    assert caplog.text == ""
