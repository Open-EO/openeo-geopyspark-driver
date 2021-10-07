import unittest.mock as mock
from pathlib import Path
from typing import List, Tuple

import pytest
import schema

from openeo.util import deep_get
from openeo_driver.utils import read_json
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata
from openeogeotrellis.layercatalog import get_layer_catalog


def _get_layers() -> List[Tuple[str, dict]]:
    catalog = get_layer_catalog()
    layers = catalog.get_all_metadata()
    return [(layer["id"], layer) for layer in layers]


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




def test_get_layer_catalog_with_updates():
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            "tests/data/layercatalog01.json",
            "tests/data/layercatalog02.json",
        ]
        catalog = get_layer_catalog()
        assert sorted(l["id"] for l in catalog.get_all_metadata()) == ["BAR", "BZZ", "FOO", "QUU"]
        foo = catalog.get_collection_metadata("FOO")
        assert foo["license"] == "apache"
        assert foo["links"] == ["example.com/foo"]
        bar = catalog.get_collection_metadata("BAR")
        assert bar["description"] == "The BAR layer"
        assert bar["links"] == ["example.com/bar"]


# skip because test depends on external config
def skip_sentinelhub_layer():
    catalog = get_layer_catalog()
    viewingParameters = {}
    viewingParameters["from"] = "2018-01-01"
    viewingParameters["to"] = "2018-01-02"

    viewingParameters["left"] = 4
    viewingParameters["right"] = 4.0001
    viewingParameters["top"] = 50.00001
    viewingParameters["bottom"] = 50.0
    viewingParameters["srs"] = "EPSG:4326"
    datacube = catalog.load_collection("SENTINEL1_GAMMA0_SENTINELHUB", viewingParameters)


def test_get_layer_catalog_opensearch_enrich_oscars(requests_mock):
    test_root = Path(__file__).parent / "data"
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            test_root / "layercatalog01.json",
            test_root / "layercatalog02.json",
            test_root / "layercatalog03_oscars.json"
        ]


        collections_response = read_json(test_root / "collections_oscars01.json")
        requests_mock.get("https://services.terrascope.test/catalogue/collections", json=collections_response)

        all_metadata = get_layer_catalog(opensearch_enrich=True).get_all_metadata()

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


def test_get_layer_catalog_opensearch_enrich_creodias(requests_mock):
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            "tests/data/layercatalog01.json",
            "tests/data/layercatalog04_creodias.json"
        ]
        collections_response = read_json("tests/data/collections_creodias01.json")
        requests_mock.get("https://finder.creodias.test/resto/collections.json", json=collections_response)

        all_metadata = get_layer_catalog(opensearch_enrich=True).get_all_metadata()

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
