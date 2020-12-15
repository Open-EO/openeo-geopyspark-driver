import os
import unittest.mock as mock
from typing import List, Tuple

import pytest
import schema

from openeo.util import deep_get
from openeo_driver.backend import LoadParameters
from openeo_driver.utils import EvalEnv
from openeogeotrellis.layercatalog import get_layer_catalog, GeoPySparkLayerCatalog


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


def test_get_layer_catalog_from_opensearch():
    with mock.patch("openeogeotrellis.layercatalog.ConfigParams") as ConfigParams:
        ConfigParams.return_value.layer_catalog_metadata_files = [
            "tests/data/layercatalog01.json",
            "tests/data/layercatalog02.json",
            "tests/data/layercatalog03.json"
        ]

        opensearch = mock.MagicMock()
        opensearch.get_collections.return_value = [
            {
                "id": "urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
                "bbox": [-1.05893, 47.66031, 11.6781, 53.67487],
                "properties": {
                    "title": "SENTINEL-1 Level-1 Ground Range Detected (GRD) SIGMA0 products",
                    "abstract": "The Sigma0 product describes how much of the radar signal that was sent out by "
                                "Sentinel-1 is reflected back to the sensor...",
                    "links": {
                        "describedby": [
                            {
                                "href": "https://docs.terrascope.be/#/DataProducts/Sentinel-1/ProductsOverview",
                                "type": "text/html",
                                "title": "Online User Documentation"
                            },
                            {
                                "href": "https://www.vito-eodata.be/collections/srv/eng/main.home?uuid=urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
                                "type": "text/html"
                            }
                        ],
                        "search": [
                            {
                                "href": "http://oscars-01.vgt.vito.be:8080/description.geojson?collection=urn:eop:VITO:CGS_S1_GRD_SIGMA0_L1",
                                "type": "application/geo+json",
                                "title": "OpenSearch entry point"
                            }
                        ]
                    },
                    "acquisitionInformation": [
                        {
                            "acquisitionParameters": {"beginningDateTime": "2014-10-23T00:00:00Z"}
                        },
                        {
                            "acquisitionParameters": {"beginningDateTime": "2014-10-24T00:00:00Z"}
                        }
                    ],
                    "bands": [
                        {
                            "description": "Calibrated radar backscattering coefficient (unitless), describing the returned radar signal strength in the cross-polarized channel (V transmit, H receive). Values are stored as floats.",
                            "type": "VH",
                            "title": "VH",
                            "resolution": 10,
                            "bitPerValue": 32
                        }
                    ]
                }
            }
        ]

        all_metadata = get_layer_catalog(lambda _: opensearch).get_all_metadata()

    assert all_metadata == [
        {
            "id": "XIP",
            "_vito": {
                "data_source": {
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
                ]
            }
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


@pytest.mark.skipif(not os.environ.get("CREODIAS"), reason="Requires CREODIAS environment.")
def test_creodias_s1_backscatter(tmp_path):
    catalog = GeoPySparkLayerCatalog(all_metadata=[{
        "id": "Creodias-S1-Backscatter",
        "_vito": {"data_source": {"type": 'creodias-s1-backscatter'}}
    }])

    load_params = LoadParameters(
        temporal_extent=("2020-06-06", "2020-06-08"),
        spatial_extent=dict(west=3.15, south=51.3, east=3.25, north=51.37),
    )
    datacube = catalog.load_collection("Creodias-S1-Backscatter", load_params=load_params, env=EvalEnv())

    filename = tmp_path / "s1backscatter.tiff"
    datacube.save_result(filename, format="GTiff")#, format_options={'stitch':True})
