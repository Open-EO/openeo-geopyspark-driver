import unittest.mock as mock

from openeo.util import deep_get
from openeogeotrellis.layercatalog import get_layer_catalog


def test_issue77_band_metadata():
    # TODO: move to integration tests?
    catalog = get_layer_catalog()
    for layer in catalog.get_all_metadata():
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


#skip because test depends on external config
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
