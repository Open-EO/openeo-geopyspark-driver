import re
import unittest.mock as mock
from unittest import skip

from openeogeotrellis.layercatalog import get_layer_catalog


def test_layercatalog_json():
    catalog = get_layer_catalog()
    for layer in catalog.get_all_metadata():
        assert re.match(r'^[A-Za-z0-9_\-\.~\/]+$', layer['id'])
        assert 'stac_version' in layer
        assert 'description' in layer
        assert 'license' in layer
        assert 'extent' in layer


def test_issue77_band_metadata():
    catalog = get_layer_catalog()
    for layer in catalog.get_all_metadata():
        # print(layer['id'])
        # TODO: stop doing this non-standard band metadata ("bands" item in metadata root)
        old_bands = [b if isinstance(b, str) else b["band_id"] for b in layer.get("bands", [])]
        eo_bands = [b["name"] for b in layer.get("properties", {}).get('eo:bands', [])]
        cube_dimension_bands = []
        for cube_dim in layer.get("properties", {}).get("cube:dimensions", {}).values():
            if cube_dim["type"] == "bands":
                cube_dimension_bands = cube_dim["values"]
        if len(old_bands) > 1:
            assert old_bands == eo_bands
            assert old_bands == cube_dimension_bands
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
