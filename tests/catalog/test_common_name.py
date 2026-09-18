from openeogeotrellis.catalog.common_name import merge_layers_with_common_name


def test_merge_layers_with_common_name_nothing():
    metadata = {"FOO": {"id": "FOO"}, "BAR": {"id": "BAR"}}
    merge_layers_with_common_name(metadata)
    assert metadata == {"FOO": {"id": "FOO"}, "BAR": {"id": "BAR"}}


def test_merge_layers_with_common_name_simple():
    metadata = {
        "FOO": {"id": "FOO", "common_name": "S2"},
        "BAR": {"id": "BAR", "common_name": "S2"},
    }
    merge_layers_with_common_name(metadata)
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
    merge_layers_with_common_name(metadata)
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
    merge_layers_with_common_name(metadata)
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
    merge_layers_with_common_name(metadata)
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
