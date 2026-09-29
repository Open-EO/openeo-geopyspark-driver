"""
Focused unit tests for `openeogeotrellis.stac.property_filter`, exercised
through its own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import logging

import dirty_equals
import pystac_client
import pytest
from openeo_driver.filter_properties import PropertyConditionException
from openeo_driver.utils import EvalEnv

from openeogeotrellis.stac.property_filter import AdaptingPropertyFilter, PropertyFilter


class TestPropertyFilter:
    def test_build_matcher_empty(self):
        """Empty property filter: always matches"""
        property_filter = PropertyFilter(properties={})
        matcher = property_filter.build_matcher()
        assert matcher({}) == True
        assert matcher({"foo": "bar"}) == True

    def test_build_matcher_basic(self):
        """Basic use case: single (equality) condition"""
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "bar",
                        },
                        "result": True,
                    }
                }
            }
        }
        property_filter = PropertyFilter(properties)
        matcher = property_filter.build_matcher()
        assert matcher({}) == False
        assert matcher({"foo": "bar"}) == True
        assert matcher({"foo": "nope"}) == False
        assert matcher({"fooooo": "bar"}) == False

    def test_build_matcher_strips_properties_prefix(self):
        """
        Regression test for https://github.com/Open-EO/openeo-geopyspark-driver/issues/1690:
        when the user filters on `properties.<name>` (e.g. to satisfy STAC APIs that require
        that prefix in CQL2 property references), the local post-query matcher should still
        match against `pystac.Item.properties`, which are stored without that prefix.
        """
        properties = {
            "properties.foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "bar",
                        },
                        "result": True,
                    }
                }
            }
        }
        property_filter = PropertyFilter(properties)
        matcher = property_filter.build_matcher()
        assert matcher({"foo": "bar"}) == True
        assert matcher({"foo": "nope"}) == False
        assert matcher({"properties.foo": "bar"}) == False

    def test_build_matcher_multiple_conditions(self):
        """Multiple conditions: all must match"""
        properties = {
            "color": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "red",
                        },
                        "result": True,
                    }
                }
            },
            "size": {
                "process_graph": {
                    "lte1": {
                        "process_id": "lte",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": 42,
                        },
                        "result": True,
                    }
                }
            },
        }
        property_filter = PropertyFilter(properties=properties)
        matcher = property_filter.build_matcher()
        assert matcher({}) == False
        assert matcher({"color": "red", "size": 10}) == True
        assert matcher({"color": "rrred", "size": 10}) == False
        assert matcher({"color": "red", "size": 41}) == True
        assert matcher({"color": "red", "size": 42}) == True
        assert matcher({"color": "red", "size": 43}) == False
        assert matcher({"color": "red", "size": 100}) == False

    @pytest.mark.parametrize(
        ["pg_node", "matching", "non_matching"],
        [
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "y-bar"}},
                ["y-bar"],
                ["nope", None],
            ),
            (
                {"process_id": "eq", "arguments": {"x": "x-bar", "y": {"from_parameter": "value"}}},
                ["x-bar"],
                ["nope", None],
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                [42],
                [0, 42.01, 44, None],
            ),
            (
                {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                [0, 42],
                [42.01, 100, None],
            ),
            (
                {"process_id": "lte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                [42, 100],
                [0, 41, None],
            ),
            (
                {"process_id": "gte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                [42, 100],
                [0, 41, None],
            ),
            (
                {"process_id": "gte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                [42, 0],
                [100, None],
            ),
            (
                {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
                [42, 4242],
                [0, 41, None, -101],
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "32U*B"}},
                ["32UXXB", "32UB"],
                ["32UXXC", "33UXXB", None],
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "31*"}},
                ["31UFS", "31ABC"],
                ["32UFS", None],
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "31U?S"}},
                ["31UFS", "31UXS"],
                ["31UFFS", "31UF", None],
            ),
        ],
    )
    def test_build_matcher_operators(self, pg_node, matching, non_matching):
        """Single conditions in multiple variants (operators, argument order)"""
        properties = {"foo": {"process_graph": {"_": {**pg_node, "result": True}}}}
        property_filter = PropertyFilter(properties=properties)
        matcher = property_filter.build_matcher()
        for value in matching:
            assert matcher({"foo": value}) == True
        for value in non_matching:
            assert matcher({"foo": value}) == False

    def test_build_matcher_with_env(self):
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": {"from_parameter": "name"},
                        },
                        "result": True,
                    }
                }
            }
        }
        env = EvalEnv().push_parameters({"name": "alice"})
        property_filter = PropertyFilter(properties=properties, env=env)
        matcher = property_filter.build_matcher()
        assert matcher({"foo": "alice"}) == True
        assert matcher({"foo": "bob"}) == False

    @pytest.mark.parametrize(
        ["properties", "expected"],
        [
            ({}, ""),
            (
                {
                    "foo": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {"x": {"from_parameter": "value"}, "y": "bar"},
                                "result": True,
                            }
                        }
                    }
                },
                "\"foo\" = 'bar'",
            ),
            (
                {
                    "color": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": "red",
                                },
                                "result": True,
                            }
                        }
                    },
                    "size": {
                        "process_graph": {
                            "lte1": {
                                "process_id": "lte",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": 42,
                                },
                                "result": True,
                            }
                        }
                    },
                },
                dirty_equals.IsOneOf(
                    '"color" = \'red\' and "size" <= 42',
                    '"size" <= 42 and "color" = \'red\'',
                ),
            ),
        ],
    )
    def test_to_cql2_text(self, properties, expected):
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_text() == expected

    @pytest.mark.parametrize(
        ["pg_node", "expected"],
        [
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "y-bar"}},
                "\"foo\" = 'y-bar'",
            ),
            (
                {"process_id": "eq", "arguments": {"x": "x-bar", "y": {"from_parameter": "value"}}},
                "\"foo\" = 'x-bar'",
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                '"foo" = 42',
            ),
            (
                {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                '"foo" <= 42',
            ),
            (
                {"process_id": "lte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                '"foo" >= 42',
            ),
            (
                {"process_id": "gte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                '"foo" >= 42',
            ),
            (
                {"process_id": "gte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                '"foo" <= 42',
            ),
            (
                {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
                '"foo" in (42, 4242)',
            ),
            (
                {
                    "process_id": "array_contains",
                    "arguments": {"data": ["blue", "green"], "y": {"from_parameter": "value"}},
                },
                "\"foo\" in ('blue', 'green')",
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "32U*B"}},
                "",
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "31?FS"}},
                "",
            ),
        ],
    )
    def test_to_cql2_text_operators(self, pg_node, expected):
        properties = {"foo": {"process_graph": {"_": {**pg_node, "result": True}}}}
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_text() == expected

    def test_to_cql2_text_with_env(self):
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": {"from_parameter": "name"},
                        },
                        "result": True,
                    }
                }
            }
        }
        env = EvalEnv().push_parameters({"name": "alice"})
        property_filter = PropertyFilter(properties=properties, env=env)
        expected = "\"foo\" = 'alice'"
        assert property_filter.to_cql2_text() == expected

    @pytest.mark.parametrize(
        ["properties", "expected"],
        [
            ({}, None),
            (
                {
                    "foo": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {"x": {"from_parameter": "value"}, "y": "bar"},
                                "result": True,
                            }
                        }
                    }
                },
                {"op": "=", "args": [{"property": "foo"}, "bar"]},
            ),
            (
                {
                    "color": {
                        "process_graph": {
                            "eq1": {
                                "process_id": "eq",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": "red",
                                },
                                "result": True,
                            }
                        }
                    },
                    "size": {
                        "process_graph": {
                            "lte1": {
                                "process_id": "lte",
                                "arguments": {
                                    "x": {"from_parameter": "value"},
                                    "y": 42,
                                },
                                "result": True,
                            }
                        }
                    },
                },
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "color"}, "red"]},
                        {"op": "<=", "args": [{"property": "size"}, 42]},
                    ],
                },
            ),
        ],
    )
    def test_to_cql2_json(self, properties, expected):
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_json() == expected

    @pytest.mark.parametrize(
        ["pg_node", "expected"],
        [
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "y-bar"}},
                {"op": "=", "args": [{"property": "foo"}, "y-bar"]},
            ),
            (
                {"process_id": "eq", "arguments": {"x": "x-bar", "y": {"from_parameter": "value"}}},
                {"op": "=", "args": [{"property": "foo"}, "x-bar"]},
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                {"op": "=", "args": [{"property": "foo"}, 42]},
            ),
            (
                {"process_id": "lte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                {"op": "<=", "args": [{"property": "foo"}, 42]},
            ),
            (
                {"process_id": "lte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                {"op": ">=", "args": [{"property": "foo"}, 42]},
            ),
            (
                {"process_id": "gte", "arguments": {"x": {"from_parameter": "value"}, "y": 42}},
                {"op": ">=", "args": [{"property": "foo"}, 42]},
            ),
            (
                {"process_id": "gte", "arguments": {"x": 42, "y": {"from_parameter": "value"}}},
                {"op": "<=", "args": [{"property": "foo"}, 42]},
            ),
            (
                {"process_id": "array_contains", "arguments": {"data": [42, 4242], "y": {"from_parameter": "value"}}},
                {"op": "in", "args": [{"property": "foo"}, [42, 4242]]},
            ),
            (
                {
                    "process_id": "array_contains",
                    "arguments": {"data": ["blue", "green"], "y": {"from_parameter": "value"}},
                },
                {"op": "in", "args": [{"property": "foo"}, ["blue", "green"]]},
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "32U*B"}},
                None,
            ),
            (
                {"process_id": "eq", "arguments": {"x": {"from_parameter": "value"}, "y": "31?FS"}},
                None,
            ),
        ],
    )
    def test_to_cql2_json_operators(self, pg_node, expected):
        properties = {"foo": {"process_graph": {"_": {**pg_node, "result": True}}}}
        property_filter = PropertyFilter(properties=properties)
        assert property_filter.to_cql2_json() == expected

    def test_to_cql2_json_with_env(self):
        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": {"from_parameter": "name"},
                        },
                        "result": True,
                    }
                }
            }
        }
        env = EvalEnv().push_parameters({"name": "alice"})
        property_filter = PropertyFilter(properties=properties, env=env)
        expected = {"op": "=", "args": [{"property": "foo"}, "alice"]}
        assert property_filter.to_cql2_json() == expected

    @pytest.mark.parametrize(
        ["use_filter_extension", "search_method", "expected"],
        [
            ("cql2-text", None, "\"foo\" = 'bar'"),
            ("cql2-json", None, {"op": "=", "args": [{"property": "foo"}, "bar"]}),
            (True, "POST", {"op": "=", "args": [{"property": "foo"}, "bar"]}),
            (True, "GET", "\"foo\" = 'bar'"),
        ],
    )
    def test_to_cql2_filter(self, use_filter_extension, search_method, expected, requests_mock):
        links = [{"rel": "self", "href": "https://stac.test/"}]
        if search_method:
            links.append({"rel": "search", "href": "https://stac.test/search", "method": search_method})

        requests_mock.get(
            "https://stac.test/",
            json={
                "stac_version": "1.0.0",
                "conformsTo": ["https://api.stacspec.org/v1.0.0/item-search"],
                "type": "Catalog",
                "id": "test-catalog",
                "description": "Test STAC catalog",
                "links": links,
            },
        )

        properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "bar",
                        },
                        "result": True,
                    }
                }
            }
        }
        property_filter = PropertyFilter(properties=properties)
        client = pystac_client.Client.open("https://stac.test/")

        assert (
            property_filter.to_cql2_filter(
                use_filter_extension=use_filter_extension,
                client=client,
            )
            == expected
        )

    @pytest.mark.parametrize(
        ["allow", "deny", "input_output_cases"],
        [
            (
                None,
                None,
                [
                    ({}, False),
                    ({"color": "red", "size": 10}, True),
                    ({"color": "rrred", "size": 10}, False),
                    ({"color": "red", "size": 1000}, False),
                ],
            ),
            (
                ["color"],
                None,
                [
                    ({}, False),
                    ({"color": "red", "size": 10}, True),
                    ({"color": "rrred", "size": 10}, False),
                    ({"color": "red", "size": 1000}, True),
                ],
            ),
            (
                ["color", "flavor"],
                None,
                [
                    ({}, False),
                    ({"color": "red", "size": 10}, True),
                    ({"color": "rrred", "size": 10}, False),
                    ({"color": "red", "size": 1000}, True),
                ],
            ),
            (
                ["size"],
                None,
                [
                    ({}, False),
                    ({"color": "red", "size": 10}, True),
                    ({"color": "rrred", "size": 10}, True),
                    ({"color": "red", "size": 1000}, False),
                ],
            ),
            (
                None,
                ["color"],
                [
                    ({}, False),
                    ({"color": "red", "size": 10}, True),
                    ({"color": "rrred", "size": 10}, True),
                    ({"color": "red", "size": 1000}, False),
                ],
            ),
            (
                None,
                ["size", "color", "flavor"],
                [
                    ({}, True),
                    ({"color": "green"}, True),
                    ({"size": 10000}, True),
                ],
            ),
        ],
    )
    def test_subsetted(self, allow, deny, input_output_cases):
        properties = {
            "color": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": "red",
                        },
                        "result": True,
                    }
                }
            },
            "size": {
                "process_graph": {
                    "lte1": {
                        "process_id": "lte",
                        "arguments": {
                            "x": {"from_parameter": "value"},
                            "y": 42,
                        },
                        "result": True,
                    }
                }
            },
        }
        property_filter_orig = PropertyFilter(properties=properties)
        property_filter = property_filter_orig.subsetted(allow=allow, deny=deny)
        matcher = property_filter.build_matcher()

        expected = [o for i, o in input_output_cases]
        actual = [matcher(i) for i, o in input_output_cases]
        assert actual == expected


class TestAdaptingPropertyFilter:
    @pytest.mark.parametrize(
        ["adaptations", "expected_text", "expected_json"],
        [
            (
                # Empty case (no adaptations)
                {},
                "\"foo\" = 'FOO' and \"bar\" = 'BAR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "foo"}, "FOO"]},
                        {"op": "=", "args": [{"property": "bar"}, "BAR"]},
                    ],
                },
            ),
            (
                # Drop "foo"
                {"foo": "drop"},
                "\"bar\" = 'BAR'",
                {"op": "=", "args": [{"property": "bar"}, "BAR"]},
            ),
            (
                # Rename "foo" to "fancyfoo"
                {"foo": {"rename": "fancyfoo"}},
                "\"fancyfoo\" = 'FOO' and \"bar\" = 'BAR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "fancyfoo"}, "FOO"]},
                        {"op": "=", "args": [{"property": "bar"}, "BAR"]},
                    ],
                },
            ),
            (
                # Map values
                {
                    "foo": {"value_mapping": {"SOMETHING": "else"}},
                    "bar": {"value_mapping": {"BAR": "BARRRR"}},
                },
                "\"foo\" = 'FOO' and \"bar\" = 'BARRRR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "foo"}, "FOO"]},
                        {"op": "=", "args": [{"property": "bar"}, "BARRRR"]},
                    ],
                },
            ),
            (
                # add-MGRS-prefix
                {"foo": {"value_mapping": "add-MGRS-prefix"}},
                "\"foo\" = 'MGRS-FOO' and \"bar\" = 'BAR'",
                {
                    "op": "and",
                    "args": [
                        {"op": "=", "args": [{"property": "foo"}, "MGRS-FOO"]},
                        {"op": "=", "args": [{"property": "bar"}, "BAR"]},
                    ],
                },
            ),
        ],
    )
    def test_overrides_to_cql2(self, adaptations, expected_text, expected_json):
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "FOO"},
                        "result": True,
                    }
                }
            },
            "bar": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "BAR"},
                        "result": True,
                    }
                }
            },
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)
        assert property_filter.to_cql2_text() == expected_text
        assert property_filter.to_cql2_json() == expected_json

    @pytest.mark.parametrize(
        ["adaptations", "no_match", "match"],
        [
            (
                {},
                [{}, {"foo": "bar"}],
                [{"foo": "FOO"}],
            ),
            (
                {"foo": "drop"},
                [],
                [{}, {"anything": "goes"}],
            ),
            (
                {"foo": {"rename": "hohoho", "value_mapping": {"FOO": "HAHAHA"}}},
                [{}, {"fancyfoo": "FOO"}, {"foo": "FOO"}],
                [{"hohoho": "HAHAHA"}],
            ),
            (
                {"foo": {"rename": "mgrs-foo", "value_mapping": "add-MGRS-prefix"}},
                [{}, {"foo": "FOO"}, {"mgrs-foo": "FOO"}],
                [{"mgrs-foo": "MGRS-FOO"}],
            ),
        ],
    )
    def test_overrides_to_matcher(self, adaptations, no_match, match):
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "FOO"},
                        "result": True,
                    }
                }
            }
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)

        matcher = property_filter.build_matcher()
        assert [matcher(input) for input in no_match] == [False] * len(no_match)
        assert [matcher(input) for input in match] == [True] * len(match)

    def test_wildcard_with_mgrs_prefix(self):
        """Wildcard tileId with MGRS prefix adaptation: not sent to STAC API, only filtered locally."""
        user_specified_properties = {
            "tileId": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "32U*B"},
                        "result": True,
                    }
                }
            }
        }
        adaptations = {"tileId": {"rename": "grid:code", "value_mapping": "add-MGRS-prefix"}}
        pf = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)

        # CQL2: wildcard filters should be excluded (not supported by STAC API)
        assert pf.to_cql2_text() == ""
        assert pf.to_cql2_json() is None
        # Client-side matcher: uses fnmatch with shell wildcards
        matcher = pf.build_matcher()
        assert matcher({"grid:code": "MGRS-32UXXB"}) == True
        assert matcher({"grid:code": "MGRS-32UB"}) == True
        assert matcher({"grid:code": "MGRS-32UXXC"}) == False
        assert matcher({"grid:code": "MGRS-33UXXB"}) == False

    def test_logging(self, caplog):
        caplog.set_level(level=logging.INFO)
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "eq1": {
                        "process_id": "eq",
                        "arguments": {"x": {"from_parameter": "value"}, "y": "FOO"},
                        "result": True,
                    }
                }
            }
        }
        adaptations = {
            "foo": {"rename": "mgrs-foo", "value_mapping": "add-MGRS-prefix"},
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)
        property_filter.to_cql2_text()
        assert caplog.messages == [
            """AdaptingPropertyFilter: updates=["Rename 'foo' to 'mgrs-foo'", "Map 'mgrs-foo' value 'FOO' to 'MGRS-FOO'"]"""
        ]

    @pytest.mark.parametrize(
        ["adaptations", "expected_text", "expected_json"],
        [
            (
                {"foo": {"rename": "ffooo", "value_mapping": {"F22": "F2000"}}},
                """"ffooo" in ('F1', 'F2000', 'F333')""",
                {"op": "in", "args": [{"property": "ffooo"}, ["F1", "F2000", "F333"]]},
            ),
            (
                {"foo": {"rename": "mgrs-foo", "value_mapping": "add-MGRS-prefix"}},
                """"mgrs-foo" in ('MGRS-F1', 'MGRS-F22', 'MGRS-F333')""",
                {"op": "in", "args": [{"property": "mgrs-foo"}, ["MGRS-F1", "MGRS-F22", "MGRS-F333"]]},
            ),
        ],
    )
    def test_contains(self, adaptations, expected_text, expected_json):
        user_specified_properties = {
            "foo": {
                "process_graph": {
                    "contains": {
                        "process_id": "array_contains",
                        "arguments": {"data": ["F1", "F22", "F333"], "value": {"from_parameter": "value"}},
                        "result": True,
                    }
                }
            }
        }
        property_filter = AdaptingPropertyFilter(user_specified_properties, adaptations=adaptations)
        assert property_filter.to_cql2_text() == expected_text
        assert property_filter.to_cql2_json() == expected_json




def test_properties_prefix_applied_in_cql2():
    """`properties_prefix` (deprecated `_experimental_properties_prefix` feature flag) prefixes property names."""
    properties = {
        "foo": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "bar"},
                    "result": True,
                }
            }
        }
    }
    without_prefix = PropertyFilter(properties=properties)
    assert without_prefix.to_cql2_text() == '"foo" = \'bar\''
    assert without_prefix.to_cql2_json() == {"op": "=", "args": [{"property": "foo"}, "bar"]}

    with_prefix = PropertyFilter(properties=properties, properties_prefix="properties.")
    assert with_prefix.to_cql2_text() == '"properties.foo" = \'bar\''
    assert with_prefix.to_cql2_json() == {"op": "=", "args": [{"property": "properties.foo"}, "bar"]}


def test_unsupported_process_graph_raises():
    """A process not supported by property filtering (only eq/lte/gte/array_contains) raises."""
    properties = {
        "foo": {
            "process_graph": {
                "neq1": {
                    "process_id": "neq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "bar"},
                    "result": True,
                }
            }
        }
    }
    property_filter = PropertyFilter(properties=properties)
    with pytest.raises(PropertyConditionException, match="not 'neq'"):
        property_filter.to_cql2_text()
    with pytest.raises(PropertyConditionException, match="not 'neq'"):
        property_filter.to_cql2_json()
    with pytest.raises(PropertyConditionException, match="not 'neq'"):
        property_filter.build_matcher()


def test_matcher_used_as_post_query_filter():
    """
    `build_matcher()` is what `ItemCollection.from_stac_api` uses to drop items that a STAC API
    returned (e.g. because it doesn't support the requested property filter server-side) but that
    do not actually satisfy the filter.
    """
    properties = {
        "product:type": {
            "process_graph": {
                "eq1": {
                    "process_id": "eq",
                    "arguments": {"x": {"from_parameter": "value"}, "y": "IW_GRDH_1S"},
                    "result": True,
                }
            }
        }
    }
    property_filter = PropertyFilter(properties=properties)
    matcher = property_filter.build_matcher()

    # Simulate items "returned by the API" (e.g. because it ignores unsupported filters server-side).
    items_from_api = [
        {"id": "item-1", "product:type": "IW_GRDH_1S"},
        {"id": "item-2", "product:type": "SM_GRDH_1S"},
    ]
    kept = [item["id"] for item in items_from_api if matcher(item)]
    assert kept == ["item-1"]
