"""
Focused unit tests for `openeogeotrellis.stac.item_deduplicator`, exercised
through its own public surface (no JVM/GeoPySpark dependency), independent of
`test_load_stac.py`'s end-to-end JVM-backed checks.
"""
import pystac
import pytest
from openeo.testing.stac import StacDummyBuilder

from openeogeotrellis.stac.item_collection import ItemCollection
from openeogeotrellis.stac.item_deduplicator import ItemDeduplicator, deduplicator_from_feature_flags


class TestItemDeduplicator:
    def test_trivial(self):
        item = pystac.Item.from_dict(StacDummyBuilder.item())
        deduplicator = ItemDeduplicator()
        assert deduplicator.deduplicate([item]) == [item]

    def test_repr(self):
        deduplicator = ItemDeduplicator(
            duplication_properties=["platform"], score_property_preference={"gsd": [10, 100]}
        )
        expected = "ItemDeduplicator(duplication_properties=['platform'], score_property_preference={'gsd': {10: 2, 100: 1}}, properties_from_id=None)"
        assert repr(deduplicator) == expected
        assert str(deduplicator) == expected

    def test_basic(self):
        item10 = pystac.Item.from_dict(StacDummyBuilder.item(id="item-10", datetime="2025-11-10T00:00:00Z"))
        item10_1s = pystac.Item.from_dict(StacDummyBuilder.item(id="item-10+1s", datetime="2025-11-10T00:00:01Z"))
        item10_1h = pystac.Item.from_dict(StacDummyBuilder.item(id="item-10+1h", datetime="2025-11-10T01:00:00Z"))
        item11 = pystac.Item.from_dict(StacDummyBuilder.item(id="item-11", datetime="2025-11-11T00:00:00Z"))

        deduplicator = ItemDeduplicator()
        assert deduplicator.deduplicate([item10, item10_1s, item11]) == [item10_1s, item11]
        assert deduplicator.deduplicate([item10, item10_1h, item11]) == [item10, item10_1h, item11]

    def test_property_based(self):
        item600 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item1", properties={"product:type": "a", "flavor": "apple"})
        )
        item600b = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", properties={"product:type": "a", "flavor": "banana"})
        )
        item601 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item3", properties={"product:type": "b", "flavor": "apple"})
        )

        deduplicator = ItemDeduplicator()
        assert deduplicator.deduplicate([item600, item600b, item601]) == [item600b, item601]

        deduplicator = ItemDeduplicator(duplication_properties=["flavor"])
        assert deduplicator.deduplicate([item600, item600b, item601]) == [item601, item600b]

    @pytest.mark.parametrize(
        ["item2_updated", "best"],
        [
            ("2025-11-12T12:00:10Z", "item2"),
            ("2025-11-12T11:00:00Z", "item1"),
        ],
    )
    def test_updated(self, item2_updated, best):
        item1 = pystac.Item.from_dict(StacDummyBuilder.item(id="item1", properties={"updated": "2025-11-12T12:00:00Z"}))
        item2 = pystac.Item.from_dict(StacDummyBuilder.item(id="item2", properties={"updated": item2_updated}))
        deduplicator = ItemDeduplicator()
        result = deduplicator.deduplicate([item1, item2])
        assert [r.id for r in result] == [best]

    @pytest.mark.parametrize(
        ["bbox2", "expected"],
        [
            ([3, 50, 4, 51], ["item2", "item3"]),
            ([4, 50, 5, 51], ["item1", "item3"]),
            (None, ["item1", "item2", "item3"]),
            ([8, 40, 9, 41], ["item1", "item2", "item3"]),
            # Invalid bboxes, but should not break deduplication
            (123, ["item1", "item2", "item3"]),
            ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], ["item1", "item2", "item3"]),
            (["one", "two", "three"], ["item1", "item2", "item3"]),
        ],
    )
    def test_duplicate_by_bbox(self, bbox2, expected):
        item1 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item1", bbox=[3, 50, 4, 51], properties={"updated": "2025-11-01"})
        )
        item2 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", bbox=bbox2, properties={"updated": "2025-11-02"})
        )
        item3 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item3", bbox=[4, 50, 5, 51], properties={"updated": "2025-11-03"})
        )

        deduplicator = ItemDeduplicator()
        result = deduplicator.deduplicate([item1, item2, item3])
        assert [r.id for r in result] == expected

    @pytest.mark.parametrize(
        ["geometry2", "expected"],
        [
            (
                {"type": "Polygon", "coordinates": [[[3, 50], [4, 50], [4, 51], [3, 51], [3, 50]]]},
                ["item2", "item3"],
            ),
            (
                {"type": "Polygon", "coordinates": [[[4, 50], [5, 50], [5, 51], [4, 51], [4, 50]]]},
                ["item1", "item3"],
            ),
            (
                {"type": "Polygon", "coordinates": [[[4, 50], [5, 50], [5, 51], [4, 51], [4, 50.0001]]]},
                ["item1", "item3"],
            ),
            (None, ["item1", "item2", "item3"]),
            (
                {"type": "Polygon", "coordinates": [[[8, 40], [9, 40], [9, 41], [8, 41], [8, 40]]]},
                ["item1", "item2", "item3"],
            ),
            # Invalid geometry, but should not break deduplication
            ({"type": "MobiusRing"}, ["item1", "item2", "item3"]),
            ([666, 777], ["item1", "item2", "item3"]),
        ],
    )
    def test_duplicate_by_geometry(self, geometry2, expected):
        item1 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="item1",
                geometry={"type": "Polygon", "coordinates": [[[3, 50], [4, 50], [4, 51], [3, 51], [3, 50]]]},
                properties={"updated": "2025-11-01"},
            )
        )
        item2 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item2", geometry=geometry2, properties={"updated": "2025-11-02"})
        )
        item3 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="item3",
                geometry={"type": "Polygon", "coordinates": [[[4, 50], [5, 50], [5, 51], [4, 51], [4, 50]]]},
                properties={"updated": "2025-11-03"},
            )
        )

        deduplicator = ItemDeduplicator()
        result = deduplicator.deduplicate([item1, item2, item3])
        assert [r.id for r in result] == expected

    @pytest.mark.parametrize(
        ["datetime2", "expected"],
        [
            ("2025-11-10T00:00:00Z", ["item2", "item3"]),
            ("2025-11-10T12:00:00Z", ["item1", "item2", "item3"]),
            ("2025-11-11T00:00:00Z", ["item1", "item3"]),
            ("2025-11-12T00:00:00Z", ["item1", "item3", "item2"]),
            ("2025-11-10T00:00:00+00", ["item2", "item3"]),
            ("2025-11-10T00:00:00+07", ["item2", "item1", "item3"]),
            ("2025-11-10", ["item2", "item3"]),
            ("2025-11-11", ["item1", "item3"]),
        ],
    )
    def test_datetime_and_timezones(self, datetime2, expected):
        item1 = pystac.Item.from_dict(StacDummyBuilder.item(id="item1", datetime="2025-11-10T00:00:00Z"))
        item2 = pystac.Item.from_dict(StacDummyBuilder.item(id="item2", datetime=datetime2))
        item3 = pystac.Item.from_dict(StacDummyBuilder.item(id="item3", datetime="2025-11-11T00:00:00Z"))

        deduplicator = ItemDeduplicator()
        result = deduplicator.deduplicate([item1, item2, item3])
        assert [r.id for r in result] == expected

    def test_s1_sigma0_version_dedup(self):
        """Sentinel-1 SIGMA0 items with different processing versions (V110 vs V120) should be deduplicated,
        keeping the higher version."""
        common_properties = {
            "datetime": "2020-03-25T17:24:42Z",
            "platform": "sentinel-1a",
            "constellation": "sentinel-1",
            "sar:frequency_band": "C",
            "sar:instrument_mode": "IW",
            "sar:observation_direction": "right",
            "sar:polarizations": ["VV", "VH"],
            "sat:absolute_orbit": 31835,
            "sat:orbit_state": "ascending",
        }
        common_bbox = [2.0, 50.0, 4.0, 52.0]
        common_geometry = {
            "type": "Polygon",
            "coordinates": [[[2.0, 50.0], [4.0, 50.0], [4.0, 52.0], [2.0, 52.0], [2.0, 50.0]]],
        }
        common_datetime = "2020-03-25T17:24:42Z"

        item_v110 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V110",
                datetime=common_datetime,
                bbox=common_bbox,
                geometry=common_geometry,
                properties={**common_properties, "updated": "2025-07-06T06:06:09.620403Z"},
            )
        )
        item_v120 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V120",
                datetime=common_datetime,
                bbox=common_bbox,
                geometry=common_geometry,
                properties={**common_properties, "updated": "2025-07-06T06:06:09.708329Z"},
            )
        )

        deduplicator = ItemDeduplicator()
        result = deduplicator.deduplicate([item_v110, item_v120])
        assert [r.id for r in result] == [
            "S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V120"
        ]

        # Order of input should not matter
        result = deduplicator.deduplicate([item_v120, item_v110])
        assert [r.id for r in result] == [
            "S1A_IW_GRDH_SIGMA0_DV_20200325T172442_ASCENDING_88_5C3F_V120"
        ]

    def test_score_property_preference(self):
        """score_property_preference prefers items by property value order, falling back to updated+id."""
        common_props = {"datetime": "2025-11-10T00:00:00Z"}
        item_v110 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-v110", properties={**common_props, "processing:version": 110})
        )
        item_v100 = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-v100", properties={**common_props, "processing:version": 100})
        )
        item_unknown = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-unknown", properties={**common_props, "processing:version": 999, "updated": "2099-01-01T00:00:00Z"})
        )
        item_no_version = pystac.Item.from_dict(
            StacDummyBuilder.item(id="item-no-version", properties={**common_props, "updated": "2099-01-01T00:00:00Z"})
        )

        deduplicator = ItemDeduplicator(score_property_preference={"processing:version": [110, 100]})

        # v110 is preferred over v100 regardless of input order
        assert [r.id for r in deduplicator.deduplicate([item_v100, item_v110])] == ["item-v110"]
        assert [r.id for r in deduplicator.deduplicate([item_v110, item_v100])] == ["item-v110"]

        # v100 is preferred over an unknown version (not in preference list),
        # even if the unknown version has a later "updated" timestamp
        assert [r.id for r in deduplicator.deduplicate([item_unknown, item_v100])] == ["item-v100"]

        # Property absent → same fallback as unknown value: uses updated+id
        assert [r.id for r in deduplicator.deduplicate([item_no_version, item_v100])] == ["item-v100"]

    @pytest.mark.parametrize(
        ["duplicator_kwargs", "expected"],
        [
            (
                # Default: pick item with highest "updated" value
                {},
                ["RT1"],
            ),
            (
                # Score by RT value
                dict(
                    properties_from_id={"consolidation_period": r"-(RT\d+)_"},
                    score_property_preference={
                        "consolidation_period": ["RT6", "RT5", "RT4", "RT3", "RT2", "RT1", "RT0"]
                    },
                ),
                ["RT2"],
            ),
            (
                # Reverse RT value scoring
                dict(
                    properties_from_id={"consolidation_period": r"-(RT\d+)_"},
                    score_property_preference={"consolidation_period": ["RT0", "RT1"]},
                ),
                ["RT0"],
            ),
            (
                # use RT as deduplication property
                dict(
                    properties_from_id={"consolidation_period": r"-(RT\d+)_"},
                    duplication_properties=["consolidation_period"],
                ),
                ["RT0", "RT1", "RT2"],
            ),
        ],
    )
    def test_by_consolidation_period_from_id(self, duplicator_kwargs, expected):
        # Three items with different consolidation periods (only in "RT" part of id)
        # and different "updated" properties.
        # Note that consolidation period and "updated" follow different order
        # (RT1 is updated after RT2)
        item_0 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="c_gls_LAI300-RT0_202602200000_GLOBE",
                datetime="2026-02-20T00:00:00Z",
                properties={"updated": "2026-02-22T00:00:00Z"},
            )
        )
        item_1 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="c_gls_LAI300-RT1_202602200000_GLOBE",
                datetime="2026-02-20T00:00:00Z",
                properties={"updated": "2026-02-25T00:00:00Z"},
            )
        )
        item_2 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="c_gls_LAI300-RT2_202602200000_GLOBE",
                datetime="2026-02-20T00:00:00Z",
                properties={"updated": "2026-02-21T00:00:00Z"},
            )
        )

        deduplicator = ItemDeduplicator(**duplicator_kwargs)
        deduped = deduplicator.deduplicate([item_0, item_1, item_2])
        assert [i.id for i in deduped] == [f"c_gls_LAI300-{e}_202602200000_GLOBE" for e in expected]

    @pytest.mark.parametrize(
        ["feature_flags", "expected"],
        [
            (
                # Default: no deduplication
                {},
                ["RT0", "RT1", "RT2"],
            ),
            (
                # Default deduplication: pick item with highest "updated" value
                {"deduplicate_items": True},
                ["RT1"],
            ),
            (
                # Score by RT value
                {
                    "deduplicate_items": {
                        "properties_from_id": {"consolidation_period": r"-(RT\d+)_"},
                        "score_property_preference": {
                            "consolidation_period": ["RT6", "RT5", "RT4", "RT3", "RT2", "RT1", "RT0"]
                        },
                    }
                },
                ["RT2"],
            ),
            (
                # Deprecated feature flag usage
                {
                    "deduplicate_items": True,
                    "deduplicator_properties_from_id": {"consolidation_period": r"-(RT\d+)_"},
                    "score_property_preference": {
                        "consolidation_period": ["RT6", "RT5", "RT4", "RT3", "RT2", "RT1", "RT0"]
                    },
                },
                ["RT2"],
            ),
            (
                # use RT as deduplication property
                {
                    "deduplicate_items": {
                        "properties_from_id": {"consolidation_period": r"-(RT\d+)_"},
                        "duplication_properties": ["consolidation_period"],
                    },
                },
                ["RT0", "RT1", "RT2"],
            ),
        ],
    )
    def test_from_feature_flags(self, feature_flags, expected):
        # Three items with different consolidation periods (only in "RT" part of id)
        # and different "updated" properties.
        # Note that consolidation period and "updated" follow different order
        # (RT1 is updated after RT2)
        item_0 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="c_gls_LAI300-RT0_202602200000_GLOBE",
                datetime="2026-02-20T00:00:00Z",
                properties={"updated": "2026-02-22T00:00:00Z"},
            )
        )
        item_1 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="c_gls_LAI300-RT1_202602200000_GLOBE",
                datetime="2026-02-20T00:00:00Z",
                properties={"updated": "2026-02-25T00:00:00Z"},
            )
        )
        item_2 = pystac.Item.from_dict(
            StacDummyBuilder.item(
                id="c_gls_LAI300-RT2_202602200000_GLOBE",
                datetime="2026-02-20T00:00:00Z",
                properties={"updated": "2026-02-21T00:00:00Z"},
            )
        )

        item_collection = ItemCollection([item_0, item_1, item_2])
        deduplicator = deduplicator_from_feature_flags(feature_flags)
        if deduplicator:
            item_collection = item_collection.deduplicated(deduplicator=deduplicator)

        assert [i.id for i in item_collection.items] == [f"c_gls_LAI300-{e}_202602200000_GLOBE" for e in expected]

