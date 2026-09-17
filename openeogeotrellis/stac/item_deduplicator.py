"""
Post-collection deduplication of STAC Items for load_stac.
"""
from __future__ import annotations

import datetime
import logging
import re
from typing import Any, Dict, Iterable, Iterator, List, Optional, Union

import pystac
import pystac.utils
import shapely.geometry

from openeogeotrellis.config import get_backend_config

logger = logging.getLogger(__name__)


class ItemDeduplicator:
    """
    Deduplicate STAC Items based on nominal datetime and selected properties.
    """

    DEFAULT_DUPLICATION_PROPERTIES = [
        "platform",
        "constellation",
        "gsd",
        "processing:level",
        "product:timeliness",
        "product:type",
        # "proj:code", TODO: Sometimes UTM zone can differ.
        "sar:frequency_band",
        "sar:instrument_mode",
        "sar:observation_direction",
        "sar:polarizations",
        "sat:absolute_orbit",
        "sat:orbit_state",
    ]

    def __init__(
        self,
        *,
        time_shift_max: float = 30,
        duplication_properties: Optional[List[str]] = None,
        score_property_preference: Optional[Dict[str, Union[List, Dict]]] = None,
        properties_from_id: Optional[Dict[str, Union[str, re.Pattern]]] = None,
    ):
        """

        :param score_property_preference: dict mapping property name to a value scoring, given as:
            - ordered list from most preferred to least preferred
              e.g. {"processing:version": [110, 100]} means:
              prefer items where processing:version==110 over those with 100
            - or as dict mapping a value to a score
        :param properties_from_id: optional mapping to support extracting (fake) properties
            from item id using regular expressions. For example,
            to extract the "RT" value (consolidation period) from
            item ids like "c_gls_GPP300-RT0_202603310000_GLOBE"
            and pick the items with the highest RT value, use something like:

                properties_from_id={"rt": "-(RT[0-9]+)_"},
                score_property_preference={"rt": ["RT2", "RT1", "RT0"]},
        """
        self._time_shift_max = time_shift_max

        # Duplication properties: properties that will be compared
        # with simple equality to determine duplication (among other criteria).
        if duplication_properties is None:
            self._duplication_properties = self.DEFAULT_DUPLICATION_PROPERTIES
        else:
            self._duplication_properties = duplication_properties

        # Pre-compute the property-score mapping
        self._score_property_preference: Dict[str, Dict[str, int]] = {
            p: self._to_score_map(m) for p, m in (score_property_preference or {}).items()
        }

        # Dict of regular expressions to allow extracting (fake) properties from item id, e.g. as fallback
        self._properties_from_id: Optional[Dict[str, re.Pattern]] = (
            {k: (v if isinstance(v, re.Pattern) else re.compile(v)) for k, v in properties_from_id.items()}
            if properties_from_id
            else None
        )

    @staticmethod
    def _to_score_map(score_map: Union[list, dict]) -> Dict[str, int]:
        if isinstance(score_map, list):
            return {v: len(score_map) - i for i, v in enumerate(score_map)}
        else:
            return score_map

    def __repr__(self):
        return f"ItemDeduplicator({self._duplication_properties=}, {self._score_property_preference=}, {self._properties_from_id=})".replace(
            "self._", ""
        )

    @staticmethod
    def _item_nominal_date(item: pystac.Item) -> datetime.datetime:
        # TODO: cache result (e.g. by item id)?
        dt = item.datetime or pystac.utils.str_to_datetime(item.properties["start_datetime"])
        # ensure UTC timezone for proper comparison
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt

    def _get_item_property(self, item: pystac.Item, property: str) -> Any:
        if property in item.properties:
            return item.properties[property]
        elif self._properties_from_id and property in self._properties_from_id:
            if match := self._properties_from_id[property].search(item.id):
                return match.group(1)
        return None

    def _is_duplicate_item(self, item1: pystac.Item, item2: pystac.Item) -> bool:
        try:
            # Note this large `and` chain to leverage short-circuiting so that
            # the more expensive checks (e.g. geometry) are only done
            # when cheaper checks (e.g. date and properties) pass.
            return (
                # Same date
                (
                    abs((self._item_nominal_date(item1) - self._item_nominal_date(item2)).total_seconds())
                    < self._time_shift_max
                )
                # Same properties
                and all(
                    self._get_item_property(item1, property=p) == self._get_item_property(item2, property=p)
                    for p in self._duplication_properties
                )
                # Comparable bbox
                and self._is_same_bbox(item1.bbox, item2.bbox, epsilon=1e-3)  # 1e-3 degrees ≈ 111 meters
                # Same geometry
                and self._is_same_geometry(item1.geometry, item2.geometry)
            )
        except Exception as e:
            logger.warning(f"Failed to compare {item1.id=} and {item2.id=} for duplication: {e=}", exc_info=True)
            return False

    def _is_same_bbox(self, bbox1: Optional[List[float]], bbox2: Optional[List[float]], epsilon=1e-6) -> bool:
        if isinstance(bbox1, list) and isinstance(bbox2, list):
            return len(bbox1) == 4 and len(bbox2) == 4 and all(abs(a - b) <= epsilon for a, b in zip(bbox1, bbox2))
        elif bbox1 is None and bbox2 is None:
            return True
        else:
            return False

    def _is_same_geometry(self, geom1: Optional[Dict], geom2: Optional[Dict], epsilon=3e-4, dice_threshold=0.99) -> bool:
        """Check if two GeoJSON geometries are approximately equal.

        First tries shapely.equals_exact (cheap coordinate-wise comparison with tolerance).
        If that fails (e.g. different vertex counts), falls back to the Sorensen-Dice
        coefficient on area overlap, same approach as isDuplicate in OpenSearchResponses.scala.

        STAC item geometries are always in WGS 84 (EPSG:4326),
        so the epsilon is in degrees (3e-4 degrees ≈ 33 meters at the equator).
        """
        if isinstance(geom1, dict) and isinstance(geom2, dict):
            shape1 = shapely.geometry.shape(geom1)
            shape2 = shapely.geometry.shape(geom2)
            if shapely.equals_exact(shapely.normalize(shape1), shapely.normalize(shape2), tolerance=epsilon):
                return True
            # Fallback: Dice coefficient on area overlap
            try:
                area_sum = shape1.area + shape2.area
                if area_sum == 0:
                    return False
                dice_score = 2 * shape1.intersection(shape2).area / area_sum
                return dice_score >= dice_threshold
            except Exception as e:
                logger.warning(f"Failed geometry Dice score comparison: {e}", exc_info=True)
                return False
        elif geom1 is None and geom2 is None:
            return True
        else:
            return False

    def _score(self, item: pystac.Item) -> tuple:
        """Score an item for deduplication preference (higher is better)."""
        # Primary: score by property preference (if configured)
        score = tuple(
            score_map.get(self._get_item_property(item, property=prop), 0)
            for prop, score_map in self._score_property_preference.items()
        )
        # Fallback: prefer more recently updated items, then use item id as tie breaker
        return score + (item.properties.get("updated", ""), item.id)

    def _group_duplicates(self, items: Iterable[pystac.Item]) -> Iterator[List[pystac.Item]]:
        """Produce groups of duplicate items."""
        # Pre-sort items, to allow quick breaking out of inner loop
        items = sorted(items, key=self._item_nominal_date)
        handled = set()
        time_shift_max = datetime.timedelta(seconds=self._time_shift_max)
        stats = {"items": 0, "groups": 0}
        for i, item_i in enumerate(items):
            stats["items"] += 1
            if i in handled:
                continue
            group = [item_i]
            horizon = self._item_nominal_date(item_i) + time_shift_max
            for j in range(i + 1, len(items)):
                item_j = items[j]
                if self._item_nominal_date(item_j) > horizon:
                    break
                if self._is_duplicate_item(item_i, item_j):
                    group.append(item_j)
                    handled.add(j)
            yield group
            stats["groups"] += 1
        logger.debug(f"ItemDeduplicator._group_duplicates {stats=}")

    def deduplicate(self, items: Iterable[pystac.Item]) -> List[pystac.Item]:
        result = []
        for group in self._group_duplicates(items):
            if len(group) > 1:
                best = max(group, key=self._score)
                logger.debug(f"Deduplicate: keeping {best.id=} from {len(group)=}")
            else:
                best = group[0]
            result.append(best)
        return result


def _deduplicator_from_feature_flags(feature_flags: dict, *, id: Optional[str] = None) -> Union[ItemDeduplicator, None]:
    deduplicate_items = feature_flags.get("deduplicate_items", get_backend_config().load_stac_deduplicate_items_default)

    if deduplicate_items:
        if isinstance(deduplicate_items, dict):
            if not deduplicate_items.get("enable", True):
                return None
            duplication_properties = deduplicate_items.get("duplication_properties")
            score_property_preference = deduplicate_items.get("score_property_preference")
            properties_from_id = deduplicate_items.get("properties_from_id")
        else:
            # Legacy feature flags
            # TODO: remove support for these sub-feature flags at top-level
            duplication_properties = feature_flags.get("duplication_properties")
            score_property_preference = feature_flags.get("score_property_preference")
            properties_from_id = feature_flags.get("deduplicator_properties_from_id")
            if duplication_properties or score_property_preference or properties_from_id:
                logger.warning(f"Deprecated 'deduplicate_items' feature flag usage ({id=})")

        return ItemDeduplicator(
            duplication_properties=duplication_properties,
            score_property_preference=score_property_preference,
            properties_from_id=properties_from_id,
        )
    return None
