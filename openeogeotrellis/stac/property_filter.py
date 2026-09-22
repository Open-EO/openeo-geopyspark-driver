"""
Property filter translation for load_stac.

Translates openEO property filters (e.g. the `properties` argument of
`load_collection`/`load_stac` processes) into STAC API CQL2 queries
and local matching predicates.
"""
from __future__ import annotations

import fnmatch
import logging
from copy import deepcopy
from typing import Any, Callable, Dict, Iterable, Iterator, Optional, Tuple, Union

import pystac_client
from openeo_driver import filter_properties
from openeo_driver.utils import EvalEnv

logger = logging.getLogger(__name__)

# Some type aliases related to property filters expressed as process graphs
# (e.g. like the `properties` argument of `load_collection`/`load_stac` processes).
FlatProcessGraph = Dict[str, dict]
PropertyFilterPGMap = Dict[str, FlatProcessGraph]


class PropertyFilter:
    """
    Container for STAC object property filters declared as process graphs
    (e.g. like the `properties` argument of `load_collection`/`load_stac` processes).

    :param properties: mapping of property names to the desired conditions
        expressed as openEO-style process graphs (flat graph)
    :param env: optional evaluation environment,
        e.g. with extra parameters to consider when evaluating the process graphs
    """

    # TODO: move this utility to a more generic location for better reuse

    def __init__(
        self,
        properties: PropertyFilterPGMap,
        *,
        env: Optional[EvalEnv] = None,
        # TODO: remove this prefix option again, as the consensus seems that prefix should not be used. #1584
        properties_prefix: str = "",
    ):
        self._properties = properties
        self._env = env or EvalEnv()
        if properties_prefix:
            logger.warning(f"PropertyFilter with non-empty {properties_prefix=} which is deprecated")
        self._properties_prefix = properties_prefix

    def _iter_literal_matches(self) -> Iterator[Tuple[str, str, Any]]:
        """Helper to produce tuples of property-name, operator and value"""
        for property_name, pg in self._properties.items():
            for operator, value in filter_properties.extract_literal_match(pg, env=self._env).items():
                if operator == "eq" and isinstance(value, str) and ("*" in value or "?" in value):
                    operator = "like"
                yield property_name, operator, value

    @staticmethod
    def _build_callable(operator: str, value: Any) -> Callable[[Any], bool]:
        if operator == "eq":
            return lambda actual: actual == value
        elif operator == "like":
            return lambda actual, p=value: actual is not None and fnmatch.fnmatch(str(actual), p)
        elif operator == "lte":
            return lambda actual: actual is not None and actual <= value
        elif operator == "gte":
            return lambda actual: actual is not None and value <= actual
        elif operator == "in":
            return lambda actual: actual is not None and actual in value
        else:
            # TODO: support more operators?
            raise ValueError(f"Unsupported operator: {operator}")

    def build_matcher(self) -> Callable[[Dict[str, Any]], bool]:
        """
        Build an evaluating function (a closure)
        that can be used to check if properties match the filter conditions.
        """
        conditions = [
            (property_name, self._build_callable(operator, value))
            for property_name, operator, value in self._iter_literal_matches()
        ]

        def match(properties: Dict[str, Any]) -> bool:
            return all(name in properties and condition(properties[name]) for name, condition in conditions)

        return match

    def to_cql2_filter(
        self,
        *,
        use_filter_extension: Union[bool, str],
        client: pystac_client.Client,
    ) -> Union[str, dict, None]:
        # TODO: the strong coupling between GET+CQL2-text and POST+CQL2-JSON is a bit off here:
        #       per [STAC API filter spec](https://github.com/stac-api-extensions/filter?tab=readme-ov-file#get-query-parameters-and-post-json-fields)
        #       GET can use both CQL2 text and JSON, but POST should only use JSON.
        #       Method and CQL2 format should ideally be decoupled.
        if use_filter_extension == "cql2-json":  # force POST JSON
            return self.to_cql2_json()
        elif use_filter_extension == "cql2-text":  # force GET text
            return self.to_cql2_text()
        elif use_filter_extension == True:  # auto-detect, favor POST
            # TODO: CQL2 format detection should be done through conformance classes instead of link rels
            #      also see https://github.com/stac-api-extensions/filter?tab=readme-ov-file#get-query-parameters-and-post-json-fields
            search_links = client.get_links(rel="search")
            supports_post_search = any(link.extra_fields.get("method") == "POST" for link in search_links)
            if supports_post_search:
                return self.to_cql2_json()
            else:
                # assume serves ignores filter if no "search" method advertised
                return self.to_cql2_text()
        elif use_filter_extension == False:
            return None  # explicitly disabled
        else:
            raise ValueError(f"Invalid use-filter-extension value: {use_filter_extension!r}")

    def to_cql2_text(self) -> str:
        """Convert the property filter to a CQL2 text representation."""
        filters = []
        for property_name, operator, value in self._iter_literal_matches():
            if operator == "like":
                continue
            operator = self._to_cql2_operator(operator)
            # Bit of ad-hoc value encoding (note that we exploit the fact here
            # that `repr` produces single quoted strings, as expected in CQL2 text format)
            if isinstance(value, (list, set)):
                value = repr(tuple(value))
            else:
                value = repr(value)
            filters.append(f'"{self._properties_prefix}{property_name}" {operator} {value}')
        return " and ".join(filters)

    def _to_cql2_operator(self, operator: str):
        """Map operators produced by extract_literal_match to CQL2 operators."""
        cql2_op = {
            "eq": "=",
            "neq": "<>",
            "lt": "<",
            "lte": "<=",
            "gt": ">",
            "gte": ">=",
            # Note that the operators produced by `extract_literal_match`
            # (the keys in this mapping) are currently somewhat arbitrairy:
            # most correspond directly to openEO-processes naming,
            # while openEO's `array_contains` is translated to `in` for some reason,
            "in": "in",
            "array_contains": "in",  # Still cover for openEO-style naming here to be future-proof
        }.get(operator)
        if not cql2_op:
            raise ValueError(f"Unsupported operator {operator}")
        return cql2_op

    def to_cql2_json(self) -> Union[dict, None]:
        filters = [
            {
                "op": self._to_cql2_operator(operator),
                "args": [{"property": f"{self._properties_prefix}{property_name}"}, value],
            }
            for property_name, operator, value in self._iter_literal_matches()
            if operator != "like"
        ]
        if len(filters) == 0:
            return None
        elif len(filters) == 1:
            return filters[0]
        else:
            return {"op": "and", "args": filters}

    def subsetted(self, allow: Optional[Iterable[str]] = None, deny: Optional[Iterable[str]] = None):
        """
        Create a new PropertyFilter object with a subset from the current property filters,
        based on provided allow- and deny-lists.
        """
        properties = {
            k: deepcopy(v)
            for k, v in self._properties.items()
            if (allow is None or k in allow) and (deny is None or k not in deny)
        }
        return PropertyFilter(properties, env=self._env, properties_prefix=self._properties_prefix)


class AdaptingPropertyFilter(PropertyFilter):
    """
    PropertyFilter subclass with extra mapping of (legacy) property names and values.

    Mapping instructions are given as a dictionary, with (legacy) user-provided property names as key
    (named "legacy_property" in examples below), supporting the following transformations:

    - drop filtering on a property (e.g. because legacy property is no longer available,
      and filtering would cause nothing to match):

           {"legacy_property": "drop"}

    - rename legacy property to new property name:

          {"legacy_property": {"rename" : "new_name"}}

    - Rename property values:

          {"legacy_property": {"value_mapping": {"old_value": "new_value"}}}

      "value_mapping" here can be
      - a dictionary for simple mapping (missing values are kept)
      - (string) "add-MGRS-prefix": to add a "MGRS-" prefix to the legacy value.
    """

    def __init__(
        self,
        properties: PropertyFilterPGMap,
        *,
        env: Optional[EvalEnv] = None,
        adaptations: Dict[str, Union[dict, str]],
        properties_prefix: str = "",
    ):
        super().__init__(properties=properties, env=env, properties_prefix=properties_prefix)
        self._adaptations = adaptations

    def _iter_literal_matches(self) -> Iterator[Tuple[str, str, Any]]:
        updates = []
        for property_name, operator, value in super()._iter_literal_matches():
            adaptation = self._adaptations.get(property_name, "preserve")
            if adaptation == "preserve":
                # Keep everything as-is (default)
                pass
            elif adaptation == "drop":
                updates.append(f"Drop {property_name!r}")
                # Skip yield
                continue
            elif isinstance(adaptation, dict):
                if rename := adaptation.get("rename"):
                    updates.append(f"Rename {property_name!r} to {rename!r}")
                    property_name = rename
                if value_mapping := adaptation.get("value_mapping"):
                    new_value = self._map_value(value_mapping=value_mapping, value=value)
                    if new_value != value:
                        updates.append(f"Map {property_name!r} value {value!r} to {new_value!r}")
                        value = new_value
            else:
                raise ValueError(f"Invalid {adaptation=}")

            yield property_name, operator, value
        if updates:
            # TODO: make this a (more descriptive) warning to push users to update their filters?
            logger.info(f"AdaptingPropertyFilter: {updates=}")

    def _map_value(self, value_mapping: Union[dict, str], value: Any) -> Any:
        if isinstance(value_mapping, dict):
            mapper = lambda v: value_mapping.get(v, v)
        elif value_mapping == "add-MGRS-prefix":
            # TODO: make this more generic with something like "add-prefix:<prefix>"?
            mapper = lambda v: f"MGRS-{v}"
        elif value_mapping == "make_lower_case":
            mapper = lambda v: v.lower() if isinstance(v, str) else v
        elif value_mapping == "make_upper_case":
            mapper = lambda v: v.upper() if isinstance(v, str) else v
        else:
            raise ValueError(f"Invalid {value_mapping=}")

        if isinstance(value, (list, tuple, set)):
            new_value = type(value)(mapper(v) for v in value)
        else:
            new_value = mapper(value)
        return new_value
