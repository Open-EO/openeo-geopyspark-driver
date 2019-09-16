import copy
import json
import warnings
from pathlib import Path
from typing import Union, List, Dict


class UnknownCollectionException(ValueError):
    # TODO move this to openeo package?
    # TODO subclass from openeo base exception?
    def __init__(self, collection_id):
        super().__init__("Unknown collection {c!r}".format(c=collection_id))


class LayerCatalog:
    """Catalog describing the available image collections."""

    _stac_version = "0.7.0"

    def __init__(self, source: Union[str, List[dict], Dict[str, dict]] = 'layercatalog.json'):
        if isinstance(source, (str, Path)):
            path = Path(source)
            if not path.is_file():
                raise OSError("LayerCatalog file not found: {f}".format(f=path))
            with path.open() as f:
                source = json.load(f)

        if isinstance(source, list) and all(isinstance(layer, dict) for layer in source):
            self.catalog = {layer["id"]: layer for layer in source}
        elif isinstance(source, dict) and all(isinstance(layer, dict) for layer in source.values()):
            self.catalog = source.copy()
        else:
            raise ValueError("Don't know how to handle {s!r}".format(s=source))

    def _normalize_layer_metadata(self, metadata: dict, hide_private=True) -> dict:
        """Make sure the layer metadata follows OpenEO spec to some extent."""
        metadata = copy.deepcopy(metadata)

        collection_id = metadata["id"]

        # Make sure required fields are set.
        metadata.setdefault("stac_version", self._stac_version)
        metadata.setdefault("links", [])
        metadata.setdefault("other_properties", {})
        # Warn about missing fields where sensible defaults are not feasible
        fallbacks = {
            "description": "Description of {c} (#TODO)".format(c=collection_id),
            "license": "proprietary",
            "extent": {"spatial": [0, 0, 0, 0], "temporal": [None, None]},
            "properties": {"cube:dimensions": {}},
        }
        for key, value in fallbacks.items():
            if key not in metadata:
                warnings.warn("Collection {c} is missing required metadata field {k!r}.".format(c=collection_id, k=key))
                metadata[key] = value

        if hide_private:
            # Don't expose "private" fields
            for key in [k for k in metadata.keys() if k.startswith('_')]:
                del metadata[key]

        return metadata

    def assert_collection_id(self, collection_id):
        if collection_id not in self.catalog:
            raise UnknownCollectionException(collection_id)

    def layers(self) -> list:
        """Returns all available layers."""
        return [self._normalize_layer_metadata(m) for m in self.catalog.values()]

    def layer(self, collection_id: str, hide_private=True) -> dict:
        """Returns the layer config for a given id."""
        self.assert_collection_id(collection_id)
        return self._normalize_layer_metadata(self.catalog[collection_id], hide_private=hide_private)
