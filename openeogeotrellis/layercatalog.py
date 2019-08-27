import json
from pathlib import Path
from typing import List, Dict

import geopyspark as gps


class LayerCatalog:

    """Catalog providing access to GeoPySpark layers"""
    def __init__(self, filename='layercatalog.json'):
        path = Path(filename)
        if not path.is_file():
            raise RuntimeError("layercatalog.json not found, please make sure that it is available in the working directory.")

        with path.open() as f:
            self.catalog = {layer["id"]: layer for layer in json.load(f)}

    def layers(self) -> List:
        """Returns all available layers."""
        #TODO make this work with Kerberos authentication
        store = gps.AttributeStore("accumulo+kerberos://epod-master1.vgt.vito.be:2181/hdp-accumulo-instance")
        layers = store.layers()
        return [LayerCatalog._clean_config(config)  for config in self.catalog.values()]

    @classmethod
    def _clean_config(cls, layer_config):
        desired_keys = set(layer_config.keys()) - {"data_id"}
        return {k:v for k,v in layer_config.items() if k in desired_keys}

    def layer(self,product_id) -> Dict:
        """Returns the layer config for a given id."""
        if product_id in self.catalog:
            return self.catalog[product_id]
        else:
            raise ValueError("Unknown collection id: " + product_id)



