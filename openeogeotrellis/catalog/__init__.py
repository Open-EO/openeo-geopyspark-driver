from typing import Callable, Dict, List, Union

# Some type annotation aliases to make things more self-documenting.
CollectionId = str
CollectionMetadataDict = Dict[str, Union[str, dict, list]]
CatalogDict = Dict[CollectionId, CollectionMetadataDict]
LinksList = List[dict]
LinksFilter = Callable[[LinksList], LinksList]


class DATA_SOURCE_PROPERTIES:
    # Container for fields used under `"_vito": {"data_source": {` collection metadata

    # TODO: ultimately this flag can be fully eliminated
    #       once layer catalog management and enrichment is done in integrated, pre-runtime approach
    ENRICH = "enrich"
