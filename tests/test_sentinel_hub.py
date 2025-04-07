from openeogeotrellis import sentinel_hub
from openeogeotrellis.geopysparkdatacube import GeopysparkCubeMetadata
from openeogeotrellis.layercatalog import get_layer_catalog


class TestSentinelHub:
    def test_assure_polarization_from_sentinel_bands_dv(self, tmp_path, vault):
        metadata_properties = {}
        all_metadata = get_layer_catalog(vault, opensearch_enrich=False)
        collection_id = "SENTINEL1_GRD"
        metadata = GeopysparkCubeMetadata(all_metadata.get_collection_metadata(collection_id))
        metadata = metadata.filter_bands(["VV", "VH"])
        sentinel_hub.assure_polarization_from_sentinel_bands(metadata, metadata_properties)
        assert metadata_properties["polarization"] == {'eq': 'DV'}

    def test_assure_polarization_from_sentinel_bands_no_polarization(self, tmp_path, vault):
        metadata_properties = {}
        all_metadata = get_layer_catalog(vault, opensearch_enrich=False)
        collection_id = "SENTINEL1_GRD"
        metadata = GeopysparkCubeMetadata(all_metadata.get_collection_metadata(collection_id))
        metadata = metadata.filter_bands(["VV"])
        sentinel_hub.assure_polarization_from_sentinel_bands(metadata, metadata_properties)
        assert "polarization" not in metadata_properties

    def test_assure_polarization_from_sentinel_bands_no_polarization_2(self, tmp_path, vault):
        metadata_properties = {}
        all_metadata = get_layer_catalog(vault, opensearch_enrich=True)
        collection_id = "SENTINEL1_CARD4L"
        metadata = GeopysparkCubeMetadata(all_metadata.get_collection_metadata(collection_id))
        metadata = metadata.filter_bands(["VV", "VH"])
        sentinel_hub.assure_polarization_from_sentinel_bands(metadata, metadata_properties)
        assert "polarization" not in metadata_properties
