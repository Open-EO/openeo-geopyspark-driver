from openeogeotrellis import sentinel_hub


class TestSentinelHub:
    def test_assure_polarization_from_sentinel_bands_dv(self, tmp_path):
        metadata_properties = {}
        sentinel_hub.assure_polarization_from_sentinel_bands(["VV", "VH", "randomBandName"], metadata_properties)
        assert metadata_properties["polarization"] == {'eq': 'DV'}

    def test_assure_polarization_from_sentinel_bands_no_polarization(self, tmp_path):
        metadata_properties = {}
        sentinel_hub.assure_polarization_from_sentinel_bands(["VV"], metadata_properties)
        assert "polarization" not in metadata_properties
