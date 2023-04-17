from openeogeotrellis.integrations.etl_api import ETL_API_STATE


def test_etl_api_state():
    constant_values = (value for key, value in vars(ETL_API_STATE).items() if not key.startswith("_"))
    for value in constant_values:
        assert isinstance(value, str)
