from typing import List, Optional

import pytest
from openeo.rest.auth.testing import OidcMock
from openeo_driver.testing import DictSubSet
from openeo_driver.users import User
from openeo_driver.util.auth import ClientCredentials

from openeogeotrellis.integrations.etl_api import DynamicEtlApiConfig
from openeogeotrellis.job_costs_calculator import CostsDetails, DynamicEtlApiJobCostCalculator
from openeogeotrellis.testing import gps_config_overrides


class TestDynamicEtlApiJobCostCalculator:
    @pytest.fixture
    def etl_credentials(self) -> ClientCredentials:
        """Default client credentials for ETL API access"""
        return ClientCredentials(oidc_issuer="https://oidc.test", client_id="client123", client_secret="s3cr3t")

    @pytest.fixture
    def custom_etl_api_config(self, etl_credentials):
        class CustomEtlConfig(DynamicEtlApiConfig):
            def get_root_url(self, *, user: Optional[User] = None, job_options: Optional[dict] = None) -> str:
                return {"alt": "https://etl-alt.test", "planb": "https://etl.planb.test"}[job_options["my_etl"]]

        return CustomEtlConfig(
            urls_and_credentials={
                # Note using same credentials for all ETL API instances, to keep testing here simple
                "https://etl-alt.test": etl_credentials,
                "https://etl.planb.test": etl_credentials,
            }
        )

    @pytest.fixture(autouse=True)
    def oidc_mock(self, requests_mock, etl_credentials: ClientCredentials) -> OidcMock:
        oidc_mock = OidcMock(
            requests_mock=requests_mock,
            oidc_issuer=etl_credentials.oidc_issuer,
            expected_grant_type="client_credentials",
            expected_client_id=etl_credentials.client_id,
            expected_fields={"client_secret": etl_credentials.client_secret, "scope": "openid"},
        )
        return oidc_mock

    def _build_post_resources_handler(self, oidc_mock: OidcMock, expected_data: dict, expected_result: List[dict]):
        """Build a requests_mock handler for etl api `POST /resources` request"""

        def post_resources(request, context):
            """Handler for etl api `POST /resources` request"""
            assert request.headers["Authorization"] == "Bearer " + oidc_mock.state["access_token"]
            assert request.json() == DictSubSet(expected_data)
            return expected_result

        return post_resources

    def test_calculate_cost(self, custom_etl_api_config, requests_mock, oidc_mock):
        with gps_config_overrides(etl_api_config=custom_etl_api_config):
            calculator = DynamicEtlApiJobCostCalculator()

            mock_alt = requests_mock.post(
                "https://etl-alt.test/resources",
                json=self._build_post_resources_handler(
                    oidc_mock,
                    expected_data={
                        "jobId": "job-123",
                        "userId": "john",
                        "executionId": "exec123",
                        "state": "FINISHED",
                        "status": "UNDEFINED",  # TODO #610
                    },
                    expected_result=[{"cost": 33}, {"cost": 55}],
                ),
            )
            mock_planb = requests_mock.post(
                "https://etl.planb.test/resources",
                json=self._build_post_resources_handler(
                    oidc_mock,
                    expected_data={
                        "jobId": "job-456",
                        "userId": "john",
                        "executionId": "exec456",
                        "state": "FAILED",
                        "status": "UNDEFINED",  # TODO #610
                    },
                    expected_result=[{"cost": 100}, {"cost": 2000}],
                ),
            )

            costs_details = CostsDetails(
                job_id="job-123",
                user_id="john",
                execution_id="exec123",
                job_options={"my_etl": "alt"},
                app_state_etl_api_deprecated="FINISHED",
                job_status="finished",
            )
            costs = calculator.calculate_costs(costs_details)
            assert costs == 88.0
            assert (mock_alt.call_count, mock_planb.call_count) == (1, 0)

            costs_details = CostsDetails(
                job_id="job-456",
                user_id="john",
                execution_id="exec456",
                job_options={"my_etl": "planb"},
                app_state_etl_api_deprecated="FAILED",
                job_status="failed",
            )
            costs = calculator.calculate_costs(costs_details)
            assert costs == 2100.0
            assert (mock_alt.call_count, mock_planb.call_count) == (1, 1)
