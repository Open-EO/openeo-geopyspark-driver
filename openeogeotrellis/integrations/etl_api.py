import logging

import requests
import sys

from openeo.rest.auth.oidc import OidcClientCredentialsAuthenticator, OidcClientInfo, OidcProviderInfo
from requests.exceptions import RequestException
from time import sleep
from typing import Optional

SOURCE_ID = "TerraScope/MEP"
ORCHESTRATOR = "openeo"

_log = logging.getLogger(__name__)


class EtlApi:
    def __init__(self, endpoint: str):
        self._endpoint = endpoint
        self._session = requests.session()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self._session.close()

    def log_resource_usage(self, batch_job_id: str, title: Optional[str], application_id: str, user_id: str,
                           started_ms: float, finished_ms: float, state: str, status: str, cpu_seconds: float,
                           mb_seconds: float, duration_ms: float, sentinel_hub_processing_units: float,
                           access_token: str) -> float:
        metrics = {
            'cpu': {'value': cpu_seconds, 'unit': 'cpu-seconds'},
            'memory': {'value': mb_seconds, 'unit': 'mb-seconds'},
            'time': {'value': duration_ms, 'unit': 'milliseconds'},
        }

        if sentinel_hub_processing_units >= 0:
            metrics['processing'] = {'value': sentinel_hub_processing_units, 'unit': 'shpu'}

        data = {
            'jobId': batch_job_id,
            'jobName': title,
            'executionId': application_id,
            'userId': user_id,
            'sourceId': SOURCE_ID,
            'orchestrator': ORCHESTRATOR,
            'jobStart': started_ms,
            'jobFinish': finished_ms,
            'state': state,
            'status': status,
            'metrics': metrics
        }

        def send_request():
            with self._session.post(f"{self._endpoint}/resources", headers={'Authorization': f"Bearer {access_token}"},
                                    json=data) as resp:
                if not resp.ok:
                    _log.warning(
                        f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}",
                        extra={
                            'user_id': user_id,
                            'job_id': batch_job_id
                        })

                resp.raise_for_status()

                total_credits = sum(resource['cost'] for resource in resp.json())
                return total_credits

        return self._retry(send_request)

    def log_added_value(self, batch_job_id: str, title: Optional[str], application_id: str, user_id: str,
                        started_ms: float, finished_ms: float, process_id: str, square_meters: float,
                        access_token: str) -> float:
        billable = process_id not in ["fahrenheit_to_celsius", "mask_polygon", "mask_scl_dilation", "filter_bbox",
                                      "mean", "aggregate_spatial", "discard_result", "filter_temporal",
                                      "load_collection", "reduce_dimension", "apply_dimension", "not", "max", "or",
                                      "and", "run_udf", "save_result", "mask", "array_element", "add_dimension",
                                      "multiply", "subtract", "divide", "filter_spatial", "merge_cubes", "median",
                                      "filter_bands"]

        if not billable:
            return 0.0

        data = {
            'jobId': batch_job_id,
            'jobName': title,
            'executionId': application_id,
            'userId': user_id,
            'sourceId': SOURCE_ID,
            'orchestrator': ORCHESTRATOR,
            'jobStart': started_ms,
            'jobFinish': finished_ms,
            'service': process_id,
            'area': {'value': square_meters, 'unit': 'square_meter'}
        }

        def send_request():
            with self._session.post(f"{self._endpoint}/addedvalue", headers={'Authorization': f"Bearer {access_token}"},
                                    json=data) as resp:
                if not resp.ok:
                    _log.warning(
                        f"{resp.request.method} {resp.request.url} {data} returned {resp.status_code}: {resp.text}",
                        extra={
                            'user_id': user_id,
                            'job_id': batch_job_id
                        })

                resp.raise_for_status()

                total_credits = sum(resource['cost'] for resource in resp.json())
                return total_credits

        return self._retry(send_request)

    @staticmethod
    def _retry(func):
        attempt = 1

        while True:
            try:
                return func()
            except RequestException as e:
                if attempt >= 5:
                    raise e

                attempt += 1
                sleep(10)


def main(argv):
    logging.basicConfig()

    client_id, client_secret = argv[1:3]

    oidc_provider = OidcProviderInfo(
        issuer="https://sso-int.terrascope.be/auth/realms/terrascope")

    client_info = OidcClientInfo(
        provider=oidc_provider,
        client_id=client_id,
        client_secret=client_secret,
    )

    authenticator = OidcClientCredentialsAuthenticator(client_info)
    access_token = authenticator.get_tokens().access_token
    print(access_token)

    with EtlApi("https://etl-dev.terrascope.be") as etl_api:
        batch_job_id = 'j-284a19e1d7c14c5480b51a31a645c660'
        title = 'SentinelhubSarBackscatterBatch'
        application_id = 'application_1674538064532_2295'
        user_id = 'jenkins'
        started_ms = 1674565473000
        finished_ms = 1674565596000
        state = 'FINISHED'
        status = 'SUCCEEDED'
        cpu_seconds = 4358
        mb_seconds = 10105049
        duration_ms = 123000
        sentinel_hub_processing_units = 127.15657552083333
        process_id = 'sar_backscatter'
        square_meters = 359818999.0591266

        resources_cost = etl_api.log_resource_usage(batch_job_id, title, application_id, user_id, started_ms,
                                                    finished_ms, state, status, cpu_seconds, mb_seconds, duration_ms,
                                                    sentinel_hub_processing_units, access_token)

        print(f"{resources_cost=}")

        added_value_cost = etl_api.log_added_value(batch_job_id, title, application_id, user_id, started_ms,
                                                   finished_ms, process_id, square_meters, access_token)

        print(f"{added_value_cost=}")


if __name__ == '__main__':
    main(sys.argv)
