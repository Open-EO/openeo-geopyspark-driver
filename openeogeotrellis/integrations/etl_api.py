import logging

import requests
import sys

from requests.exceptions import RequestException
from time import sleep

SOURCE_ID = "TerraScope/MEP"
ORCHESTRATOR = "openeo"

_log = logging.getLogger(__name__)


class EtlApi:
    def __init__(self, endpoint: str = "https://etl-dev.terrascope.be"):  # TODO: point this to prod
        self._endpoint = endpoint
        self._session = requests.session()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self._session.close()

    def _can_execute_request(self, access_token: str) -> bool:
        with self._session.get(f"{self._endpoint}/user/permissions",
                               headers={'Authorization': f"Bearer {access_token}"}) as resp:
            if not resp.ok:
                print(resp.text)

            resp.raise_for_status()

            return resp.json()["execution"]

    def log_resource_usage(self, batch_job_id: str, application_id: str, user_id: str, state: str, status: str,
                           cpu_seconds: float, mb_seconds: float, duration_ms: float,
                           sentinel_hub_processing_units: float, access_token: str) -> float:
        metrics = {
            'cpu': {'value': cpu_seconds, 'unit': 'cpu-seconds'},
            'memory': {'value': mb_seconds, 'unit': 'mb-seconds'},
            'time': {'value': duration_ms, 'unit': 'milliseconds'},
        }

        if sentinel_hub_processing_units >= 0:
            metrics['processing'] = {'value': sentinel_hub_processing_units, 'unit': 'shpu'}

        data = {
            'jobId': batch_job_id,
            'executionId': application_id,
            'userId': user_id,
            'sourceId': SOURCE_ID,
            'orchestrator': ORCHESTRATOR,
            'state': state,
            'status': status,
            'metrics': metrics
            # TODO: add optional fields?
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

    def log_added_value(self, batch_job_id: str, application_id: str, user_id: str, process_id: str,
                        square_meters: float, access_token: str) -> float:
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
            'executionId': application_id,
            'userId': user_id,
            'sourceId': SOURCE_ID,
            'orchestrator': ORCHESTRATOR,
            'service': process_id,
            'area': {'value': square_meters, 'unit': 'square_meter'}
            # TODO: add optional fields?
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
    from openeogeotrellis.integrations import keycloak

    logging.basicConfig()

    client_id, client_secret = argv[1:3]

    access_token = keycloak.authenticate_oidc(client_id, client_secret)
    print(access_token)

    with EtlApi() as etl_api:
        assert etl_api._can_execute_request(access_token)

        batch_job_id = 'j-c9df97f8fea046e0ba08705e0fbd9b3b'
        application_id = 'application_1671092799310_78188'
        user_id = 'jenkins'
        state = 'FINISHED'
        status = 'SUCCEEDED'
        cpu_seconds = 5971
        mb_seconds = 5971000
        duration_ms = 123000
        sentinel_hub_processing_units = 127.15657552083333
        process_id = 'sar_backscatter'
        square_meters = 359818999.0591266

        resources_cost = etl_api.log_resource_usage(batch_job_id, application_id, user_id, state, status, cpu_seconds,
                                                    mb_seconds, duration_ms, sentinel_hub_processing_units,
                                                    access_token)

        print(f"{resources_cost=}")

        added_value_cost = etl_api.log_added_value(batch_job_id, application_id, user_id, process_id, square_meters,
                                                   access_token)

        print(f"{added_value_cost=}")


if __name__ == '__main__':
    main(sys.argv)
