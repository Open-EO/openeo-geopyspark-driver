import requests
import sys

KEYCLOAK = "https://sso-int.terrascope.be"
ETL_API = "https://etl-dev.terrascope.be"


def _authenticate_oidc(client_id: str, client_secret: str) -> str:  # access token
    with requests.post(f"{KEYCLOAK}/auth/realms/terrascope/protocol/openid-connect/token",
                       headers={'Content-Type': 'application/x-www-form-urlencoded'},
                       data={
                           'grant_type': 'client_credentials',
                           'client_id': client_id,
                           'client_secret': client_secret
                       }) as resp:
        if not resp.ok:
            print(resp.text)

        resp.raise_for_status()
        return resp.json()["access_token"]


def _can_execute_request(access_token: str) -> bool:
    with requests.get(f"{ETL_API}/user/permissions", headers={'Authorization': f"Bearer {access_token}"}) as resp:
        if not resp.ok:
            print(resp.text)

        resp.raise_for_status()

        return resp.json()["execution"]


def _log_resource_usage(batch_job_id: str, application_id: str, user_id: str, state: str, status: str,
                        cpu_seconds: float, sentinel_hub_processing_units: float, access_token: str) -> float:
    with requests.post(f"{ETL_API}/resources",
                       headers={'Authorization': f"Bearer {access_token}"},
                       json={
                           'jobId': batch_job_id,
                           'executionId': application_id,
                           'userId': user_id,
                           'sourceId': "TerraScope/MEP",
                           'orchestrator': "openeo",
                           'state': state,
                           'status': status,
                           'metrics': {
                               'cpu': {'value': cpu_seconds, 'unit': 'cpu-seconds'},
                               'processing': {'value': sentinel_hub_processing_units, 'unit': 'shpu'}
                           }
                       }) as resp:
        print(resp.text)
        resp.raise_for_status()

        total_credits = sum(resource['cost'] for resource in resp.json())
        return total_credits


def _log_added_value(batch_job_id: str, application_id: str, user_id: str, process_id: str, square_meters: float,
                     access_token: str) -> float:
    billable = process_id not in ["fahrenheit_to_celsius", "mask_polygon", "mask_scl_dilation", "filter_bbox", "mean",
                                  "aggregate_spatial", "discard_result", "filter_temporal", "load_collection",
                                  "reduce_dimension", "apply_dimension", "not", "max", "or", "and", "run_udf",
                                  "save_result", "mask", "array_element", "add_dimension", "multiply", "subtract",
                                  "divide", "filter_spatial", "merge_cubes", "median", "filter_bands"]

    if not billable:
        return 0.0

    with requests.post(f"{ETL_API}/addedvalue",
                       headers={'Authorization': f"Bearer {access_token}"},
                       json={
                           'jobId': batch_job_id,
                           'executionId': application_id,
                           'userId': user_id,
                           'sourceId': "TerraScope/MEP",
                           'orchestrator': "openeo",
                           'service': process_id,
                           'area': {'value': square_meters, 'unit': 'square_meter'}
                       }) as resp:
        print(resp.text)
        resp.raise_for_status()

        total_credits = sum(resource['cost'] for resource in resp.json())
        return total_credits


def main(argv):
    client_id, client_secret = argv[1:3]

    access_token = _authenticate_oidc(client_id, client_secret)
    print(access_token)

    assert _can_execute_request(access_token)

    batch_job_id = 'j-c9df97f8fea046e0ba08705e0fbd9b3b'
    application_id = 'application_1671092799310_78188'
    user_id = 'jenkins'
    state = 'FINISHED'
    status = 'SUCCEEDED'
    cpu_seconds = 5971
    sentinel_hub_processing_units = 127.15657552083333
    process_id = 'sar_backscatter'
    square_meters = 359818999.0591266

    _log_resource_usage(batch_job_id, application_id, user_id, state, status, cpu_seconds,
                        sentinel_hub_processing_units, access_token)

    _log_added_value(batch_job_id, application_id, user_id, process_id, square_meters, access_token)


if __name__ == '__main__':
    main(sys.argv)
