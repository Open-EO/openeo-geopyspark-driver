import requests
import sys


SOURCE_ID = "TerraScope/MEP"
ORCHESTRATOR = "openeo"

# TODO: point this to prod
ETL_API = "https://etl-dev.terrascope.be"


def _can_execute_request(access_token: str) -> bool:
    with requests.get(f"{ETL_API}/user/permissions", headers={'Authorization': f"Bearer {access_token}"}) as resp:
        if not resp.ok:
            print(resp.text)

        resp.raise_for_status()

        return resp.json()["execution"]


def log_resource_usage(batch_job_id: str, application_id: str, user_id: str, state: str, status: str,
                       cpu_seconds: float, mb_seconds: float, duration_ms: float,
                       sentinel_hub_processing_units: float, access_token: str) -> float:
    metrics = {
        'cpu': {'value': cpu_seconds, 'unit': 'cpu-seconds'},
        'memory': {'value': mb_seconds, 'unit': 'mb-seconds'},
        'time': {'value': duration_ms, 'unit': 'milliseconds'},
    }

    if sentinel_hub_processing_units >= 0:
        metrics['processing'] = {'value': sentinel_hub_processing_units, 'unit': 'shpu'}

    with requests.post(f"{ETL_API}/resources",
                       headers={'Authorization': f"Bearer {access_token}"},
                       json={
                           'jobId': batch_job_id,
                           'executionId': application_id,
                           'userId': user_id,
                           'sourceId': SOURCE_ID,
                           'orchestrator': ORCHESTRATOR,
                           'state': state,
                           'status': status,
                           'metrics': metrics
                           # TODO: add optional fields?
                       }) as resp:
        print(resp.text)
        resp.raise_for_status()

        total_credits = sum(resource['cost'] for resource in resp.json())
        return total_credits


def log_added_value(batch_job_id: str, application_id: str, user_id: str, process_id: str, square_meters: float,
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
                           'sourceId': SOURCE_ID,
                           'orchestrator': ORCHESTRATOR,
                           'service': process_id,
                           'area': {'value': square_meters, 'unit': 'square_meter'}
                           # TODO: add optional fields?
                       }) as resp:
        print(resp.text)
        resp.raise_for_status()

        total_credits = sum(resource['cost'] for resource in resp.json())
        return total_credits


def main(argv):
    from openeogeotrellis.integrations import keycloak

    client_id, client_secret = argv[1:3]

    access_token = keycloak.authenticate_oidc(client_id, client_secret)
    print(access_token)

    assert _can_execute_request(access_token)

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

    resources_cost = log_resource_usage(batch_job_id, application_id, user_id, state, status, cpu_seconds, mb_seconds,
                                        duration_ms, sentinel_hub_processing_units, access_token)

    print(f"{resources_cost=}")

    added_value_cost = log_added_value(batch_job_id, application_id, user_id, process_id, square_meters, access_token)

    print(f"{added_value_cost=}")


if __name__ == '__main__':
    main(sys.argv)
