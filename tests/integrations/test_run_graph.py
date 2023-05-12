import os
from openeo_driver.utils import read_json
from pathlib import Path
from openeo_driver.testing import TEST_USER_AUTH_HEADER

def test_run_graph(api100):
    """
    Emile 2023-05-11: This script is for debugging only, and should not end up in the master branch.
    Could use tmp_path as argument, but to see results in CI, ./tmp is handier
    """
    containing_folder = os.path.dirname(os.path.abspath(__file__))
    graph_file = "array_apply.json"
    process_graph = read_json(os.path.join(containing_folder, graph_file)) # ["process_graph"]

    logFile = os.path.join(containing_folder, "openeo.log")
    open(logFile, 'w').close()  # Clear file content

    # response = api100.check_result(process_graph)

    process_graph = {"process":process_graph}
    resp1 = api100.post('/jobs', headers=TEST_USER_AUTH_HEADER, json=process_graph).assert_status_code(201)
    id = resp1.headers.get("OpenEO-Identifier")
    resp2 = api100.post(f'/jobs/{id}/results', headers=TEST_USER_AUTH_HEADER, json={}).assert_status_code(202)
    print(resp2)

    # Path("tmp").mkdir(parents=True, exist_ok=True)
    # path = f"tmp/{graph_file}_{id(response)}.nc"
    # print("path: " + str(path))
    # with open(path, mode="wb") as f:
    #     f.write(response.data)

    with open(logFile) as f:
        src = f.read()

    print("count: " + str(src.count("VERSION=3")))
