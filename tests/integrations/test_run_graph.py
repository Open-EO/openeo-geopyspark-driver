import os
from openeo_driver.utils import read_json
from pathlib import Path


def test_run_graph(api100):
    """
    Emile 2023-05-11: This script is for debugging only, and should not end up in the master branch.
    Could use tmp_path as argument, but to see results in CI, ./tmp is handier
    """
    containing_folder = os.path.dirname(os.path.abspath(__file__))
    graph_file = "array_apply.json"
    process_graph = read_json(os.path.join(containing_folder, graph_file))["process_graph"]

    logFile = os.path.join(containing_folder, "openeo.log")
    open(logFile, 'w').close()  # Clear file content

    response = api100.check_result(process_graph)

    Path("tmp").mkdir(parents=True, exist_ok=True)
    path = f"tmp/{graph_file}_{id(response)}.tiff"
    print("path: " + str(path))
    with open(path, mode="wb") as f:
        f.write(response.data)

    with open(logFile) as f:
        src = f.read()

    print("count: " + str(src.count("VERSION=3")))
