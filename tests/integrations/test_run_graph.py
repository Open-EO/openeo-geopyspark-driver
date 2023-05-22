import os
from openeo_driver.utils import read_json
from pathlib import Path
from openeo_driver.testing import TEST_USER_AUTH_HEADER
import re


def test_run_graph(api100):
    """
    Emile 2023-05-11: This script is for debugging only, and should not end up in the master branch.
    Could use tmp_path as argument, but to see results in CI, ./tmp is handier
    """
    containing_folder = os.path.dirname(os.path.abspath(__file__))
    graph_file = "/home/emile/openeo/VITO/double_request/double.json"
    process_graph = read_json(os.path.join(containing_folder, graph_file))

    logFile = os.path.join(containing_folder, "openeo.log")
    open(logFile, 'w').close()  # Clear file content

    response = api100.check_result(process_graph["process_graph"])

    Path("tmp").mkdir(parents=True, exist_ok=True)
    path = f"tmp/{os.path.basename(graph_file)}_{id(response)}.nc"
    print("path: " + str(path))
    with open(path, mode="wb") as f:
        f.write(response.data)

    with open(logFile) as f:
        src = f.read()

    s = set(filter(lambda x: x.find("VERSION=3") > 0, src.split("\n")))
    s = set(map(lambda x: re.sub(r'"created".*?,"', '', x), s))  # filter noisy parameter
    print("count: " + str(len(s)))  # kind of counts ,number of requests
