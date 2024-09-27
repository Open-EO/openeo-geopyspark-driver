import os
from pathlib import Path

import requests
from openeo.util import ensure_dir
from openeo_driver.utils import read_json
from openeogeotrellis.deploy.batch_job import run_job

# Avoid IPv6, to avoid hanging on https://services.terrascope.be/catalogue//collections
requests.packages.urllib3.util.connection.HAS_IPV6 = False

# workdir = Path(os.path.dirname(os.path.abspath(__file__)))
workdir = Path(os.getcwd())

process_graph_path = workdir / "process_graph.json"
print("process_graph_path: " + str(process_graph_path))
process_graph = read_json(process_graph_path)
if "process_graph" not in process_graph and "job_options" not in process_graph:
    print("Wrapping process graph")
    process_graph = {"process_graph": process_graph}

run_job(
    process_graph,
    output_file=workdir / "random_folder_name",
    metadata_file=workdir / "metadata.json",
    api_version="1.0.0",
    job_dir=ensure_dir(workdir / "job_dir"),
    dependencies=[],
    user_id="jenkins",
)
