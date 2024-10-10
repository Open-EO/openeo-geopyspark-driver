import os
import sys
from pathlib import Path

import requests

import openeogeotrellis.deploy.local
from openeo.internal.graph_building import as_flat_graph
from openeo.util import ensure_dir

openeogeotrellis.deploy.local.setup_environment()

# Can only import after setup_environment:
from openeogeotrellis.deploy.batch_job import run_job

# Avoid IPv6, to avoid hanging on https://services.terrascope.be/catalogue//collections
requests.packages.urllib3.util.connection.HAS_IPV6 = False


def run_graph_locally(process_graph, output_dir):
    output_dir = Path(output_dir)
    process_graph = as_flat_graph(process_graph)
    if "process_graph" not in process_graph:
        process_graph = {"process_graph": process_graph}
    run_job(
        process_graph,
        output_file=output_dir / "random_folder_name",
        metadata_file=output_dir / "metadata.json",
        api_version="1.0.0",
        job_dir=ensure_dir(output_dir),
        dependencies=[],
        user_id="jenkins",
    )


def main():
    """
    for setup.py entry_points
    """
    process_graph_path = Path(sys.argv[1])
    if len(sys.argv) > 2:
        workdir = Path(sys.argv[2])
    else:
        workdir = process_graph_path.parent
    run_graph_locally(process_graph_path, workdir)


if __name__ == "__main__":
    main()
