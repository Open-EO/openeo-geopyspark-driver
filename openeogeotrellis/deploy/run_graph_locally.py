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
    run_job(
        as_flat_graph(process_graph),
        output_file=output_dir / "random_folder_name",
        metadata_file=output_dir / "metadata.json",
        api_version="1.0.0",
        job_dir=ensure_dir(output_dir / "job_dir"),
        dependencies=[],
        user_id="jenkins",
    )


def start_entrypoint():
    """
    for setup.py entry_points
    """
    if len(sys.argv) >= 2:
        workdir = Path(sys.argv[1])
    else:
        workdir = Path.cwd()
    process_graph_path = workdir / "process_graph.json"
    run_graph_locally(process_graph_path, workdir)


if __name__ == "__main__":
    start_entrypoint()
