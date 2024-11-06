import json
import os
import sys
from pathlib import Path

from openeo.internal.graph_building import as_flat_graph
from openeo.util import ensure_dir

import openeogeotrellis.deploy.local


def run_graph_locally(process_graph, output_dir):
    output_dir = ensure_dir(output_dir)
    openeogeotrellis.deploy.local.setup_environment(output_dir)
    # Can only import after setup_environment:
    from openeogeotrellis.backend import JOB_METADATA_FILENAME
    from openeogeotrellis.deploy.batch_job import run_job
    process_graph = as_flat_graph(process_graph)
    if "process_graph" not in process_graph:
        process_graph = {"process_graph": process_graph}
    run_job(
        process_graph,
        output_file=output_dir / "random_folder_name",
        metadata_file=output_dir / JOB_METADATA_FILENAME,
        api_version="2.0.0",
        job_dir=output_dir,
        dependencies=[],
        user_id="run_graph_locally",
    )
    # Set the permissions so any user can read and delete the files:
    # For when running inside a docker container.
    files = [
        output_dir / JOB_METADATA_FILENAME,
        output_dir / "collection.json",
        output_dir / "openeo.log",
    ]
    with open(output_dir / JOB_METADATA_FILENAME) as f:
        j = json.load(f)
        files += [output_dir / asset["href"] for asset in j.get("links", [])]
    with open(output_dir / "collection.json") as f:
        j = json.load(f)
        files += [output_dir / asset["href"] for asset in j.get("links", [])]
    for file in files:
        os.chmod(file, 0o666)


def main():
    """
    for setup.py entry_points
    """
    if len(sys.argv) < 2:
        print("Usage: run_graph_locally.py path/to/process_graph.json [path/to/output/]")
        sys.exit(1)
    process_graph_path = Path(sys.argv[1])
    if len(sys.argv) > 2:
        workdir = Path(sys.argv[2])
    else:
        workdir = process_graph_path.parent
    run_graph_locally(process_graph_path, workdir)


if __name__ == "__main__":
    main()
